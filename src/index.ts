import express, { Request, Response } from "express";
import multer from "multer";
import path from "node:path";
import fs from "node:fs";
import { pathToFileURL, fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { hasDatabaseConfig, testDatabaseConnection } from "./db";
import { enqueueJob, registerWorker, stopBoss } from "./pgBoss";
import { WorkerError } from "./types";

const app = express();
const port = Number(process.env.PORT) || 3000;
const WEBHOOK_QUEUE_NAME = "webhook-zip-process";
const DEFAULT_FILE_LIMIT = 2 * 1024 * 1024 * 1024;
const DEFAULT_LARGE_FILE_LIMIT = 50 * 1024 * 1024;

type WebhookJobData = {
  fileUrl: string;
  originalFileName: string;
};

let queueWorkerStarted = false;

const uploadsDir = path.resolve(process.cwd(), "uploads");
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadsDir),
  filename: (_req, file, cb) => {
    const timestamp = Date.now();
    const safeName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_");
    cb(null, `${timestamp}_${safeName}`);
  }
});

function parseSizeLimit(
  value: string | undefined,
  fallback: number,
  variableName: string,
  defaultDisplayValue: string
): number {
  if (!value) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  const match = normalized.match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb)?$/);

  if (!match) {
    console.warn(
      `Invalid ${variableName} value \"${value}\". Falling back to ${defaultDisplayValue}.`
    );
    return fallback;
  }

  const amount = Number(match[1]);
  const unit = match[2] ?? "b";
  const multipliers: Record<string, number> = {
    b: 1,
    kb: 1024,
    mb: 1024 * 1024,
    gb: 1024 * 1024 * 1024
  };

  return Math.floor(amount * multipliers[unit]);
}

const fileLimit = parseSizeLimit(process.env.FILE_LIMIT, DEFAULT_FILE_LIMIT, "FILE_LIMIT", "2gb");
const largeFileLimit = parseSizeLimit(
  process.env.LARGE_FILE_LIMIT,
  DEFAULT_LARGE_FILE_LIMIT,
  "LARGE_FILE_LIMIT",
  "50mb"
);
const upload = multer({ storage, limits: { fileSize: fileLimit } });

function runWorker(workerFileName: string, zipPath: string, originalFileName: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const workerPath = path.resolve(__dirname, "workers", workerFileName);
    const worker = new Worker(workerPath, {
      workerData: { zipPath, originalFileName }
    });

    worker.once("message", (data: { ok: true } | WorkerError) => {
      if ("error" in data) {
        reject(new Error(data.error));
        return;
      }
      resolve();
    });

    worker.once("error", (err) => {
      reject(err);
    });

    worker.once("exit", (code) => {
      if (code !== 0) {
        reject(new Error(`Worker exited with code ${code}`));
      }
    });
  });
}

function unzipInWorker(zipPath: string, originalFileName: string): Promise<void> {
  return runWorker("unzipWorker.js", zipPath, originalFileName);
}

function unzipInLargeWorker(zipPath: string, originalFileName: string): Promise<void> {
  return runWorker("largeUnzipWorker.js", zipPath, originalFileName);
}

function isUnzipUnavailableError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  return error.message.includes("spawn unzip ENOENT");
}

async function processZipFile(
  zipPath: string,
  uploadedFileSize: number,
  originalFileName: string
): Promise<void> {
  if (uploadedFileSize <= largeFileLimit) {
    console.log(
      `Selected worker: unzipWorker (file size: ${uploadedFileSize} bytes, threshold: ${largeFileLimit} bytes)`
    );
    return unzipInWorker(zipPath, originalFileName);
  }

  try {
    console.log(
      `Selected worker: largeUnzipWorker (file size: ${uploadedFileSize} bytes, threshold: ${largeFileLimit} bytes)`
    );
    return await unzipInLargeWorker(zipPath, originalFileName);
  } catch (error) {
    if (!isUnzipUnavailableError(error)) {
      throw error;
    }

    console.warn("unzip binary is unavailable. Falling back to unzipWorker.");
    return unzipInWorker(zipPath, originalFileName);
  }
}

function resolveZipPath(fileUrl: string): string {
  try {
    const url = new URL(fileUrl);
    if (url.protocol !== "file:") {
      throw new Error(`Unsupported URL protocol: ${url.protocol}`);
    }
    return fileURLToPath(url);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Invalid file URL.";
    throw new Error(`Invalid job file URL: ${message}`);
  }
}

async function processWebhookJob(data: WebhookJobData): Promise<void> {
  const zipPath = resolveZipPath(data.fileUrl);

  if (!fs.existsSync(zipPath)) {
    throw new Error("Job archive file not found.");
  }

  const uploadedFileSize = fs.statSync(zipPath).size;

  try {
    await processZipFile(zipPath, uploadedFileSize, data.originalFileName);
  } finally {
    await cleanupArtifacts(zipPath);
  }
}

async function startWebhookQueueWorker(): Promise<void> {
  if (queueWorkerStarted) {
    return;
  }

  await registerWorker<WebhookJobData>(WEBHOOK_QUEUE_NAME, async (jobData) => {
    await processWebhookJob(jobData);
  });

  queueWorkerStarted = true;
  console.log(`Registered pg-boss worker for queue: ${WEBHOOK_QUEUE_NAME}`);
}

async function stopWebhookQueueWorker(): Promise<void> {
  await stopBoss();
  queueWorkerStarted = false;
}

async function cleanupArtifacts(archivePath?: string, extractedTo?: string): Promise<void> {
  const cleanupTasks: Promise<void>[] = [];

  if (archivePath) {
    cleanupTasks.push(
      fs.promises.rm(archivePath, { force: true }).catch((error: unknown) => {
        const message = error instanceof Error ? error.message : "Unknown cleanup error";
        console.warn(`Failed to remove uploaded archive ${archivePath}: ${message}`);
      })
    );
  }

  if (extractedTo) {
    cleanupTasks.push(
      fs.promises.rm(extractedTo, { recursive: true, force: true }).catch((error: unknown) => {
        const message = error instanceof Error ? error.message : "Unknown cleanup error";
        console.warn(`Failed to remove extracted directory ${extractedTo}: ${message}`);
      })
    );
  }

  await Promise.all(cleanupTasks);
}

app.get("/", (_req: Request, res: Response) => {
  res.json({
    message: "Webhook ZIP service is running.",
    uploadEndpoint: "POST /webhook (multipart/form-data, file field name: file)"
  });
});

app.post("/webhook", upload.single("file"), async (req: Request, res: Response) => {
  let uploadedPath: string | undefined;

  try {
    if (!req.file) {
      res.status(400).json({ error: "No file uploaded. Use form-data with field name 'file'." });
      return;
    }

    uploadedPath = req.file.path;

    if (!req.file.originalname.toLowerCase().endsWith(".zip")) {
      res.status(400).json({ error: "Uploaded file must be a .zip archive." });
      await cleanupArtifacts(uploadedPath);
      return;
    }

    if (!hasDatabaseConfig()) {
      res.status(503).json({
        error: "Queue processing is unavailable.",
        details: "Postgres connection is not configured for pg-boss."
      });
      await cleanupArtifacts(uploadedPath);
      return;
    }

    const fileUrl = pathToFileURL(req.file.path).toString();
    await enqueueJob<WebhookJobData>(WEBHOOK_QUEUE_NAME, {
      fileUrl,
      originalFileName: req.file.originalname
    });

    res.status(202).json({
      message: "ZIP received and queued for processing."
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    console.error("Failed to enqueue zip processing job:", errorMessage);

    if (errorMessage.includes("Database configuration is missing")) {
      res.status(503).json({
        error: "Queue processing is unavailable.",
        details: errorMessage
      });
    } else {
      res.status(500).json({
        error: "Failed to enqueue ZIP file.",
        details: errorMessage
      });
    }

    await cleanupArtifacts(uploadedPath);
  }
});

if (require.main === module) {
  app.listen(port, async () => {
    console.log(`Webhook ZIP service listening on port ${port}`);

    if (!hasDatabaseConfig()) {
      console.log("Postgres connection not configured. Skipping database check.");
      return;
    }

    try {
      await testDatabaseConnection();
      console.log("Connected to Postgres successfully.");
      await startWebhookQueueWorker();
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      console.error("Failed to initialize Postgres or pg-boss worker:", errorMessage);
    }
  });
}

export { app, startWebhookQueueWorker, stopWebhookQueueWorker };

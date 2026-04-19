import express, { Request, Response } from "express";
import path from "node:path";
import { Worker } from "node:worker_threads";
import { hasDatabaseConfig, testDatabaseConnection } from "./db";
import { enqueueJob, registerWorker, stopBoss } from "./pgBoss";
import { WorkerError } from "./types";

const app = express();
app.use(express.json());

const port = Number(process.env.PORT) || 3000;
const WEBHOOK_QUEUE_NAME = "webhook-zip-process";

type WebhookJobData = {
  fileUrl: string;
};

let queueWorkerStarted = false;

function runWorker(workerFileName: string, data: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    const workerPath = path.resolve(__dirname, "workers", workerFileName);
    const worker = new Worker(workerPath, {
      workerData: data
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

function unzipInUrlWorker(fileUrl: string): Promise<void> {
  return runWorker("unzipperWorker.js", { fileUrl });
}

async function processWebhookJob(data: WebhookJobData): Promise<void> {
  await unzipInUrlWorker(data.fileUrl);
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

app.get("/", (_req: Request, res: Response) => {
  res.json({
    message: "Webhook ZIP service is running.",
    uploadEndpoint: "POST /webhook (application/json: { fileUrl })"
  });
});

app.post("/webhook", async (req: Request, res: Response) => {
  try {
    const { fileUrl } = (req.body ?? {}) as Partial<WebhookJobData>;

    if (!fileUrl || typeof fileUrl !== "string") {
      res.status(400).json({ error: "fileUrl is required in JSON body." });
      return;
    }

    let parsedUrl: URL;
    try {
      parsedUrl = new URL(fileUrl);
      if (!["http:", "https:", "file:"].includes(parsedUrl.protocol)) {
        res.status(400).json({ error: "fileUrl must use http, https, or file protocol." });
        return;
      }
    } catch {
      res.status(400).json({ error: "fileUrl must be a valid absolute URL." });
      return;
    }

    // Enforce .zip only when URL path clearly exposes a filename (has an extension).
    const exposedName = path.posix.basename(parsedUrl.pathname || "");
    const hasExposedExtension = exposedName.includes(".");
    if (hasExposedExtension && !exposedName.toLowerCase().endsWith(".zip")) {
      res.status(400).json({ error: "fileUrl path filename must end with .zip when provided." });
      return;
    }

    if (!hasDatabaseConfig()) {
      res.status(503).json({
        error: "Queue processing is unavailable.",
        details: "Postgres connection is not configured for pg-boss."
      });
      return;
    }

    await enqueueJob<WebhookJobData>(WEBHOOK_QUEUE_NAME, { fileUrl });

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

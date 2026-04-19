import express, { Request, Response } from "express";
import path from "node:path";
import { availableParallelism } from "node:os";
import { FixedThreadPool } from "poolifier";
import { hasDatabaseConfig, testDatabaseConnection } from "./db";
import { createQueuedJob, getJobDetails, getJobRetries, updateJobRetry, updateJobStatus, updateJobPgbossId } from "./models/jobs";
import { enqueueJob, registerWorker, stopBoss } from "./pgBoss";
import { ZipWorkerPayload, ZipWorkerResult } from "./workers/processZipJob";

const app = express();
app.use(express.json());

const port = Number(process.env.PORT) || 3000;
const WEBHOOK_QUEUE_NAME = "webhook-zip-process";

type WebhookJobData = {
  jobId: string;
  fileUrl: string;
};

let queueWorkerStarted = false;
let unzipWorkerPool: FixedThreadPool<ZipWorkerPayload, ZipWorkerResult> | null = null;

function getUnzipWorkerPool(): FixedThreadPool<ZipWorkerPayload, ZipWorkerResult> {
  if (unzipWorkerPool) {
    return unzipWorkerPool;
  }

  const poolSize = Math.max(1, availableParallelism());
  const workerPath = path.resolve(__dirname, "workers", "unzipperPoolWorker.js");
  unzipWorkerPool = new FixedThreadPool<ZipWorkerPayload, ZipWorkerResult>(poolSize, workerPath);
  console.log(`Initialized unzip worker pool with ${poolSize} threads.`);
  return unzipWorkerPool;
}

function isRetryableError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const message = error.message.toLowerCase();
  const code = (error as NodeJS.ErrnoException).code;

  // Network-related errors
  const networkErrors = [
    "ECONNREFUSED",
    "ECONNRESET",
    "ENOTFOUND",
    "ETIMEDOUT",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "EPIPE"
  ];
  if (networkErrors.includes(code ?? "")) {
    return true;
  }

  // Message-based detection for connection/timeout issues
  const retryablePatterns = [
    "connection",
    "timeout",
    "temporarily unavailable",
    "too many connections",
    "database connection",
    "unable to reach",
    "network",
    "socket"
  ];
  if (retryablePatterns.some((pattern) => message.includes(pattern))) {
    return true;
  }

  return false;
}

function unzipInUrlWorker(fileUrl: string, jobId: string): Promise<void> {
  const pool = getUnzipWorkerPool();
  return pool.execute({ fileUrl, jobId }).then(() => undefined);
}

async function processWebhookJob(data: WebhookJobData): Promise<void> {
  await updateJobStatus(data.jobId, "started", null);

  try {
    await unzipInUrlWorker(data.fileUrl, data.jobId);
    await updateJobStatus(data.jobId, "complete", null);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown worker error";

    if (isRetryableError(error)) {
      // For retryable errors, increment retry count and reset status to queued
      // pg-boss will automatically retry this job
      const currentRetries = await getJobRetries(data.jobId);
      await updateJobRetry(data.jobId, currentRetries + 1);
      console.log(
        `Retryable error on job ${data.jobId} (retries: ${currentRetries + 1}/3): ${errorMessage}`
      );
      // Re-throw so pg-boss handles the retry
      throw error;
    } else {
      // Fatal error; mark as failed and do not retry
      await updateJobStatus(data.jobId, "error", errorMessage);
      console.log(`Fatal error on job ${data.jobId}: ${errorMessage}`);
    }
  }
}

async function startWebhookQueueWorker(): Promise<void> {
  if (queueWorkerStarted) {
    return;
  }

  getUnzipWorkerPool();

  await registerWorker<WebhookJobData>(WEBHOOK_QUEUE_NAME, async (jobData) => {
    await processWebhookJob(jobData);
  });

  queueWorkerStarted = true;
  console.log(`Registered pg-boss worker for queue: ${WEBHOOK_QUEUE_NAME}`);
}

async function stopWebhookQueueWorker(): Promise<void> {
  if (unzipWorkerPool) {
    await unzipWorkerPool.destroy();
    unzipWorkerPool = null;
  }

  await stopBoss();
  queueWorkerStarted = false;
}

app.get("/", (_req: Request, res: Response) => {
  res.json({
    message: "Webhook ZIP service is running.",
    uploadEndpoint: "POST /webhook (application/json: { fileUrl })",
    jobEndpoint: "GET /job/:id"
  });
});

app.get("/job/:id", async (req: Request, res: Response) => {
  try {
    const jobId = req.params.id;

    if (!jobId) {
      res.status(400).json({ error: "job id is required." });
      return;
    }

    if (!hasDatabaseConfig()) {
      res.status(503).json({
        error: "Job lookup is unavailable.",
        details: "Postgres connection is not configured."
      });
      return;
    }

    const result = await getJobDetails(jobId);
    if (!result) {
      res.status(404).json({ error: "Job not found." });
      return;
    }

    res.status(200).json(result);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    res.status(500).json({
      error: "Failed to fetch job.",
      details: errorMessage
    });
  }
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

    const jobId = await createQueuedJob(fileUrl);

    try {
      const pgbossJobId = await enqueueJob<WebhookJobData>(WEBHOOK_QUEUE_NAME, { fileUrl, jobId });
      await updateJobPgbossId(jobId, pgbossJobId);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      await updateJobStatus(jobId, "error", errorMessage);
      throw error;
    }

    res.status(200).json({
      message: "ZIP received and queued for processing.",
      jobId
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

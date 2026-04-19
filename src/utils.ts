import path from "node:path";
import { availableParallelism } from "node:os";
import { FixedThreadPool } from "poolifier";
import { getJobRetries, updateJobRetry, updateJobStatus } from "./models/jobs";
import { registerWorker, stopBoss } from "./pgBoss";
import { ZipWorkerPayload, ZipWorkerResult } from "./workers/processZipJob";

const DEFAULT_WORKER_CONCURRENCY = Math.max(1, availableParallelism());
export const WEBHOOK_QUEUE_NAME = "webhook-zip-process";

export type WebhookJobData = {
  jobId: string;
  fileUrl: string;
};

let queueWorkerStarted = false;
let unzipWorkerPool: FixedThreadPool<ZipWorkerPayload, ZipWorkerResult> | null = null;

export function getWorkerConcurrency(): number {
  const raw = process.env.WORKER_CONCURRENCY;

  if (!raw) {
    return DEFAULT_WORKER_CONCURRENCY;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`WORKER_CONCURRENCY must be a positive integer. Received: ${raw}`);
  }

  return parsed;
}

export function isRetryableError(error: unknown): boolean {
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
  return retryablePatterns.some((pattern) => message.includes(pattern));
}

export function createUnzipWorkerPool(
  workerConcurrency: number
): FixedThreadPool<ZipWorkerPayload, ZipWorkerResult> {
  const workerPath = path.resolve(__dirname, "workers", "unzipperPoolWorker.js");
  return new FixedThreadPool<ZipWorkerPayload, ZipWorkerResult>(workerConcurrency, workerPath);
}

export async function executeZipInWorker(
  pool: FixedThreadPool<ZipWorkerPayload, ZipWorkerResult>,
  fileUrl: string,
  jobId: string
): Promise<void> {
  await pool.execute({ fileUrl, jobId });
}

function getUnzipWorkerPool(workerConcurrency: number): FixedThreadPool<ZipWorkerPayload, ZipWorkerResult> {
  if (unzipWorkerPool) {
    return unzipWorkerPool;
  }

  unzipWorkerPool = createUnzipWorkerPool(workerConcurrency);
  console.log(`Initialized unzip worker pool with ${workerConcurrency} threads.`);
  return unzipWorkerPool;
}

async function unzipInUrlWorker(
  workerConcurrency: number,
  fileUrl: string,
  jobId: string
): Promise<void> {
  const pool = getUnzipWorkerPool(workerConcurrency);
  await executeZipInWorker(pool, fileUrl, jobId);
}

async function processWebhookJob(
  workerConcurrency: number,
  data: WebhookJobData
): Promise<void> {
  await updateJobStatus(data.jobId, "started", null);

  try {
    await unzipInUrlWorker(workerConcurrency, data.fileUrl, data.jobId);
    await updateJobStatus(data.jobId, "complete", null);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown worker error";

    if (isRetryableError(error)) {
      const currentRetries = await getJobRetries(data.jobId);
      await updateJobRetry(data.jobId, currentRetries + 1);
      console.log(
        `Retryable error on job ${data.jobId} (retries: ${currentRetries + 1}/3): ${errorMessage}`
      );
      throw error;
    }

    await updateJobStatus(data.jobId, "error", errorMessage);
    console.log(`Fatal error on job ${data.jobId}: ${errorMessage}`);
  }
}

export async function startWebhookQueueWorker(workerConcurrency = getWorkerConcurrency()): Promise<void> {
  if (queueWorkerStarted) {
    return;
  }

  getUnzipWorkerPool(workerConcurrency);

  await registerWorker<WebhookJobData>(
    WEBHOOK_QUEUE_NAME,
    async (jobData) => {
      await processWebhookJob(workerConcurrency, jobData);
    },
    workerConcurrency
  );

  queueWorkerStarted = true;
  console.log(
    `Registered pg-boss worker for queue: ${WEBHOOK_QUEUE_NAME} (concurrency: ${workerConcurrency})`
  );
}

export async function stopWebhookQueueWorker(): Promise<void> {
  if (unzipWorkerPool) {
    await unzipWorkerPool.destroy();
    unzipWorkerPool = null;
  }

  await stopBoss();
  queueWorkerStarted = false;
}
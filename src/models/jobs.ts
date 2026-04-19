import { randomUUID } from "node:crypto";
import { getDbPool } from "../db";

export type JobStatus = "queued" | "started" | "complete" | "error";

export async function createQueuedJob(fileUrl: string): Promise<string> {
  const pool = getDbPool();
  const jobId = randomUUID();

  await pool.query(
    `INSERT INTO jobs (job_id, url, status, retries, error)
     VALUES ($1, $2, 'queued', 0, NULL)`,
    [jobId, fileUrl]
  );

  return jobId;
}

export async function updateJobStatus(
  jobId: string,
  status: JobStatus,
  errorMessage: string | null
): Promise<void> {
  const pool = getDbPool();

  await pool.query(
    `UPDATE jobs
     SET status = $2,
         error = $3,
         updated_at = NOW()
     WHERE job_id = $1`,
    [jobId, status, errorMessage]
  );
}

import { randomUUID } from "node:crypto";
import { getDbPool } from "../db";

export type JobStatus = "queued" | "started" | "complete" | "error";

type JobRow = {
  job_id: string;
  pgboss_job_id: string | null;
  url: string;
  status: JobStatus;
  retries: number;
  error: string | null;
  created_at: Date;
  updated_at: Date;
};

type HoldingRow = {
  id: string;
  account_id: string;
  ticker: string;
  cusip: string | null;
  description: string | null;
  quantity: string;
  market_value: string;
  cost_basis: string;
  price: string;
  asset_class: string | null;
  created_at: Date;
  updated_at: Date;
};

type AccountRow = {
  account_id: string;
  client_id: string;
  account_type: string;
  custodian: string;
  opened_date: string;
  status: string;
  cash_balance: string;
  total_value: string;
  created_at: Date;
  updated_at: Date;
};

type FileRow = {
  file_name: string;
  job_id: string | null;
  client_id: string;
  first_name: string;
  last_name: string;
  email: string;
  advisor_id: string;
  last_updated: Date;
  created_at: Date;
  updated_at: Date;
};

export type JobDetailsHolding = HoldingRow;

export type JobDetailsAccount = AccountRow & {
  holdings: JobDetailsHolding[];
};

export type JobDetailsFile = FileRow & {
  accounts: JobDetailsAccount[];
};

export type JobDetailsResult = {
  job: JobRow;
  files?: JobDetailsFile[];
};

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

export async function updateJobPgbossId(jobId: string, pgbossJobId: string): Promise<void> {
  const pool = getDbPool();

  await pool.query(
    `UPDATE jobs
     SET pgboss_job_id = $2,
         updated_at = NOW()
     WHERE job_id = $1`,
    [jobId, pgbossJobId]
  );
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

export async function updateJobRetry(
  jobId: string,
  retryCount: number
): Promise<void> {
  const pool = getDbPool();

  await pool.query(
    `UPDATE jobs
     SET retries = $2,
         status = 'queued',
         error = NULL,
         updated_at = NOW()
     WHERE job_id = $1`,
    [jobId, retryCount]
  );
}

export async function getJobRetries(jobId: string): Promise<number> {
  const pool = getDbPool();

  const result = await pool.query<{ retries: number }>(
    `SELECT retries FROM jobs WHERE job_id = $1`,
    [jobId]
  );

  if (result.rows.length === 0) {
    return 0;
  }

  return result.rows[0].retries;
}

export async function getJobDetails(jobId: string): Promise<JobDetailsResult | null> {
  const pool = getDbPool();

  const jobResult = await pool.query<JobRow>(
    `SELECT job_id, url, status, retries, error, created_at, updated_at
     FROM jobs
     WHERE job_id = $1`,
    [jobId]
  );

  if (jobResult.rows.length === 0) {
    return null;
  }

  const job = jobResult.rows[0];

  if (job.status !== "started" && job.status !== "complete") {
    return { job };
  }

  const filesResult = await pool.query<FileRow>(
    `SELECT file_name, job_id, client_id, first_name, last_name, email, advisor_id,
            last_updated, created_at, updated_at
     FROM files
     WHERE job_id = $1
     ORDER BY client_id`,
    [jobId]
  );

  const files: JobDetailsFile[] = filesResult.rows.map((file) => ({
    ...file,
    accounts: []
  }));

  if (files.length === 0) {
    return { job, files };
  }

  const clientIds = files.map((file) => file.client_id);
  const accountsResult = await pool.query<AccountRow>(
    `SELECT account_id, client_id, account_type, custodian, opened_date,
            status, cash_balance::numeric::text AS cash_balance,
            total_value::numeric::text AS total_value,
            created_at, updated_at
     FROM accounts
     WHERE client_id = ANY($1::text[])
     ORDER BY client_id, account_id`,
    [clientIds]
  );

  const accountsByClientId = new Map<string, JobDetailsAccount[]>();
  const accountIds: string[] = [];
  for (const account of accountsResult.rows) {
    const hydrated: JobDetailsAccount = {
      ...account,
      holdings: []
    };

    const existing = accountsByClientId.get(account.client_id) ?? [];
    existing.push(hydrated);
    accountsByClientId.set(account.client_id, existing);
    accountIds.push(account.account_id);
  }

  for (const file of files) {
    file.accounts = accountsByClientId.get(file.client_id) ?? [];
  }

  if (accountIds.length === 0) {
    return { job, files };
  }

  const holdingsResult = await pool.query<HoldingRow>(
    `SELECT id::text, account_id, ticker, cusip, description,
            quantity::numeric::text AS quantity,
            market_value::numeric::text AS market_value,
            cost_basis::numeric::text AS cost_basis,
            price::numeric::text AS price,
            asset_class, created_at, updated_at
     FROM holdings
     WHERE account_id = ANY($1::text[])
     ORDER BY account_id, id`,
    [accountIds]
  );

  const accountsById = new Map<string, JobDetailsAccount>();
  for (const file of files) {
    for (const account of file.accounts) {
      accountsById.set(account.account_id, account);
    }
  }

  for (const holding of holdingsResult.rows) {
    const account = accountsById.get(holding.account_id);
    if (account) {
      account.holdings.push(holding);
    }
  }

  return { job, files };
}

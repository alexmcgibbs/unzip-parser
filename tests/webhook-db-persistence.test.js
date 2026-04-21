const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const request = require("supertest");
const { Pool } = require("pg");

process.env.DB_HOST ||= "localhost";
process.env.DB_PORT ||= "5432";
process.env.DB_NAME ||= "webhook_service";
process.env.DB_USER ||= "webhook_user";
process.env.DB_PASSWORD ||= "webhook_password";
process.env.WORKER_CONCURRENCY ||= "2";
process.env.WEBHOOK_QUEUE_NAME ||= `webhook-zip-process-dbtest-${process.pid}`;

const { app, startWebhookQueueWorker, stopWebhookQueueWorker } = require("../dist/index.js");
const WEBHOOK_QUEUE_NAME = process.env.WEBHOOK_QUEUE_NAME;
let appServer;
let appBaseUrl;

function getReachableBaseUrl(address) {
  if (!address || typeof address === "string") {
    throw new Error("Failed to resolve app test server address.");
  }

  return address.family === "IPv6"
    ? `http://[::1]:${address.port}`
    : `http://127.0.0.1:${address.port}`;
}

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD
});

async function canConnectToDb() {
  const client = await pool.connect();
  try {
    await client.query("SELECT 1");
    return true;
  } finally {
    client.release();
  }
}

async function hasRequiredTables() {
  const result = await pool.query(
    `SELECT
       to_regclass('public.jobs') AS jobs_table,
       to_regclass('public.files') AS files_table,
       to_regclass('public.accounts') AS accounts_table,
       to_regclass('public.holdings') AS holdings_table`
  );

  const row = result.rows[0];
  return Boolean(row.jobs_table && row.files_table && row.accounts_table && row.holdings_table);
}

async function clearPendingWebhookQueueJobs() {
  try {
    await pool.query(
      `DELETE FROM pgboss.job
       WHERE name = $1`,
      [WEBHOOK_QUEUE_NAME]
    );
  } catch {
    // Skip cleanup when pgboss schema is unavailable in the current environment.
  }
}

test.before(async () => {
  appServer = await new Promise((resolve, reject) => {
    const server = app.listen(0, () => resolve(server));
    server.once("error", reject);
  });

  const address = appServer.address();
  appBaseUrl = getReachableBaseUrl(address);
});

async function waitForCondition(check, timeoutMs = 45000, intervalMs = 500) {
  const start = Date.now();
  let lastError = null;

  while (Date.now() - start < timeoutMs) {
    try {
      if (await check()) {
        return;
      }
    } catch (error) {
      lastError = error;
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  const elapsedMs = Date.now() - start;
  const lastErrorMessage =
    lastError instanceof Error ? ` Last poll error: ${lastError.message}` : "";

  throw new Error(
    `Timed out waiting for queued webhook processing to complete after ${elapsedMs}ms.${lastErrorMessage}`
  );
}

test("persists files, accounts, and holdings rows after webhook upload", async (t) => {
  let dbReady = false;
  try {
    dbReady = await canConnectToDb();
  } catch {
    t.skip("Postgres is not reachable; skipping DB persistence test.");
    return;
  }

  if (!dbReady) {
    t.skip("Postgres is not reachable; skipping DB persistence test.");
    return;
  }

  if (!(await hasRequiredTables())) {
    t.skip("Required tables are missing; run migrations before DB persistence test.");
    return;
  }

  const projectRoot = path.resolve(__dirname, "..");
  const sampleJsonPath = path.join(projectRoot, "sample.json");
  const expectedPayload = JSON.parse(fs.readFileSync(sampleJsonPath, "utf8"));
  const expectedHoldingsCount = expectedPayload.accounts.reduce(
    (sum, account) => sum + account.holdings.length,
    0
  );

  const sampleZipUrl = `${appBaseUrl}/test?file=sample.zip`;

  await clearPendingWebhookQueueJobs();

  await request(appServer).get("/test?file=sample.zip").expect(200);

  await startWebhookQueueWorker();

  const response = await request(app)
    .post("/webhook")
    .send({ fileUrl: sampleZipUrl })
    .expect(200);

  assert.equal(response.body.message, "ZIP received and queued for processing.");
  assert.equal(typeof response.body.jobId, "string");
  assert.ok(response.body.jobId.length > 0);

  await waitForCondition(async () => {
    const jobResult = await pool.query(
      `SELECT status, error, retries
       FROM jobs
       WHERE job_id = $1`,
      [response.body.jobId]
    );

    if (jobResult.rows.length === 0) {
      return false;
    }

    if (jobResult.rows[0].status === "error") {
      throw new Error(
        `Job entered error status. retries=${jobResult.rows[0].retries}, error=${jobResult.rows[0].error ?? "unknown"}`
      );
    }

    return jobResult.rows[0].status === "complete";
  });

  await waitForCondition(async () => {
    const countResult = await pool.query(
      `SELECT COUNT(*)::int AS total
       FROM files
       WHERE client_id = $1
         AND job_id = $2`,
      [expectedPayload.client_id, response.body.jobId]
    );

    return countResult.rows[0].total === 1;
  });

  const fileCountResult = await pool.query(
    `SELECT COUNT(*)::int AS total
     FROM files
     WHERE client_id = $1
       AND job_id = $2`,
    [expectedPayload.client_id, response.body.jobId]
  );

  assert.equal(fileCountResult.rows[0].total, 1, "Expected one files row for matching client_id/job_id");

  const filesResult = await pool.query(
    `SELECT file_name, job_id, client_id, first_name, last_name, email, advisor_id
     FROM files
     WHERE client_id = $1
       AND job_id = $2`,
    [expectedPayload.client_id, response.body.jobId]
  );

  assert.equal(filesResult.rows.length, 1, "Expected one files row for the client_id/job_id pair");
  const fileRow = filesResult.rows[0];
  assert.equal(fileRow.file_name, "sample.json");
  assert.equal(fileRow.job_id, response.body.jobId);
  assert.equal(fileRow.client_id, expectedPayload.client_id);
  assert.equal(fileRow.first_name, expectedPayload.first_name);
  assert.equal(fileRow.last_name, expectedPayload.last_name);
  assert.equal(fileRow.email, expectedPayload.email);
  assert.equal(fileRow.advisor_id, expectedPayload.advisor_id);

  const accountsResult = await pool.query(
    `SELECT account_id, account_type, custodian, status, cash_balance::numeric::text AS cash_balance,
            total_value::numeric::text AS total_value
     FROM accounts
     WHERE client_id = $1
     ORDER BY account_id`,
    [expectedPayload.client_id]
  );

  assert.equal(accountsResult.rows.length, expectedPayload.accounts.length);

  const expectedAccountsById = new Map(
    expectedPayload.accounts.map((account) => [account.account_id, account])
  );

  for (const row of accountsResult.rows) {
    const expected = expectedAccountsById.get(row.account_id);
    assert.ok(expected, `Unexpected account_id row returned: ${row.account_id}`);
    assert.equal(row.account_type, expected.account_type);
    assert.equal(row.custodian, expected.custodian);
    assert.equal(row.status, expected.status);
    assert.equal(Number(row.cash_balance), Number(expected.cash_balance));
    assert.equal(Number(row.total_value), Number(expected.total_value));
  }

  const holdingsCountResult = await pool.query(
    `SELECT COUNT(*)::int AS total
     FROM holdings h
     JOIN accounts a ON a.account_id = h.account_id
     WHERE a.client_id = $1`,
    [expectedPayload.client_id]
  );

  assert.equal(holdingsCountResult.rows[0].total, expectedHoldingsCount);

  const jobLookupResponse = await request(app).get(`/job/${response.body.jobId}`).expect(200);
  assert.equal(jobLookupResponse.body.job.job_id, response.body.jobId);
  assert.equal(jobLookupResponse.body.job.status, "complete");
  assert.ok(Array.isArray(jobLookupResponse.body.files));
  assert.equal(jobLookupResponse.body.files.length, 1);

  const lookedUpFile = jobLookupResponse.body.files[0];
  assert.equal(lookedUpFile.file_name, "sample.json");
  assert.equal(lookedUpFile.job_id, response.body.jobId);
  assert.equal(lookedUpFile.client_id, expectedPayload.client_id);
  assert.ok(Array.isArray(lookedUpFile.accounts));
  assert.equal(lookedUpFile.accounts.length, expectedPayload.accounts.length);

  const lookedUpHoldingsCount = lookedUpFile.accounts.reduce(
    (sum, account) => sum + account.holdings.length,
    0
  );
  assert.equal(lookedUpHoldingsCount, expectedHoldingsCount);

  await pool.query("DELETE FROM files WHERE job_id = $1 AND client_id = $2", [
    response.body.jobId,
    expectedPayload.client_id
  ]);
  await pool.query("DELETE FROM jobs WHERE job_id = $1", [response.body.jobId]);
});

test.after(async () => {
  if (appServer) {
    await new Promise((resolve, reject) => {
      appServer.close((err) => (err ? reject(err) : resolve()));
    });
  }

  await stopWebhookQueueWorker();
  await pool.end();
});

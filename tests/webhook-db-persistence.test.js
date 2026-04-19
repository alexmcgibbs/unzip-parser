const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const request = require("supertest");
const { Pool } = require("pg");

process.env.DB_HOST ||= "localhost";
process.env.DB_PORT ||= "5432";
process.env.DB_NAME ||= "webhook_service";
process.env.DB_USER ||= "webhook_user";
process.env.DB_PASSWORD ||= "webhook_password";

const { app, startWebhookQueueWorker, stopWebhookQueueWorker } = require("../dist/index.js");

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
       to_regclass('public.files') AS files_table,
       to_regclass('public.accounts') AS accounts_table,
       to_regclass('public.holdings') AS holdings_table`
  );

  const row = result.rows[0];
  return Boolean(row.files_table && row.accounts_table && row.holdings_table);
}

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
  const sampleZipPath = path.join(projectRoot, "sample.zip");
  const sampleZipUrl = pathToFileURL(sampleZipPath).toString();
  const sampleJsonPath = path.join(projectRoot, "sample.json");
  const expectedPayload = JSON.parse(fs.readFileSync(sampleJsonPath, "utf8"));
  const expectedHoldingsCount = expectedPayload.accounts.reduce(
    (sum, account) => sum + account.holdings.length,
    0
  );

  await pool.query("DELETE FROM files WHERE client_id = $1", [expectedPayload.client_id]);

  await startWebhookQueueWorker();

  const response = await request(app)
    .post("/webhook")
    .send({ fileUrl: sampleZipUrl })
    .expect(202);

  assert.equal(response.body.message, "ZIP received and queued for processing.");

  await waitForCondition(async () => {
    const countResult = await pool.query(
      `SELECT COUNT(*)::int AS total
       FROM files
       WHERE client_id = $1`,
      [expectedPayload.client_id]
    );

    return countResult.rows[0].total === 1;
  });

  const fileCountResult = await pool.query(
    `SELECT COUNT(*)::int AS total
     FROM files
     WHERE client_id = $1`,
    [expectedPayload.client_id]
  );

  assert.equal(fileCountResult.rows[0].total, 1, "Expected one files row after upload");

  const filesResult = await pool.query(
    `SELECT file_name, client_id, first_name, last_name, email, advisor_id
     FROM files
     WHERE client_id = $1`,
    [expectedPayload.client_id]
  );

  assert.equal(filesResult.rows.length, 1, "Expected one files row for the client_id");
  const fileRow = filesResult.rows[0];
  assert.equal(fileRow.file_name, "sample.json");
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

  await pool.query("DELETE FROM files WHERE client_id = $1", [expectedPayload.client_id]);
});

test.after(async () => {
  await stopWebhookQueueWorker();
  await pool.end();
});

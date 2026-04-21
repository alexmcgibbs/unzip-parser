const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { FixedThreadPool } = require("poolifier");
const { app } = require("../dist/index.js");

const workerPath = path.resolve(__dirname, "../dist/workers/unzipperPoolWorker.js");
let pool;
let appServer;
let sampleZipUrl;
let missingZipUrl;

function getReachableBaseUrl(address) {
  if (!address || typeof address === "string") {
    throw new Error("Failed to resolve app test server address.");
  }

  return address.family === "IPv6"
    ? `http://[::1]:${address.port}`
    : `http://127.0.0.1:${address.port}`;
}

test.before(async () => {
  pool = new FixedThreadPool(1, workerPath);

  appServer = await new Promise((resolve, reject) => {
    const server = app.listen(0, () => resolve(server));
    server.once("error", reject);
  });

  const address = appServer.address();
  const appBaseUrl = getReachableBaseUrl(address);

  sampleZipUrl = `${appBaseUrl}/test?file=sample.zip`;
  missingZipUrl = `${appBaseUrl}/test?file=missing.zip`;
});

test.after(async () => {
  if (appServer) {
    await new Promise((resolve, reject) => {
      appServer.close((err) => (err ? reject(err) : resolve()));
    });
  }

  await pool.destroy();
});

test("unzipperPoolWorker streams ZIP from http URL", async () => {
  const result = await pool.execute({ fileUrl: sampleZipUrl, jobId: "test-job-id" });
  assert.equal(result.ok, true);
  assert.equal(result.resolvedName, "sample.json");
});

test("unzipperPoolWorker resolves name from ZIP entries", async () => {
  const result = await pool.execute({ fileUrl: sampleZipUrl, jobId: "test-job-id-2" });
  assert.equal(result.ok, true);
  assert.ok(typeof result.resolvedName === "string" && result.resolvedName.length > 0);
  assert.ok(!path.isAbsolute(result.resolvedName));
  assert.equal(result.resolvedName, "sample.json");
});

test("unzipperPoolWorker throws clear error on 404 URL", async () => {
  await assert.rejects(
    pool.execute({ fileUrl: missingZipUrl, jobId: "test-404" }),
    /Failed to download ZIP\. HTTP 404/i
  );
});

test("unzipperPoolWorker throws clear error on invalid URL", async () => {
  await assert.rejects(
    pool.execute({ fileUrl: "not-a-valid-url", jobId: "test-invalid" }),
    /Invalid fileUrl\. Expected absolute URL/i
  );
});

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { FixedThreadPool } = require("poolifier");

const workerPath = path.resolve(__dirname, "../dist/workers/unzipperPoolWorker.js");
let pool;

test.before(() => {
  pool = new FixedThreadPool(1, workerPath);
});

test.after(async () => {
  await pool.destroy();
});

function startZipServer(zipPath) {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      if (req.url !== "/sample.zip") {
        res.statusCode = 404;
        res.end("Not found");
        return;
      }

      res.setHeader("content-type", "application/zip");
      fs.createReadStream(zipPath).pipe(res);
    });

    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Failed to resolve test HTTP server address."));
        return;
      }

      resolve({
        server,
        fileUrl: `http://127.0.0.1:${address.port}/sample.zip`
      });
    });
  });
}

async function withZipServer(fn) {
  const projectRoot = path.resolve(__dirname, "..");
  const sampleZipPath = path.join(projectRoot, "sample.zip");
  const started = await startZipServer(sampleZipPath);
  try {
    await fn(started.fileUrl, started.server);
  } finally {
    await new Promise((resolve, reject) => {
      started.server.close((err) => (err ? reject(err) : resolve()));
    });
  }
}

test("unzipperPoolWorker streams ZIP from http URL", async () => {
  await withZipServer(async (fileUrl) => {
    const result = await pool.execute({ fileUrl, jobId: "test-job-id" });
    assert.equal(result.ok, true);
    assert.equal(result.resolvedName, "sample.json");
  });
});

test("unzipperPoolWorker resolves name from ZIP entries", async () => {
  await withZipServer(async (fileUrl) => {
    const result = await pool.execute({ fileUrl, jobId: "test-job-id-2" });
    assert.equal(result.ok, true);
    assert.ok(typeof result.resolvedName === "string" && result.resolvedName.length > 0);
    assert.ok(!path.isAbsolute(result.resolvedName));
    assert.equal(result.resolvedName, "sample.json");
  });
});

test("unzipperPoolWorker throws clear error on 404 URL", async () => {
  await withZipServer(async (fileUrl) => {
    const missingUrl = fileUrl.replace("/sample.zip", "/missing.zip");
    await assert.rejects(
      pool.execute({ fileUrl: missingUrl, jobId: "test-404" }),
      /Failed to download ZIP\. HTTP 404/i
    );
  });
});

test("unzipperPoolWorker throws clear error on invalid URL", async () => {
  await assert.rejects(
    pool.execute({ fileUrl: "not-a-valid-url", jobId: "test-invalid" }),
    /Invalid fileUrl\. Expected absolute URL/i
  );
});

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { Worker } = require("node:worker_threads");

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

function runUnzipperWorker(fileUrl) {
  return new Promise((resolve, reject) => {
    const workerPath = path.resolve(__dirname, "../dist/workers/unzipperWorker.js");
    const worker = new Worker(workerPath, {
      workerData: { fileUrl, jobId: "test-job-id" }
    });

    worker.once("message", (data) => {
      if (data && data.error) {
        reject(new Error(data.error));
        return;
      }

      resolve(data);
    });

    worker.once("error", reject);
    worker.once("exit", (code) => {
      if (code !== 0) {
        reject(new Error(`Worker exited with code ${code}`));
      }
    });
  });
}

async function withZipServer(fn) {
  const projectRoot = path.resolve(__dirname, "..");
  const sampleZipPath = path.join(projectRoot, "sample.zip");
  const started = await startZipServer(sampleZipPath);
  try {
    await fn(started.fileUrl);
  } finally {
    await new Promise((resolve, reject) => {
      started.server.close((err) => (err ? reject(err) : resolve()));
    });
  }
}

test("unzipperWorker streams ZIP from http URL", async () => {
  await withZipServer(async (fileUrl) => {
    const result = await runUnzipperWorker(fileUrl);

    assert.equal(result.ok, true);
    assert.equal(result.resolvedName, "sample.json");
  });
});

test("unzipperWorker resolves name from ZIP entries when filename is omitted", async () => {
  await withZipServer(async (fileUrl) => {
    // Worker derives name from extracted ZIP entry path
    const result = await runUnzipperWorker(fileUrl);

    assert.equal(result.ok, true);

    // Must be derived from an entry path inside the ZIP, not any repo filesystem path
    assert.ok(
      typeof result.resolvedName === "string" && result.resolvedName.length > 0,
      "resolvedName should be a non-empty string"
    );
    assert.ok(
      !path.isAbsolute(result.resolvedName),
      `resolvedName should not be an absolute filesystem path, got: ${result.resolvedName}`
    );
    assert.equal(result.resolvedName, "sample.json");
  });
});

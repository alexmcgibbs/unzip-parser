const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const request = require("supertest");

function hasUnzipCommand() {
  const result = spawnSync("unzip", ["-v"], { stdio: "ignore" });
  return result.status === 0;
}

test("uses largeUnzipWorker when uploaded file exceeds LARGE_FILE_LIMIT", async (t) => {
  if (!hasUnzipCommand()) {
    t.skip("unzip command is not available; skipping large worker test.");
    return;
  }

  const previousLimit = process.env.LARGE_FILE_LIMIT;
  process.env.LARGE_FILE_LIMIT = "1b";

  const appModulePath = require.resolve("../dist/index.js");
  delete require.cache[appModulePath];

  const { app } = require("../dist/index.js");

  try {
    const projectRoot = path.resolve(__dirname, "..");
    const sampleZipPath = path.join(projectRoot, "sample.zip");
    const sampleJsonPath = path.join(projectRoot, "sample.json");
    const expectedPayload = JSON.parse(fs.readFileSync(sampleJsonPath, "utf8"));

    const response = await request(app)
      .post("/webhook")
      .attach("file", sampleZipPath)
      .expect(200);

    assert.equal(response.body.message, "ZIP received and processed successfully.");
    assert.deepEqual(response.body.jsonContents, expectedPayload);
    assert.ok(response.body.result, "Expected result object in response body");
    assert.equal(
      response.body.result.extractedTo,
      path.join(path.dirname(response.body.result.archivePath), "[memory]"),
      "Expected in-memory large worker marker in extractedTo"
    );
    assert.ok(
      response.body.result.extractedFiles.includes("sample.json"),
      "Expected extracted file list to include sample.json"
    );

    const archivePath = response.body.result.archivePath;
    if (archivePath && fs.existsSync(archivePath)) {
      fs.rmSync(archivePath, { force: true });
    }
  } finally {
    if (previousLimit === undefined) {
      delete process.env.LARGE_FILE_LIMIT;
    } else {
      process.env.LARGE_FILE_LIMIT = previousLimit;
    }

    delete require.cache[appModulePath];
  }
});
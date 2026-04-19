const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const request = require("supertest");

const { app } = require("../dist/index.js");

test("uploads sample.zip and matches sample.json payload", async () => {
  const projectRoot = path.resolve(__dirname, "..");
  const sampleZipPath = path.join(projectRoot, "sample.zip");
  const sampleJsonPath = path.join(projectRoot, "sample.json");

  const expectedPayload = JSON.parse(fs.readFileSync(sampleJsonPath, "utf8"));

  const response = await request(app)
    .post("/webhook")
    .attach("file", sampleZipPath)
    .expect(200);

  assert.equal(response.body.message, "ZIP received and processed successfully.");
  assert.ok(response.body.jsonContents, "Expected jsonContents in response body");
  assert.deepEqual(response.body.jsonContents, expectedPayload);

  assert.ok(response.body.result, "Expected result object in response body");
  assert.ok(Array.isArray(response.body.result.extractedFiles), "Expected extractedFiles array");
  assert.ok(
    response.body.result.extractedFiles.includes("sample.json"),
    "Expected extracted ZIP file list to include sample.json"
  );

  // Clean up artifacts created by this test run.
  const extractedTo = response.body.result.extractedTo;
  const archivePath = response.body.result.archivePath;
  if (archivePath && fs.existsSync(archivePath)) {
    fs.rmSync(archivePath, { force: true });
  }
  if (extractedTo && fs.existsSync(extractedTo)) {
    fs.rmSync(extractedTo, { recursive: true, force: true });
  }
});

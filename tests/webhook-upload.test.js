const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const request = require("supertest");

const { app } = require("../dist/index.js");

test("uploads sample.zip and returns success message", async () => {
  const projectRoot = path.resolve(__dirname, "..");
  const sampleZipPath = path.join(projectRoot, "sample.zip");

  const response = await request(app)
    .post("/webhook")
    .attach("file", sampleZipPath)
    .expect(200);

    assert.equal(response.body.message, "ZIP received, processed, and persisted successfully.");
    assert.deepEqual(Object.keys(response.body), ["message"]);
});

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const request = require("supertest");

process.env.DB_HOST ||= "localhost";
process.env.DB_PORT ||= "5432";
process.env.DB_NAME ||= "webhook_service";
process.env.DB_USER ||= "webhook_user";
process.env.DB_PASSWORD ||= "webhook_password";

const { app, stopWebhookQueueWorker } = require("../dist/index.js");

test("uploads sample.zip and returns success message", async () => {
  const projectRoot = path.resolve(__dirname, "..");
  const sampleZipPath = path.join(projectRoot, "sample.zip");

  const response = await request(app)
    .post("/webhook")
    .attach("file", sampleZipPath)
    .expect(202);

  assert.equal(response.body.message, "ZIP received and queued for processing.");
  assert.deepEqual(Object.keys(response.body), ["message"]);
});

test.after(async () => {
  await stopWebhookQueueWorker();
});

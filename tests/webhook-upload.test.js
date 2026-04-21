const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const request = require("supertest");

process.env.DB_HOST ||= "localhost";
process.env.DB_PORT ||= "5432";
process.env.DB_NAME ||= "webhook_service";
process.env.DB_USER ||= "webhook_user";
process.env.DB_PASSWORD ||= "webhook_password";
process.env.WEBHOOK_QUEUE_NAME ||= `webhook-zip-process-uploadtest-${process.pid}`;

const { app, stopWebhookQueueWorker } = require("../dist/index.js");

test("queues sample.zip URL and returns success message", async () => {
  const projectRoot = path.resolve(__dirname, "..");
  const sampleZipPath = path.join(projectRoot, "sample.zip");
  const sampleZipUrl = pathToFileURL(sampleZipPath).toString();

  const response = await request(app)
    .post("/webhook")
    .send({ fileUrl: sampleZipUrl })
    .expect(200);

  assert.equal(response.body.message, "ZIP received and queued for processing.");
  assert.equal(typeof response.body.jobId, "string");
  assert.ok(response.body.jobId.length > 0);
});

test.after(async () => {
  await stopWebhookQueueWorker();
});

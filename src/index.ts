import express, { Request, Response } from "express";
import path from "node:path";
import fs from "node:fs";
import { hasDatabaseConfig, testDatabaseConnection } from "./db";
import { createQueuedJob, getJobDetails, updateJobStatus, updateJobPgbossId } from "./models/jobs";
import { enqueueJob } from "./pgBoss";
import { startWebhookQueueWorker, stopWebhookQueueWorker, WEBHOOK_QUEUE_NAME, WebhookJobData } from "./utils";

const app = express();
app.use(express.json());

const port = Number(process.env.PORT) || 3000;
const NODE_ENV = process.env.NODE_ENV ?? "development";

app.get("/", (_req: Request, res: Response) => {
  res.json({
    message: "Webhook ZIP service is running.",
    uploadEndpoint: "POST /webhook (application/json: { fileUrl })",
    jobEndpoint: "GET /job/:id"
  });
});

app.get("/job/:id", async (req: Request, res: Response) => {
  try {
    const jobId = req.params.id;

    if (!jobId) {
      res.status(400).json({ error: "job id is required." });
      return;
    }

    if (!hasDatabaseConfig()) {
      res.status(503).json({
        error: "Job lookup is unavailable.",
        details: "Postgres connection is not configured."
      });
      return;
    }

    const result = await getJobDetails(jobId);
    if (!result) {
      res.status(404).json({ error: "Job not found." });
      return;
    }

    res.status(200).json(result);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    res.status(500).json({
      error: "Failed to fetch job.",
      details: errorMessage
    });
  }
});

if (NODE_ENV !== "production") {
  app.get("/test", (req: Request, res: Response) => {
    const uploadsTestDir = path.resolve(process.cwd(), "uploads", "test");
    const uploadsDir = path.resolve(process.cwd(), "uploads");
    const sourceDirs = [uploadsTestDir, uploadsDir];

    const filesByDir = new Map<string, string[]>();
    for (const dir of sourceDirs) {
      try {
        const files = fs.readdirSync(dir)
          .filter((f) => fs.statSync(path.join(dir, f)).isFile())
          .sort((a, b) => a.localeCompare(b));
        filesByDir.set(dir, files);
      } catch {
        filesByDir.set(dir, []);
      }
    }

    const hasAnyFiles = sourceDirs.some((dir) => (filesByDir.get(dir) ?? []).length > 0);
    if (!hasAnyFiles) {
      res.status(404).json({ error: "No files found in uploads/test or uploads directory." });
      return;
    }

    const requestedFile = typeof req.query.file === "string" ? req.query.file : "";
    const normalizedRequested = path.posix.basename(requestedFile.trim());

    if (requestedFile && normalizedRequested !== requestedFile.trim()) {
      res.status(400).json({ error: "Invalid file query parameter." });
      return;
    }

    let selectedFile = "";
    let selectedDir = "";

    if (normalizedRequested) {
      for (const dir of sourceDirs) {
        const files = filesByDir.get(dir) ?? [];
        const exactMatch = files.find((file) => file === normalizedRequested);
        if (exactMatch) {
          selectedFile = exactMatch;
          selectedDir = dir;
          break;
        }
      }

      if (!selectedFile) {
        res.status(404).json({ error: `File not found in uploads/test: ${normalizedRequested}` });
        return;
      }
    } else {
      for (const dir of sourceDirs) {
        const files = filesByDir.get(dir) ?? [];
        if (files.length > 0) {
          selectedFile = files[0];
          selectedDir = dir;
          break;
        }
      }
    }

    const filePath = path.join(selectedDir, selectedFile);
    res.download(filePath, selectedFile, (err) => {
      if (err && !res.headersSent) {
        res.status(404).json({ error: "File not found." });
      }
    });
  });
}

app.post("/webhook", async (req: Request, res: Response) => {
  try {
    const { fileUrl } = (req.body ?? {}) as Partial<WebhookJobData>;

    if (!fileUrl || typeof fileUrl !== "string") {
      res.status(400).json({ error: "fileUrl is required in JSON body." });
      return;
    }

    let parsedUrl: URL;
    try {
      parsedUrl = new URL(fileUrl);
      if (!["http:", "https:", "file:"].includes(parsedUrl.protocol)) {
        res.status(400).json({ error: "fileUrl must use http, https, or file protocol." });
        return;
      }
    } catch {
      res.status(400).json({ error: "fileUrl must be a valid absolute URL." });
      return;
    }

    // Enforce .zip only when URL path clearly exposes a filename (has an extension).
    const exposedName = path.posix.basename(parsedUrl.pathname || "");
    const hasExposedExtension = exposedName.includes(".");
    if (hasExposedExtension && !exposedName.toLowerCase().endsWith(".zip")) {
      res.status(400).json({ error: "fileUrl path filename must end with .zip when provided." });
      return;
    }

    if (!hasDatabaseConfig()) {
      res.status(503).json({
        error: "Queue processing is unavailable.",
        details: "Postgres connection is not configured for pg-boss."
      });
      return;
    }

    const jobId = await createQueuedJob(fileUrl);

    try {
      const pgbossJobId = await enqueueJob<WebhookJobData>(WEBHOOK_QUEUE_NAME, { fileUrl, jobId });
      await updateJobPgbossId(jobId, pgbossJobId);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      await updateJobStatus(jobId, "error", errorMessage);
      throw error;
    }

    res.status(200).json({
      message: "ZIP received and queued for processing.",
      jobId
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    console.error("Failed to enqueue zip processing job:", errorMessage);

    if (errorMessage.includes("Database configuration is missing")) {
      res.status(503).json({
        error: "Queue processing is unavailable.",
        details: errorMessage
      });
    } else {
      res.status(500).json({
        error: "Failed to enqueue ZIP file.",
        details: errorMessage
      });
    }
  }
});

if (require.main === module) {
  app.listen(port, async () => {
    console.log(`Webhook ZIP service listening on port ${port}`);

    if (!hasDatabaseConfig()) {
      console.log("Postgres connection not configured. Skipping database check.");
      return;
    }

    try {
      await testDatabaseConnection();
      console.log("Connected to Postgres successfully.");
      await startWebhookQueueWorker();
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      console.error("Failed to initialize Postgres or pg-boss worker:", errorMessage);
    }
  });
}

export { app, startWebhookQueueWorker, stopWebhookQueueWorker };

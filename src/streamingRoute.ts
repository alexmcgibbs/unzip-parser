import express, { Request, Response } from "express";
import multer from "multer";
import fs from "node:fs";
import path from "node:path";
import { hasDatabaseConfig } from "./db";
import { processStreamZipToPostgres } from "./workers/streamUnzipWorker";

type StreamingRouteOptions = {
  fileLimit: number;
  uploadsDir: string;
};

export function registerStreamingRoute(app: express.Express, options: StreamingRouteOptions): void {
  const storage = multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, options.uploadsDir),
    filename: (_req, file, cb) => {
      const safeName = file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_");
      cb(null, `${Date.now()}_${safeName}`);
    }
  });

  const upload = multer({ storage, limits: { fileSize: options.fileLimit } });

  app.post("/webhook/streaming", upload.single("file"), async (req: Request, res: Response) => {
    try {
      if (!req.file) {
        res.status(400).json({ error: "No file uploaded. Use form-data with field name 'file'." });
        return;
      }

      if (!req.file.originalname.toLowerCase().endsWith(".zip")) {
        res.status(400).json({ error: "Uploaded file must be a .zip archive." });
        return;
      }

      if (!hasDatabaseConfig()) {
        res.status(500).json({
          error: "Database configuration is required for /webhook/streaming ingestion."
        });
        return;
      }

      const archivePath = req.file.path;

      try {
        await processStreamZipToPostgres(
          fs.createReadStream(archivePath),
          req.file.originalname
        );
      } finally {
        void fs.promises.rm(archivePath, { force: true }).catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : "Unknown cleanup error";
          console.warn(`Failed to remove streamed archive ${archivePath}: ${msg}`);
        });
      }

      res.status(200).json({
        message: "ZIP received, streamed, and persisted."
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      console.error("Failed to process streaming zip:", errorMessage);

      res.status(500).json({
        error: "Failed to process ZIP file.",
        details: errorMessage
      });
    }
  });
}
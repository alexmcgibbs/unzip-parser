import express, { Request, Response } from "express";
import Busboy from "busboy";
import { hasDatabaseConfig } from "./db";
import { processFullStreamingZipToPostgres } from "./workers/fullStreamWorker";

type FullStreamingRouteOptions = {
  fileLimit: number;
};

export function registerFullStreamingRoute(
  app: express.Express,
  options: FullStreamingRouteOptions
): void {
  app.post("/webhook/streaming/full", (req: Request, res: Response) => {
    if (!hasDatabaseConfig()) {
      res.status(500).json({
        error: "Database configuration is required for /webhook/streaming/full ingestion."
      });
      return;
    }

    let hasFileField = false;
    let invalidFileType = false;
    let fileTooLarge = false;
    let responseSent = false;
    let workerPromise: Promise<void> | null = null;

    const sendResponse = (statusCode: number, body: Record<string, unknown>) => {
      if (!responseSent) {
        responseSent = true;
        res.status(statusCode).json(body);
      }
    };

    let busboy: Busboy.Busboy;
    try {
      busboy = Busboy({
        headers: req.headers,
        limits: {
          files: 1,
          fileSize: options.fileLimit
        }
      });
    } catch {
      sendResponse(400, { error: "Invalid multipart/form-data request." });
      return;
    }

    busboy.on("file", (fieldName, file, info) => {
      if (fieldName !== "file") {
        file.resume();
        return;
      }

      if (hasFileField) {
        file.resume();
        return;
      }

      hasFileField = true;
      const originalFileName = info.filename || "upload.zip";

      if (!originalFileName.toLowerCase().endsWith(".zip")) {
        invalidFileType = true;
        file.resume();
        return;
      }

      file.on("limit", () => {
        fileTooLarge = true;
      });

      workerPromise = processFullStreamingZipToPostgres(file, originalFileName);
    });

    busboy.on("error", (error) => {
      const errorMessage = error instanceof Error ? error.message : "Unknown multipart error";
      sendResponse(500, {
        error: "Failed to process multipart stream.",
        details: errorMessage
      });
    });

    busboy.on("finish", async () => {
      if (responseSent) {
        return;
      }

      if (!hasFileField) {
        sendResponse(400, {
          error: "No file uploaded. Use form-data with field name 'file'."
        });
        return;
      }

      if (invalidFileType) {
        sendResponse(400, {
          error: "Uploaded file must be a .zip archive."
        });
        return;
      }

      if (fileTooLarge) {
        sendResponse(413, {
          error: "Uploaded file exceeds FILE_LIMIT."
        });
        return;
      }

      if (!workerPromise) {
        sendResponse(500, {
          error: "Streaming worker did not initialize."
        });
        return;
      }

      try {
        await workerPromise;
        sendResponse(200, {
          message: "ZIP received, streamed, and persisted."
        });
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : "Unknown stream worker error";
        sendResponse(500, {
          error: "Failed to process ZIP stream.",
          details: errorMessage
        });
      }
    });

    req.pipe(busboy);
  });
}

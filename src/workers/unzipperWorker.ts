import fs from "node:fs";
import { fileURLToPath } from "node:url";
import got from "got";
import { parentPort, workerData } from "node:worker_threads";
import * as unzipper from "unzipper";
import { hasDatabaseConfig } from "../db";
import { persistWebhookPayload } from "../models/filePayload";
import { ZipEntryResult } from "../types";

type WorkerPayload = {
  fileUrl: string;
};

async function readEntryToBuffer(entry: unzipper.Entry): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

function createZipSourceStream(fileUrl: string): NodeJS.ReadableStream {
  const url = new URL(fileUrl);

  if (url.protocol === "file:") {
    const localPath = fileURLToPath(url);
    if (!fs.existsSync(localPath)) {
      throw new Error("ZIP file not found.");
    }
    return fs.createReadStream(localPath);
  }

  if (url.protocol === "http:" || url.protocol === "https:") {
    return got.stream(fileUrl);
  }

  throw new Error(`Unsupported file URL protocol: ${url.protocol}`);
}

(async function run() {
  try {
    const { fileUrl } = workerData as WorkerPayload;

    if (!fileUrl) {
      throw new Error("ZIP file URL is required.");
    }

    const zipStream = createZipSourceStream(fileUrl).pipe(unzipper.Parse({ forceStream: true }));
    const entries: ZipEntryResult[] = [];
    const extractedFiles: string[] = [];
    const jsonFilePaths: string[] = [];
    const persistPromises: Promise<unknown>[] = [];

    for await (const entry of zipStream) {
      const zipEntry: ZipEntryResult = {
        name: entry.path,
        size: Number(entry.vars.uncompressedSize ?? 0),
        compressedSize: Number(entry.vars.compressedSize ?? 0),
        isDirectory: entry.type === "Directory"
      };

      entries.push(zipEntry);

      if (!zipEntry.isDirectory) {
        extractedFiles.push(zipEntry.name);
      }

      if (!zipEntry.isDirectory && zipEntry.name.toLowerCase().endsWith(".json")) {
        const jsonBuffer = await readEntryToBuffer(entry);
        const jsonContents = JSON.parse(jsonBuffer.toString("utf8"));
        jsonFilePaths.push(zipEntry.name);

        if (hasDatabaseConfig()) {
          persistPromises.push(persistWebhookPayload(zipEntry.name, jsonContents));
        }

        continue;
      }

      entry.autodrain();
    }

    if (jsonFilePaths.length === 0) {
      throw new Error("No JSON file found in ZIP contents.");
    }

    console.log("Unzipper worker unzip summary:");
    console.log(
      JSON.stringify(
        {
          archivePath: fileUrl,
          totalEntries: entries.length,
          extractedFiles,
          jsonFilePaths
        },
        null,
        2
      )
    );

    if (persistPromises.length > 0) {
      await Promise.all(persistPromises);
    } else if (!hasDatabaseConfig()) {
      console.log("Postgres connection not configured. Skipping database write.");
    }

    parentPort?.postMessage({ ok: true, resolvedName: jsonFilePaths[0] });
    process.exit(0);
    return;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";
    parentPort?.postMessage({ error: message });
    process.exit(1);
  }
})();
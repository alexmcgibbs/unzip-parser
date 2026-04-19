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
  jobId: string;
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

const MAX_UNZIPPED_DEFAULT_BYTES = 100 * 1024 * 1024; // 100 MB

function parseMaxUnzippedBytes(): number {
  const raw = process.env.MAX_UNZIPPED;
  if (!raw) return MAX_UNZIPPED_DEFAULT_BYTES;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`MAX_UNZIPPED env var is invalid: "${raw}". Must be a positive number of bytes.`);
  }
  return parsed;
}

(async function run() {
  try {
    const { fileUrl, jobId } = workerData as WorkerPayload;

    if (!fileUrl) {
      throw new Error("ZIP file URL is required.");
    }

    if (!jobId) {
      throw new Error("jobId is required.");
    }

    const maxUnzippedBytes = parseMaxUnzippedBytes();

    const zipStream = createZipSourceStream(fileUrl).pipe(unzipper.Parse({ forceStream: true }));
    const entries: ZipEntryResult[] = [];
    const extractedFiles: string[] = [];
    const jsonFilePaths: string[] = [];

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
        const uncompressedSize = Number(entry.vars.uncompressedSize ?? 0);
        if (uncompressedSize > maxUnzippedBytes) {
          throw new Error(
            `Entry "${zipEntry.name}" uncompressed size ${uncompressedSize} bytes exceeds MAX_UNZIPPED limit of ${maxUnzippedBytes} bytes.`
          );
        }

        const jsonBuffer = await readEntryToBuffer(entry);
        const jsonContents = JSON.parse(jsonBuffer.toString("utf8"));
        jsonFilePaths.push(zipEntry.name);

        if (hasDatabaseConfig()) {
          await persistWebhookPayload(zipEntry.name, jsonContents, jobId);
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

    if (!hasDatabaseConfig()) {
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
import fs from "node:fs";
import { parentPort, workerData } from "node:worker_threads";
import * as unzipper from "unzipper";
import { hasDatabaseConfig } from "../db";
import { persistWebhookPayload } from "../models/filePayload";
import { ZipEntryResult } from "../types";

type WorkerPayload = {
  zipPath: string;
  originalFileName: string;
};

async function readEntryToBuffer(entry: unzipper.Entry): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

(async function run() {
  try {
    const { zipPath, originalFileName } = workerData as WorkerPayload;

    if (!zipPath || !fs.existsSync(zipPath)) {
      throw new Error("ZIP file not found.");
    }

    const zipStream = fs.createReadStream(zipPath).pipe(unzipper.Parse({ forceStream: true }));
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
          persistPromises.push(persistWebhookPayload(originalFileName, jsonContents));
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
          archivePath: zipPath,
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

    parentPort?.postMessage({ ok: true });
    fs.rm(zipPath, { force: true }, (error) => {
      if (error) {
        const message = error instanceof Error ? error.message : "Unknown cleanup error";
        console.warn(`Failed to remove uploaded archive ${zipPath}: ${message}`);
      }
      process.exit(0);
    });
    return;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";
    parentPort?.postMessage({ error: message });
    process.exit(1);
  }
})();
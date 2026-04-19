import fs from "node:fs";
import { parentPort, workerData } from "node:worker_threads";
import AdmZip from "adm-zip";
import { hasDatabaseConfig } from "../db";
import { persistWebhookPayload } from "../models/filePayload";

type ZipEntryResult = {
  name: string;
  size: number;
  compressedSize: number;
  isDirectory: boolean;
};

type WorkerPayload = {
  zipPath: string;
  originalFileName: string;
};

function findFirstJsonEntry(entries: ZipEntryResult[]): ZipEntryResult {
  const jsonEntry = entries.find((entry) => !entry.isDirectory && entry.name.toLowerCase().endsWith(".json"));

  if (!jsonEntry) {
    throw new Error("No JSON file found in ZIP contents.");
  }

  return jsonEntry;
}

(async function run() {
  try {
    const { zipPath, originalFileName } = workerData as WorkerPayload;

    if (!zipPath || !fs.existsSync(zipPath)) {
      throw new Error("ZIP file not found.");
    }

    const zip = new AdmZip(zipPath);
    const entries = zip.getEntries().map((entry): ZipEntryResult => ({
      name: entry.entryName,
      size: entry.header.size,
      compressedSize: entry.header.compressedSize,
      isDirectory: entry.isDirectory
    }));

    const extractedFiles = entries.filter((entry) => !entry.isDirectory).map((entry) => entry.name);
    const jsonEntry = findFirstJsonEntry(entries);
    const rawJson = zip.readAsText(jsonEntry.name, "utf8");
    const jsonContents = JSON.parse(rawJson);

    console.log("Worker unzip summary:");
    console.log(
      JSON.stringify(
        {
          archivePath: zipPath,
          totalEntries: entries.length,
          extractedFiles,
          jsonFilePath: jsonEntry.name
        },
        null,
        2
      )
    );

    if (hasDatabaseConfig()) {
      await persistWebhookPayload(originalFileName, jsonContents);
    } else {
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

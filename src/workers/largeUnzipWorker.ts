import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { parentPort, workerData } from "node:worker_threads";
import { ZipEntryResult } from "../types";
import { hasDatabaseConfig } from "../db";
import { persistWebhookPayload } from "../models/filePayload";

type WorkerPayload = {
  zipPath: string;
  originalFileName: string;
};

function runUnzipCommand(args: string[], zipPath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn("unzip", [...args, zipPath], {
      stdio: ["ignore", "pipe", "pipe"]
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => {
      stdoutChunks.push(chunk);
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
    });

    child.once("error", (error) => {
      reject(error);
    });

    child.once("close", (code) => {
      const stdout = Buffer.concat(stdoutChunks).toString("utf8");
      const stderr = Buffer.concat(stderrChunks).toString("utf8").trim();

      if (code !== 0) {
        reject(new Error(stderr || `unzip exited with code ${code}`));
        return;
      }

      resolve(stdout);
    });
  });
}

function parseListOutput(output: string): string[] {
  return output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !line.endsWith("/"));
}

function buildEntries(files: string[]): ZipEntryResult[] {
  return files.map((file): ZipEntryResult => ({
    name: file,
    size: 0,
    compressedSize: 0,
    isDirectory: false
  }));
}

function findFirstJsonFile(files: string[]): string {
  const jsonFile = files.find((file) => file.toLowerCase().endsWith(".json"));

  if (!jsonFile) {
    throw new Error("No JSON file found in extracted ZIP contents.");
  }

  return jsonFile;
}

async function readFileFromArchive(zipPath: string, archiveFilePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn("unzip", ["-p", zipPath, archiveFilePath], {
      stdio: ["ignore", "pipe", "pipe"]
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => {
      stdoutChunks.push(chunk);
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
    });

    child.once("error", (error) => {
      reject(error);
    });

    child.once("close", (code) => {
      const stderr = Buffer.concat(stderrChunks).toString("utf8").trim();

      if (code !== 0) {
        reject(new Error(stderr || `unzip exited with code ${code}`));
        return;
      }

      resolve(Buffer.concat(stdoutChunks).toString("utf8"));
    });
  });
}

(async function run() {
  try {
    const { zipPath, originalFileName } = workerData as WorkerPayload;

    if (!zipPath || !fs.existsSync(zipPath)) {
      throw new Error("ZIP file not found.");
    }

    const listedFiles = parseListOutput(await runUnzipCommand(["-Z1"], zipPath));
    const jsonFilePath = findFirstJsonFile(listedFiles);
    const jsonText = await readFileFromArchive(zipPath, jsonFilePath);
    const jsonContents = JSON.parse(jsonText);

    console.log("Large worker unzip summary:");
    console.log(
      JSON.stringify(
        {
          archivePath: zipPath,
          extractedTo: path.join(path.dirname(zipPath), "[memory]"),
          totalEntries: listedFiles.length,
          entries: buildEntries(listedFiles),
          extractedFiles: listedFiles,
          jsonFilePath
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
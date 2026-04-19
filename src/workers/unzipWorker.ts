import fs from "node:fs";
import path from "node:path";
import { parentPort, workerData } from "node:worker_threads";
import AdmZip from "adm-zip";

type ZipEntryResult = {
  name: string;
  size: number;
  compressedSize: number;
  isDirectory: boolean;
};

type WorkerPayload = {
  zipPath: string;
};

function findFirstJsonFile(rootDir: string): string {
  const files = collectFiles(rootDir);
  const jsonFile = files.find((file) => file.toLowerCase().endsWith(".json"));

  if (!jsonFile) {
    throw new Error("No JSON file found in extracted ZIP contents.");
  }

  return path.join(rootDir, jsonFile);
}

function collectFiles(rootDir: string): string[] {
  const output: string[] = [];

  function walk(current: string): void {
    const entries = fs.readdirSync(current, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        walk(fullPath);
      } else {
        output.push(path.relative(rootDir, fullPath));
      }
    }
  }

  if (fs.existsSync(rootDir)) {
    walk(rootDir);
  }

  return output;
}

(function run() {
  try {
    const { zipPath } = workerData as WorkerPayload;

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

    const extractedRoot = path.resolve(process.cwd(), "uploads", "extracted");
    if (!fs.existsSync(extractedRoot)) {
      fs.mkdirSync(extractedRoot, { recursive: true });
    }

    const folderName = `${path.basename(zipPath, ".zip")}_${Date.now()}`;
    const extractedTo = path.join(extractedRoot, folderName);
    fs.mkdirSync(extractedTo, { recursive: true });

    zip.extractAllTo(extractedTo, true);

    const extractedFiles = collectFiles(extractedTo);
    const extractedJsonPath = findFirstJsonFile(extractedTo);
    const jsonContents = JSON.parse(fs.readFileSync(extractedJsonPath, "utf8"));

    const result = {
      archivePath: zipPath,
      extractedTo,
      totalEntries: entries.length,
      entries,
      extractedFiles,
      jsonFilePath: extractedJsonPath,
      jsonContents
    };

    console.log("Worker unzip result:");
    console.log(JSON.stringify(result, null, 2));

    parentPort?.postMessage(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";
    parentPort?.postMessage({ error: message });
  }
})();

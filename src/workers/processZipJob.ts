import fs from "node:fs";
import { fileURLToPath } from "node:url";
import got from "got";
import * as unzipper from "unzipper";
import { hasDatabaseConfig } from "../db";
import { persistWebhookPayload } from "../models/filePayload";
import { ZipEntryResult } from "../types";

export type ZipWorkerPayload = {
  fileUrl: string;
  jobId: string;
};

export type ZipWorkerResult = {
  ok: true;
  resolvedName: string;
};

type GotErrorLike = {
  name?: string;
  code?: string;
  message?: string;
  response?: {
    statusCode?: number;
    statusMessage?: string;
  };
};

async function readEntryToBuffer(entry: unzipper.Entry): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

async function createZipSourceStream(fileUrl: string): Promise<NodeJS.ReadableStream> {
  let url: URL;
  try {
    url = new URL(fileUrl);
  } catch {
    throw new Error(`Invalid fileUrl. Expected absolute URL, received: ${fileUrl}`);
  }

  if (url.protocol === "file:") {
    const localPath = fileURLToPath(url);
    if (!fs.existsSync(localPath)) {
      throw new Error("ZIP file not found.");
    }
    return fs.createReadStream(localPath);
  }

  if (url.protocol === "http:" || url.protocol === "https:") {
    return new Promise<NodeJS.ReadableStream>((resolve, reject) => {
      const stream = got.stream(fileUrl, { throwHttpErrors: false });
      stream.once("response", (response) => {
        const { statusCode, statusMessage } = response;
        if (statusCode < 200 || statusCode >= 300) {
          stream.destroy();
          reject(new Error(`Failed to download ZIP. HTTP ${statusCode} ${statusMessage ?? ""}. URL: ${fileUrl}`));
        } else {
          resolve(stream);
        }
      });
      stream.once("error", (err) => {
        reject(new Error(`Failed to download ZIP from URL: ${fileUrl}. ${err.message}`));
      });
    });
  }

  throw new Error(`Unsupported file URL protocol: ${url.protocol}`);
}

function normalizeZipSourceError(error: unknown, fileUrl: string): Error {
  const err = error as GotErrorLike;
  const message = err?.message ?? "Unknown error";

  if (err?.name === "HTTPError") {
    const statusCode = err.response?.statusCode ?? "unknown";
    const statusMessage = err.response?.statusMessage ?? "HTTP error";
    return new Error(`Failed to download ZIP. HTTP ${statusCode} ${statusMessage}. URL: ${fileUrl}`);
  }

  if (err?.name === "RequestError") {
    const code = err.code ? ` (${err.code})` : "";
    return new Error(`Failed to download ZIP from URL${code}: ${fileUrl}. ${message}`);
  }

  if (message.toLowerCase().includes("invalid") && message.toLowerCase().includes("zip")) {
    return new Error(`Downloaded content is not a valid ZIP archive. URL: ${fileUrl}`);
  }

  return error instanceof Error ? error : new Error(String(error));
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

export async function processZipJob({ fileUrl, jobId }: ZipWorkerPayload): Promise<ZipWorkerResult> {
  if (!fileUrl) {
    throw new Error("ZIP file URL is required.");
  }

  if (!jobId) {
    throw new Error("jobId is required.");
  }

  const maxUnzippedBytes = parseMaxUnzippedBytes();

  try {
    const sourceStream = await createZipSourceStream(fileUrl);
    const zipStream = sourceStream.pipe(unzipper.Parse({ forceStream: true }));
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

    return { ok: true, resolvedName: jsonFilePaths[0] };
  } catch (error) {
    throw normalizeZipSourceError(error, fileUrl);
  }
}
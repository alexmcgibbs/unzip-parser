import { processStreamZipToPostgres } from "./streamUnzipWorker";

export async function processFullStreamingZipToPostgres(
  fileStream: NodeJS.ReadableStream,
  originalFileName: string
): Promise<void> {
  await processStreamZipToPostgres(fileStream, originalFileName);
}

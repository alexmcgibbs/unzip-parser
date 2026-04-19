import { parentPort, workerData } from "node:worker_threads";
import { processZipJob, ZipWorkerPayload } from "./processZipJob";

(async function run() {
  try {
    const result = await processZipJob(workerData as ZipWorkerPayload);
    parentPort?.postMessage(result);
    process.exit(0);
    return;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";
    parentPort?.postMessage({ error: message });
    process.exit(1);
  }
})();
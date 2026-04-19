import { ThreadWorker } from "poolifier";
import { processZipJob, ZipWorkerPayload, ZipWorkerResult } from "./processZipJob";

new ThreadWorker<ZipWorkerPayload, ZipWorkerResult>(async (data) => {
  if (!data) throw new Error("Data is required");
  return processZipJob(data);
});
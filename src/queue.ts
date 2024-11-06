import type { Job } from "bullmq";
import { Queue, Worker } from "bullmq";
import logger from "./logger";
import { processParquetFile } from "./server";

interface ParquetJob {
  key: string;
}

const connection = {
  host: process.env.REDIS_HOST || "localhost",
  port: Number.parseInt(process.env.REDIS_PORT || "6379"),
};

export const jobQueue = new Queue<ParquetJob>("parquet-processing", {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
    removeOnComplete: 1000,
    removeOnFail: false,
  },
});

const worker = new Worker<ParquetJob>(
  "parquet-processing",
  async (job: Job<ParquetJob>) => {
    const { key } = job.data;
    logger.info(`Processing file: ${key}`);
    await processParquetFile(key);
  },
  {
    connection,
    concurrency: 3,
  }
);

worker.on("completed", (job: Job<ParquetJob>) => {
  logger.info(`Completed processing: ${job.data.key}`);
});

worker.on("failed", (job: Job<ParquetJob> | undefined, err: Error) => {
  logger.error(`Failed processing ${job?.data.key}:`, err);
});

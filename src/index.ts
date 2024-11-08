import dotenv from "dotenv";
dotenv.config();

import { createWriteStream, unlink } from "node:fs";

import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { GetObjectCommand, HeadObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";

import {
  checkIfTableExists,
  closeDBConnections,
  createClickHouseTables,
  getLatestFullImport,
  getProcessedIncrementalFiles,
  getTableNames,
  ingestParquetToClickHouse,
  updateImportTracking,
} from "./db";
import { jobQueue } from "./queue";
import { createServer } from "./server";
import { cleanupTempSubfolder, createTempSubfolder, formatFileSize } from "./utils/files";
import logger from "./utils/logger";
import { clearRedisDB } from "./utils/redis";

const { AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION } = process.env;

const S3_BUCKET = "tf-premium-parquet";

const s3 = new S3Client({
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID || "",
    secretAccessKey: AWS_SECRET_ACCESS_KEY || "",
  },
  region: AWS_REGION,
});

enum FileType {
  FULL = "full",
  INCREMENTAL = "incremental",
}

export async function processParquetFile(key: string) {
  if (key.endsWith(".empty")) return;

  logger.info(`Starting to process file: ${key}`);

  try {
    const tableNameMatch = key.match(/farcaster-(\w+)-\d+-\d+\.parquet$/);
    if (!tableNameMatch) {
      throw new Error(`Unable to extract table name from key: ${key}`);
    }
    const tableName = tableNameMatch[1];

    const tableExists = await checkIfTableExists(tableName);
    if (!tableExists) {
      logger.info(`Skipping processing: Table '${tableName}' does not exist`);
      return;
    }

    const tempSubfolder = createTempSubfolder();
    const localFilePath = path.join(tempSubfolder, `${Date.now()}.parquet`);

    const data = await s3.send(new HeadObjectCommand({ Bucket: S3_BUCKET, Key: key }));
    if (!data.ContentLength) throw new Error("Unable to get file size");

    const fileSize = formatFileSize(data.ContentLength);
    logger.info(`File size: ${fileSize}`);

    const { Body } = await s3.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: key }));
    if (!Body || !(Body instanceof Readable)) throw new Error("Invalid S3 response body");

    const writeStream = createWriteStream(localFilePath);

    logger.info(`Local file path: ${localFilePath}`);
    logger.info(`Starting download for ${key}`);

    let downloadedBytes = 0;
    const progressStream = new PassThrough();
    progressStream.on("data", (chunk: Buffer) => {
      downloadedBytes += chunk.length;
    });

    await pipeline(Body, progressStream, writeStream);

    const fileType = key.includes("/full/") ? FileType.FULL : FileType.INCREMENTAL;

    if (fileType === FileType.FULL) {
      const { processed_timestamp: existingFullImport } = await getLatestFullImport(tableName);
      if (existingFullImport) {
        logger.info(`Existing full import found for ${tableName}. Skipping this import.`);
        unlink(localFilePath, (err) => {
          if (err) {
            logger.error("Error deleting local file:", err);
          }
        });
        return;
      }
    }

    const ingestionResult = await ingestParquetToClickHouse(localFilePath, tableName);
    if (!ingestionResult) {
      throw new Error(`Failed to ingest file ${key} - no result returned`);
    }

    const isEmptyFile = ingestionResult.summary?.written_rows === "0";
    await updateImportTracking(tableName, key, fileType, isEmptyFile);

    logger.info(`Successfully processed and ingested file: ${key}`);
    logger.info(
      `Rows inserted: ${ingestionResult.summary?.written_rows} written, ${ingestionResult.summary?.written_bytes} bytes`
    );

    unlink(localFilePath, (err) => {
      if (err) {
        logger.error("Error deleting local file:", err);
      }
    });
  } catch (error) {
    logger.error({
      msg: `Failed processing ${key}`,
      err: error,
      key,
    });
    throw error; // re-throw to ensure the job fails properly
  }
}

async function processLatestFullExports() {
  logger.info("Starting full exports processing");

  const fullExportPrefix = "public-postgres/farcaster/v2/full";

  const allContents = [];
  let continuationToken: string | undefined;

  do {
    const { Contents, NextContinuationToken } = await s3.send(
      new ListObjectsV2Command({
        Bucket: S3_BUCKET,
        Prefix: fullExportPrefix,
        ContinuationToken: continuationToken,
      })
    );

    if (Contents) {
      allContents.push(...Contents);
    }

    continuationToken = NextContinuationToken;
  } while (continuationToken);

  if (!allContents.length) {
    logger.error("No full export files found");
    return;
  }

  const latestExports = new Map<string, string>();

  for (const object of allContents) {
    if (!object.Key) continue;
    const match = object.Key.match(/farcaster-(\w+)-\d+-\d+\.parquet$/);
    if (match) {
      const tableName = match[1];
      const latestExportKey = latestExports.get(tableName);

      if (!latestExportKey || object.Key > latestExportKey) {
        latestExports.set(tableName, object.Key);
      }
    }
  }

  for (const [tableName, key] of latestExports) {
    logger.info(`Checking latest full export for table: ${tableName}`);
    try {
      const { processed_timestamp: existingFullImport } = await getLatestFullImport(tableName);

      if (existingFullImport) {
        logger.info(`Existing full import found for ${tableName}. Skipping this import.`);
        continue;
      }

      logger.info(`Queueing latest full export for table: ${tableName}`);

      await jobQueue.add("process-file", { key }, { jobId: key });
      logger.info({ msg: "Queued file", key });
    } catch (error) {
      logger.error(`Error queueing full export for table ${tableName}:`, error);
    }
  }
}

async function processMissingIncrementals() {
  logger.info("Starting incrementals processing");

  const incrementalPrefix = "public-postgres/farcaster/v2/incremental";

  const tableNames = await getTableNames();

  for (const tableName of tableNames) {
    try {
      const { processed_timestamp: lastFullTimestamp } = await getLatestFullImport(tableName);

      if (!lastFullTimestamp) {
        logger.info(`No full import found for ${tableName}, skipping incrementals`);
        continue;
      }

      const allContents = [];
      let continuationToken: string | undefined;

      do {
        const { Contents, NextContinuationToken } = await s3.send(
          new ListObjectsV2Command({
            Bucket: S3_BUCKET,
            Prefix: `${incrementalPrefix}/farcaster-${tableName}`,
            ContinuationToken: continuationToken,
          })
        );

        if (Contents) {
          allContents.push(...Contents);
        }

        continuationToken = NextContinuationToken;
      } while (continuationToken);

      if (!allContents.length) {
        logger.info(`No incremental files found for ${tableName}`);
        continue;
      }

      const processedFiles = await getProcessedIncrementalFiles(tableName);

      function isValidUnprocessedFile(object: { Key?: string }): object is { Key: string } {
        return !!object.Key && !processedFiles.has(object.Key);
      }

      function isValidTimestampedFile(file: {
        key: string;
        timestamps: { startTime: number; endTime: number } | null;
      }): file is { key: string; timestamps: { startTime: number; endTime: number } } {
        if (!lastFullTimestamp) return false;

        return file.timestamps !== null && file.timestamps.startTime > lastFullTimestamp;
      }

      function parseTimestampsFromKey(key: string): { startTime: number; endTime: number } | null {
        const match = key.match(/-(\d+)-(\d+)\.parquet$/);
        if (!match) return null;

        return {
          startTime: Number.parseInt(match[1]),
          endTime: Number.parseInt(match[2]),
        };
      }

      const pendingFiles = allContents
        // Step 1: Filter out processed and invalid files
        .filter(isValidUnprocessedFile)
        // Step 2: Extract timestamp information
        .map((object) => ({
          key: object.Key,
          timestamps: parseTimestampsFromKey(object.Key),
        }))
        // Step 3: Filter files after last full import
        .filter(isValidTimestampedFile)
        // Step 4: Sort chronologically
        .sort((a, b) => a.timestamps.startTime - b.timestamps.startTime);

      logger.info(`Found ${pendingFiles.length} new incremental files for ${tableName}`);

      for (const file of pendingFiles) {
        try {
          await jobQueue.add("process-file", { key: file.key }, { jobId: file.key });
          logger.info(`Queued incremental file: ${file.key}`);
        } catch (error) {
          logger.error(`Error queueing incremental file ${file.key}:`, error);
        }
      }
    } catch (error) {
      logger.error(`Error processing incrementals for table ${tableName}:`, error);
    }
  }
}

const RESET_DB = process.argv.includes("--reset");

export async function start() {
  if (RESET_DB) {
    await clearRedisDB();
  }

  try {
    await createClickHouseTables();
    await createServer();
    await processLatestFullExports();
    await processMissingIncrementals();
  } catch (error) {
    logger.error("Initialization error:", error);
    process.exit(1);
  }
}

start();

// graceful shutdown
process.on("SIGINT", async () => {
  await cleanupTempSubfolder();
  await closeDBConnections();
  await jobQueue.close();
  process.exit(0);
});

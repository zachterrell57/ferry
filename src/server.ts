import dotenv from "dotenv";
dotenv.config();

import express from "express";
import { S3Client, GetObjectCommand, ListObjectsV2Command, HeadObjectCommand } from "@aws-sdk/client-s3";
import { SNSClient, SubscribeCommand } from "@aws-sdk/client-sns";
import {
  ingestParquetToClickHouse,
  updateImportTracking,
  checkIfTableExists,
  getProcessedIncrementalFiles,
  getLatestFullImport,
  getTableNames,
  createClickHouseTables,
} from "./db";
import { createWriteStream, unlink, mkdirSync, appendFileSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import https from "node:https";
import logger from "./logger";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import { jobQueue } from "./queue";
import { formatFileSize } from "./utils";

// Load environment variables
const {
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  AWS_REGION,
  PORT = "3001",
  HOSTNAME = "0.0.0.0",
  SNS_TOPIC_ARN,
  NGROK_URL,
} = process.env;

const S3_BUCKET = "tf-premium-parquet";

const s3 = new S3Client({
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID || "",
    secretAccessKey: AWS_SECRET_ACCESS_KEY || "",
  },
  region: AWS_REGION,
});

const sns = new SNSClient({
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

function createTempSubfolder(): string {
  const tempSubfolder = path.join(os.tmpdir(), "parquet-importer");
  mkdirSync(tempSubfolder, { recursive: true });
  return tempSubfolder;
}

async function processParquetFile(key: string) {
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
      context: "processParquetFile",
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

async function setupSNSSubscription() {
  if (!SNS_TOPIC_ARN) {
    throw new Error("SNS_TOPIC_ARN is not defined");
  }

  const endpoint = NGROK_URL ? `${NGROK_URL}/subscribe` : `http://${HOSTNAME}:${PORT}/subscribe`;

  logger.info("Setting up SNS subscription with endpoint:", endpoint);

  const subscribeParams = {
    Protocol: "https", // Always use HTTPS for ngrok
    TopicArn: SNS_TOPIC_ARN,
    Endpoint: endpoint,
    ReturnSubscriptionArn: true,
  };

  try {
    const subscriptionData = await sns.send(new SubscribeCommand(subscribeParams));
    logger.info("SNS Subscription request sent:", {
      SubscriptionArn: subscriptionData.SubscriptionArn,
      Endpoint: subscribeParams.Endpoint,
    });
  } catch (error) {
    logger.error("Error creating SNS subscription:", error);
    // Add more detailed error logging
    if (error instanceof Error) {
      logger.error("Error details:", error.message);
    }
  }
}

async function handleSNSMessage(message: { Message: string }) {
  const s3Event = JSON.parse(message.Message);
  const { object } = s3Event.Records[0].s3;

  await jobQueue.add(
    "process-file",
    {
      key: object.key,
    },
    {
      jobId: object.key, // prevent duplicates
    }
  );
}

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
  queues: [new BullMQAdapter(jobQueue)],
  serverAdapter,
});

const app = express();
app.use("/admin/queues", serverAdapter.getRouter());
app.use(express.json());
app.use(express.text());

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

app.post("/subscribe", async (req, res) => {
  try {
    let message: {
      Type: string;
      SubscribeURL?: string;
      Message?: string;
    };

    if (typeof req.body === "string") {
      message = JSON.parse(req.body);
    } else {
      message = req.body;
    }

    // Handle raw message format from SNS
    if (message.Type === "SubscriptionConfirmation") {
      if (!message.SubscribeURL) {
        logger.error("Missing SubscribeURL in confirmation message");
        return res.status(400).send("Missing SubscribeURL");
      }

      try {
        await new Promise<void>((resolve, reject) => {
          if (!message.SubscribeURL) {
            logger.error("Missing SubscribeURL in confirmation message");
            return res.status(400).send("Missing SubscribeURL");
          }

          https
            .get(message.SubscribeURL, (res) => {
              let data = "";
              res.on("data", (chunk) => {
                data = data + chunk;
              });
              res.on("end", () => resolve());
            })
            .on("error", reject);
        });
        logger.info("SNS Subscription confirmed successfully");
        return res.status(200).send("SNS Subscription confirmed");
      } catch (error) {
        logger.error("Failed to confirm subscription:", error);
        return res.status(500).send("Failed to confirm subscription");
      }
    } else if (message.Type === "Notification") {
      if (!message.Message) {
        logger.error("Missing Message in notification");
        return res.status(400).send("Missing Message in notification");
      }
      await handleSNSMessage({ Message: message.Message });
      return res.status(200).send("SNS Notification processed");
    } else {
      logger.error("Invalid message type:", message.Type);
      return res.status(400).send("Invalid SNS message type");
    }
  } catch (error) {
    logger.error("Error processing SNS message:", error);
    return res.status(500).send("Internal server error processing SNS message");
  }
});

// Initialize ClickHouse tables, process initial tables, start listening for SNS messages
async function startServer() {
  app.listen(Number(PORT), HOSTNAME, () => {
    logger.info(`Server is listening on port ${PORT}`);
  });

  try {
    await createClickHouseTables();

    await setupSNSSubscription();

    // await processLatestFullExports();

    await processMissingIncrementals();
  } catch (error) {
    logger.error("Initialization error:", error);
    process.exit(1);
  }
}

startServer();

// Graceful shutdown
process.on("SIGINT", () => {
  //   console.log("Shutting down server...");
  //   const tempSubfolder = path.join(os.tmpdir(), "parquet-importer");

  //   if (existsSync(tempSubfolder)) {
  //     rmdirSync(tempSubfolder, { recursive: true });
  //     console.log("Temporary subfolder wiped.");
  //   }

  process.exit(0);
});

// Export processParquetFile for use in queue worker
export { processParquetFile };

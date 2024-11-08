import { createReadStream } from "node:fs";
import { type InsertResult, createClient } from "@clickhouse/client";
import { tableSchemas } from "./schemas";
import logger from "./utils/logger";

// Configure ClickHouse client
const clickhouse = createClient({
  url: process.env.CLICKHOUSE_URL,
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DATABASE,
  request_timeout: 5_400_000, // 1.5 hours
  clickhouse_settings: {
    max_memory_usage: "0",
    max_execution_time: 5400, // 1.5 hours
    send_progress_in_http_headers: 1,
    http_connection_timeout: 5_400_000, // 1.5 hours
    http_headers_progress_interval_ms: "110000",
    max_insert_block_size: "100000",
  },
});

// Create tables in ClickHouse
export async function createClickHouseTables() {
  const queryPromises = Object.values(tableSchemas).map((tableQuery) =>
    clickhouse.query({ query: tableQuery }).catch((error) => {
      logger.error(`Error creating table: ${error.message}`);
      logger.error(`Query: ${tableQuery.slice(0, 100)}...`);
    })
  );

  await Promise.all(queryPromises);
}

export async function updateImportTracking(tableName: string, key: string, fileType: string, isEmptyFile: boolean) {
  const timestamps = key.match(/-(\d+)-(\d+)\.parquet$/);
  const processedTimestamp = timestamps ? Number.parseInt(timestamps[2]) : 0;

  await clickhouse.insert({
    table: "parquet_import_tracking",
    values: [
      {
        id: Date.now(),
        table_name: tableName,
        file_name: key,
        file_type: fileType,
        is_empty: isEmptyFile,
        processed_timestamp: processedTimestamp,
      },
    ],
    format: "JSONEachRow",
  });
}

export async function ingestParquetToClickHouse(
  localFilePath: string,
  tableName: string
): Promise<InsertResult | null> {
  const startTime = Date.now();
  logger.info(`Starting ingestion for table: ${tableName}`);

  try {
    const result = await clickhouse.insert({
      table: tableName,
      values: createReadStream(localFilePath),
      format: "Parquet",
      clickhouse_settings: {
        async_insert: 1,
        wait_for_async_insert: 1,
        async_insert_max_data_size: "100000000", // 100MB buffer size
        async_insert_busy_timeout_ms: 1000, // flush buffer every second
        min_insert_block_size_rows: "50000",
        max_insert_block_size: "100000",
        min_insert_block_size_bytes: "50000000", // 50MB
      },
    });

    const endTime = Date.now();
    logger.info(`Ingestion time: ${endTime - startTime}ms`);
    return result;
  } catch (error) {
    const endTime = Date.now();
    logger.error(`Ingestion failed for ${tableName}:`, {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
      duration: `${endTime - startTime}ms`,
      file: localFilePath,
    });
    throw error;
  }
}

export async function checkIfTableExists(tableName: string): Promise<boolean> {
  const tableExists = await clickhouse.query({
    query: `EXISTS TABLE ${tableName}`,
  });
  return (await tableExists.json<{ result: number }>()).data[0].result === 1;
}

export async function getTableNames(): Promise<string[]> {
  const result = await clickhouse.query({
    query: "SHOW TABLES WHERE name != 'parquet_import_tracking'",
  });
  return (await result.json<{ name: string }>()).data.map((row) => row.name);
}

export async function getLatestFullImport(tableName: string): Promise<{ processed_timestamp: number | null }> {
  const result = await clickhouse.query({
    query: `
      SELECT processed_timestamp
      FROM parquet_import_tracking
      WHERE table_name = '${tableName}'
      AND file_type = 'full'
      ORDER BY processed_timestamp DESC
      LIMIT 1
    `,
  });

  const data = await result.json<{ processed_timestamp: number }>();
  return {
    processed_timestamp: data.data[0]?.processed_timestamp || null,
  };
}

type ProcessedFile = {
  file_name: string;
};

export async function getProcessedIncrementalFiles(tableName: string): Promise<Set<string>> {
  const rows = await clickhouse.query({
    query: `
      SELECT file_name
      FROM parquet_import_tracking
      WHERE table_name = '${tableName}'
      AND file_type = 'incremental'      
    `,
    format: "JSONEachRow",
  });

  const result = await rows.json<ProcessedFile>();
  return new Set(result.map((row) => row.file_name));
}

export async function closeDBConnections() {
  await clickhouse.close();
}

import { createClient, type InsertResult } from "@clickhouse/client";
import { createReadStream, access, constants } from "node:fs";
import logger from "./logger";

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
  const tables = [
    `
      CREATE TABLE IF NOT EXISTS casts (
        id Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        timestamp DateTime64(6),
        fid Int64,
        hash String,
        parent_hash Nullable(String),
        parent_fid Nullable(Int64),
        parent_url Nullable(String),
        text String,
        embeds String,
        mentions Array(Int64),
        mentions_positions Array(Int16),
        root_parent_hash Nullable(String),
        root_parent_url Nullable(String)
      ) ENGINE = MergeTree()
      ORDER BY (fid, timestamp, id)
      PRIMARY KEY (fid, timestamp, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS fids (
        fid Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        custody_address String,
        registered_at Nullable(DateTime64(6))
      ) ENGINE = MergeTree()
      ORDER BY fid
      PRIMARY KEY (fid)
      `,
    `
      CREATE TABLE IF NOT EXISTS fnames (
        fname String,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        custody_address Nullable(String),
        expires_at Nullable(DateTime64(6)),
        fid Nullable(Int64),
        deleted_at Nullable(DateTime64(6))
      ) ENGINE = MergeTree()
      ORDER BY fname
      PRIMARY KEY (fname)
      `,
    `
      CREATE TABLE IF NOT EXISTS links (
        id Int64,
        fid Int64,
        target_fid Int64,
        hash String,
        timestamp DateTime64(6),
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        type String,
        display_timestamp Nullable(DateTime64(6))
      ) ENGINE = MergeTree()
      ORDER BY (fid, target_fid, type, id)
      PRIMARY KEY (fid, target_fid, type, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS reactions (
        id Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        timestamp DateTime64(6),
        reaction_type Int16,
        fid Int64,
        hash String,
        target_hash Nullable(String),
        target_fid Nullable(Int64),
        target_url Nullable(String)
      ) ENGINE = MergeTree()
      ORDER BY (fid, timestamp, id)
      PRIMARY KEY (fid, timestamp, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS signers (
        id Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        timestamp DateTime64(6),
        fid Int64,
        hash Nullable(String),
        custody_address Nullable(String),
        signer String,
        name Nullable(String),
        app_fid Nullable(Int64)
      ) ENGINE = MergeTree()
      ORDER BY (fid, timestamp, signer, id)
      PRIMARY KEY (fid, timestamp, signer, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS storage (
        id Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        timestamp DateTime64(6),
        fid Int64,
        units Int64,
        expiry DateTime64(6)
      ) ENGINE = MergeTree()
      ORDER BY (fid, units, expiry, id)
      PRIMARY KEY (fid, units, expiry, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS user_data (
        id Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        timestamp DateTime64(6),
        fid Int64,
        hash String,
        type Int16,
        value String
      ) ENGINE = MergeTree()
      ORDER BY (fid, type, id)
      PRIMARY KEY (fid, type, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS verifications (
        id Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        timestamp DateTime64(6),
        fid Int64,
        hash String,
        claim String
      ) ENGINE = MergeTree()
      ORDER BY (fid, timestamp, id)
      PRIMARY KEY (fid, timestamp, id)
      `,
    `
      CREATE TABLE IF NOT EXISTS profile_with_addresses (
        fid Int64,
        fname Nullable(String),
        display_name Nullable(String),
        avatar_url Nullable(String),
        bio Nullable(String),
        verified_addresses String,
        updated_at DateTime64(6)
      ) ENGINE = MergeTree()
      ORDER BY fid
      PRIMARY KEY (fid)
      `,
    `
      CREATE TABLE IF NOT EXISTS parquet_import_tracking (
        id UInt64,
        table_name String,
        file_name String,
        file_type String,
        is_empty Bool,        
        imported_at DateTime64(6) DEFAULT now64(),
        processed_timestamp Int64
      ) ENGINE = MergeTree()
      ORDER BY (table_name, file_name, imported_at)
      PRIMARY KEY (table_name, file_name, imported_at)
      `,
    `
      CREATE TABLE IF NOT EXISTS warpcast_power_users (
        fid Int64,
        created_at DateTime64(6),
        updated_at DateTime64(6),
        deleted_at Nullable(DateTime64(6))
      ) ENGINE = MergeTree()
      ORDER BY fid
      PRIMARY KEY (fid)
      `,
    `
      CREATE TABLE IF NOT EXISTS blocks (
        id Int64,
        created_at DateTime64(6),
        deleted_at Nullable(DateTime64(6)),
        blocker_fid Int64,
        blocked_fid Int64
      ) ENGINE = MergeTree()
      ORDER BY (blocker_fid, blocked_fid, id)
      PRIMARY KEY (blocker_fid, blocked_fid, id)
      `,
  ];

  const queryPromises = tables.map((tableQuery) =>
    clickhouse
      .query({
        query: tableQuery,
      })
      .catch((error) => {
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

    console.log(result);

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

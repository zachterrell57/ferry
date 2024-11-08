export const tableSchemas = {
  casts: `
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

  fids: `
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

  fnames: `
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

  links: `
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

  reactions: `
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

  signers: `
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

  storage: `
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

  user_data: `
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

  verifications: `
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

  profile_with_addresses: `
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

  parquet_import_tracking: `
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

  warpcast_power_users: `
    CREATE TABLE IF NOT EXISTS warpcast_power_users (
      fid Int64,
      created_at DateTime64(6),
      updated_at DateTime64(6),
      deleted_at Nullable(DateTime64(6))
    ) ENGINE = MergeTree()
    ORDER BY fid
    PRIMARY KEY (fid)
  `,

  blocks: `
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
} as const;

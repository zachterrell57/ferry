import { createClient } from "redis";
import logger from "./logger";

export async function clearRedisDB() {
  const client = createClient();
  try {
    await client.connect();
    await client.flushAll();
    logger.info("redis db cleared");
  } catch (error) {
    logger.error("failed to clear redis db:", error);
    throw error;
  } finally {
    await client.disconnect();
  }
}

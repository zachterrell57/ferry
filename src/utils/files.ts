import { mkdirSync } from "node:fs";
import { rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

export function formatFileSize(bytes: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`;
}

export function createTempSubfolder(): string {
  const tempSubfolder = path.join(os.tmpdir(), "parquet-importer");
  mkdirSync(tempSubfolder, { recursive: true });
  return tempSubfolder;
}

export async function cleanupTempSubfolder(): Promise<void> {
  const tempSubfolder = path.join(os.tmpdir(), "parquet-importer");
  await rm(tempSubfolder, { recursive: true, force: true });
}

import { Injectable, BeforeApplicationShutdown } from "@nestjs/common";
import { Pool, PoolClient } from "pg";
import { readFile, readdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
export function schema() {
  const name = process.env.PGSCHEMA ?? "public";
  if (!/^[a-z_][a-z0-9_]{0,50}$/.test(name))
    throw new Error("Invalid PGSCHEMA");
  return name;
}
export function pool() {
  return new Pool({
    connectionString: process.env.DATABASE_URL,
    options: `-c search_path=${schema()}`,
    max: 10,
    connectionTimeoutMillis: 3000,
    statement_timeout: 5000,
  });
}
@Injectable()
export class Database extends Pool implements BeforeApplicationShutdown {
  constructor() {
    super({
      connectionString: process.env.DATABASE_URL,
      options: `-c search_path=${schema()}`,
      max: 10,
      connectionTimeoutMillis: 3000,
      statement_timeout: 5000,
    });
  }
  async transaction<T>(fn: (tx: PoolClient) => Promise<T>) {
    const tx = await this.connect();
    try {
      await tx.query("BEGIN");
      const value = await fn(tx);
      await tx.query("COMMIT");
      return value;
    } catch (e) {
      await tx.query("ROLLBACK");
      throw e;
    } finally {
      tx.release();
    }
  }
  async beforeApplicationShutdown() {
    await this.end();
  }
}
export async function migrate() {
  const db = pool(),
    tx = await db.connect();
  try {
    await tx.query("BEGIN");
    await tx.query("SELECT pg_advisory_xact_lock(711048)");
    await tx.query(`CREATE SCHEMA IF NOT EXISTS ${schema()}`);
    await tx.query(`SET LOCAL search_path TO ${schema()}`);
    await tx.query(
      "CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, checksum TEXT)",
    );
    await tx.query(
      "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT",
    );
    const files = (await readdir(resolve("migrations")))
      .filter((file) => /^\d+.*\.sql$/.test(file))
      .sort();
    for (const file of files) {
      const version = Number(file.split("_")[0]),
        sql = (await readFile(resolve("migrations", file), "utf8")).replaceAll(
          "\r\n",
          "\n",
        ),
        checksum = createHash("sha256").update(sql).digest("hex");
      const old = await tx.query(
        "SELECT checksum FROM schema_migrations WHERE version=$1",
        [version],
      );
      if (old.rowCount) {
        if (old.rows[0].checksum && old.rows[0].checksum !== checksum)
          throw new Error(`Migration drift detected: ${file}`);
        if (!old.rows[0].checksum)
          await tx.query(
            "UPDATE schema_migrations SET checksum=$2 WHERE version=$1",
            [version, checksum],
          );
        continue;
      }
      await tx.query(sql);
      await tx.query(
        "INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)",
        [version, checksum],
      );
    }
    await tx.query("COMMIT");
  } catch (e) {
    await tx.query("ROLLBACK");
    throw e;
  } finally {
    tx.release();
    await db.end();
  }
}

import dotenv from "dotenv";
import { Pool } from "pg";

dotenv.config();

function getDatabaseConfig(): string | null {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }

  const dbHost = process.env.DB_HOST;
  const dbPort = process.env.DB_PORT ?? "5432";
  const dbName = process.env.DB_NAME;
  const dbUser = process.env.DB_USER;
  const dbPassword = process.env.DB_PASSWORD;

  if (!dbHost || !dbName || !dbUser || !dbPassword) {
    return null;
  }

  return `postgresql://${dbUser}:${dbPassword}@${dbHost}:${dbPort}/${dbName}`;
}

const connectionString = getDatabaseConfig();

const pool = connectionString
  ? new Pool({
      connectionString
    })
  : null;

function hasDatabaseConfig(): boolean {
  return pool !== null;
}

async function testDatabaseConnection(): Promise<void> {
  if (!pool) {
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("SELECT 1");
  } finally {
    client.release();
  }
}

function getDbPool(): Pool {
  if (!pool) {
    throw new Error("Database configuration is missing.");
  }

  return pool;
}

export { getDbPool, hasDatabaseConfig, testDatabaseConnection };
import PgBoss from "pg-boss";
import dotenv from "dotenv";

dotenv.config();

function getBossConnectionString(): string {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }

  const dbHost = process.env.DB_HOST;
  const dbPort = process.env.DB_PORT ?? "5432";
  const dbName = process.env.DB_NAME;
  const dbUser = process.env.DB_USER;
  const dbPassword = process.env.DB_PASSWORD;

  if (!dbHost || !dbName || !dbUser || !dbPassword) {
    throw new Error("Database configuration is missing for pg-boss.");
  }

  return `postgresql://${dbUser}:${dbPassword}@${dbHost}:${dbPort}/${dbName}`;
}

let boss: PgBoss | null = null;
let started = false;

async function getBoss(): Promise<PgBoss> {
  if (!boss) {
    boss = new PgBoss({ connectionString: getBossConnectionString() });
  }

  if (!started) {
    await boss.start();
    started = true;
  }

  return boss;
}

export async function enqueueJob<T extends object>(queueName: string, data: T): Promise<string> {
  const instance = await getBoss();
  await instance.createQueue(queueName);
  const id = await instance.send(queueName, data, { retryLimit: 3 });

  if (!id) {
    throw new Error(`Failed to enqueue job for queue ${queueName}.`);
  }

  return id;
}

export async function registerWorker<T extends object>(
  queueName: string,
  handler: (data: T) => Promise<void>,
  concurrency = 1
): Promise<void> {
  const instance = await getBoss();
  await instance.createQueue(queueName);

  const registrations: Promise<string>[] = [];

  for (let i = 0; i < concurrency; i += 1) {
    registrations.push(
      instance.work(queueName, { batchSize: 1 }, async (jobs) => {
        for (const job of jobs) {
          await handler(job.data as T);
        }
      })
    );
  }

  await Promise.all(registrations);
}

export async function stopBoss(): Promise<void> {
  if (!boss || !started) {
    return;
  }

  await boss.stop();
  started = false;
  boss = null;
}

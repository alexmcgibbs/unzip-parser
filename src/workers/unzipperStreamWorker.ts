import { randomUUID } from "node:crypto";
import { PassThrough, Transform, TransformCallback, Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { parentPort, workerData } from "node:worker_threads";
import got from "got";
import { PoolClient } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import * as unzipper from "unzipper";
import { getDbPool, hasDatabaseConfig } from "../db";

type WorkerPayload = {
  downloadUrl: string;
  originalFileName?: string;
};

type JsonToken = {
  name: string;
  value?: unknown;
};

type ParserToken = JsonToken | { name: "__docBoundary" };

type HoldingPayload = {
  ticker: string;
  cusip: string | null;
  description: string | null;
  quantity: number;
  market_value: number;
  cost_basis: number;
  price: number;
  asset_class: string | null;
};

type AccountPayload = {
  account_id: string;
  account_type: string;
  custodian: string;
  opened_date: string;
  status: string;
  cash_balance: number;
  total_value: number;
  holdings: HoldingPayload[];
};

type FileHeader = {
  client_id: string;
  first_name: string;
  last_name: string;
  email: string;
  advisor_id: string;
  last_updated: string;
};

type FlatTsvRecord =
  | { table: "files"; tsv: string }
  | { table: "accounts"; tsv: string }
  | { table: "holdings"; tsv: string };

interface AssemblerInstance {
  consume(token: JsonToken): void;
  readonly current: unknown;
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const AssemblerClass = require("stream-json/Assembler") as new () => AssemblerInstance;

// eslint-disable-next-line @typescript-eslint/no-require-imports
const { parser: createJsonParser } = require("stream-json") as {
  parser: (opts?: Record<string, unknown>) => Transform;
};

const PARSE_STATE = {
  ROOT_FIELDS: "ROOT_FIELDS",
  IN_ACCOUNTS_ARRAY: "IN_ACCOUNTS_ARRAY",
  READING_ACCOUNT: "READING_ACCOUNT",
  DONE: "DONE"
} as const;

type ParseState = (typeof PARSE_STATE)[keyof typeof PARSE_STATE];

const SCALAR_ROOT_FIELDS = new Set([
  "client_id",
  "first_name",
  "last_name",
  "email",
  "advisor_id",
  "last_updated"
]);

function tsvValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "\\N";
  }

  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

function toTsvLine(fields: unknown[]): string {
  return `${fields.map(tsvValue).join("\t")}\n`;
}

class HoldingFlattenTransform extends Transform {
  private readonly jobId: string;
  private readonly fileName: string;
  private state: ParseState = PARSE_STATE.ROOT_FIELDS;
  private header: Partial<FileHeader> = {};
  private currentKey = "";
  private accountAssembler: AssemblerInstance | null = null;
  private accountDepth = 0;
  private fileRowEmitted = false;

  constructor(jobId: string, fileName: string) {
    super({ objectMode: true });
    this.jobId = jobId;
    this.fileName = fileName;
  }

  _transform(token: ParserToken, _enc: BufferEncoding, callback: TransformCallback): void {
    try {
      if (token.name === "__docBoundary") {
        this.resetState();
        callback();
        return;
      }

      switch (this.state) {
        case PARSE_STATE.ROOT_FIELDS:
          this.handleRootField(token);
          break;
        case PARSE_STATE.IN_ACCOUNTS_ARRAY:
          this.handleAccountsArray(token);
          break;
        case PARSE_STATE.READING_ACCOUNT:
          this.handleAccountToken(token);
          break;
      }

      callback();
    } catch (error) {
      callback(error instanceof Error ? error : new Error("Failed to flatten JSON token stream."));
    }
  }

  private resetState(): void {
    this.state = PARSE_STATE.ROOT_FIELDS;
    this.header = {};
    this.currentKey = "";
    this.accountAssembler = null;
    this.accountDepth = 0;
    this.fileRowEmitted = false;
  }

  private emitFileRow(): void {
    const requiredKeys: Array<keyof FileHeader> = [
      "client_id",
      "first_name",
      "last_name",
      "email",
      "advisor_id",
      "last_updated"
    ];

    for (const key of requiredKeys) {
      if (typeof this.header[key] !== "string") {
        throw new Error(`Missing required JSON root field: ${key}`);
      }
    }

    this.push({
      table: "files",
      tsv: toTsvLine([
        this.jobId,
        this.fileName,
        this.header.client_id,
        this.header.first_name,
        this.header.last_name,
        this.header.email,
        this.header.advisor_id,
        this.header.last_updated
      ])
    } satisfies FlatTsvRecord);

    this.fileRowEmitted = true;
  }

  private emitAccountAndHoldings(account: AccountPayload): void {
    this.push({
      table: "accounts",
      tsv: toTsvLine([
        this.jobId,
        this.header.client_id,
        account.account_id,
        account.account_type,
        account.custodian,
        account.opened_date,
        account.status,
        account.cash_balance,
        account.total_value
      ])
    } satisfies FlatTsvRecord);

    for (const holding of account.holdings) {
      this.push({
        table: "holdings",
        tsv: toTsvLine([
          this.jobId,
          this.header.client_id,
          account.account_id,
          holding.ticker,
          holding.cusip,
          holding.description,
          holding.quantity,
          holding.market_value,
          holding.cost_basis,
          holding.price,
          holding.asset_class
        ])
      } satisfies FlatTsvRecord);
    }
  }

  private handleRootField(token: JsonToken): void {
    switch (token.name) {
      case "keyValue":
        this.currentKey = token.value as string;
        break;
      case "stringValue":
        if (SCALAR_ROOT_FIELDS.has(this.currentKey)) {
          (this.header as Record<string, string>)[this.currentKey] = token.value as string;
        }
        break;
      case "startArray":
        if (this.currentKey === "accounts") {
          if (!this.fileRowEmitted) {
            this.emitFileRow();
          }
          this.state = PARSE_STATE.IN_ACCOUNTS_ARRAY;
        }
        break;
      case "endObject":
        if (!this.fileRowEmitted) {
          this.emitFileRow();
        }
        this.state = PARSE_STATE.DONE;
        break;
    }
  }

  private handleAccountsArray(token: JsonToken): void {
    switch (token.name) {
      case "startObject":
        this.accountAssembler = new AssemblerClass();
        this.accountDepth = 1;
        this.accountAssembler.consume(token);
        this.state = PARSE_STATE.READING_ACCOUNT;
        break;
      case "endArray":
        this.state = PARSE_STATE.DONE;
        break;
    }
  }

  private handleAccountToken(token: JsonToken): void {
    if (token.name === "startObject" || token.name === "startArray") {
      this.accountDepth += 1;
    } else if (token.name === "endObject" || token.name === "endArray") {
      this.accountDepth -= 1;
    }

    this.accountAssembler?.consume(token);

    if (this.accountDepth === 0 && this.accountAssembler) {
      this.emitAccountAndHoldings(this.accountAssembler.current as AccountPayload);
      this.accountAssembler = null;
      this.state = PARSE_STATE.IN_ACCOUNTS_ARRAY;
    }
  }
}

class CopyDispatchWritable extends Writable {
  private readonly filesStream: PassThrough;
  private readonly accountsStream: PassThrough;
  private readonly holdingsStream: PassThrough;

  constructor(filesStream: PassThrough, accountsStream: PassThrough, holdingsStream: PassThrough) {
    super({ objectMode: true });
    this.filesStream = filesStream;
    this.accountsStream = accountsStream;
    this.holdingsStream = holdingsStream;
  }

  _write(record: FlatTsvRecord, _enc: BufferEncoding, callback: (error?: Error | null) => void): void {
    const target =
      record.table === "files"
        ? this.filesStream
        : record.table === "accounts"
          ? this.accountsStream
          : this.holdingsStream;

    if (target.write(record.tsv)) {
      callback();
      return;
    }

    target.once("drain", callback);
  }
}

async function ensureStagingTables(client: PoolClient): Promise<void> {
  await client.query(
    `CREATE TABLE IF NOT EXISTS ingest_files_stage (
       job_id TEXT NOT NULL,
       file_name TEXT NOT NULL,
       client_id TEXT NOT NULL,
       first_name TEXT NOT NULL,
       last_name TEXT NOT NULL,
       email TEXT NOT NULL,
       advisor_id TEXT NOT NULL,
       last_updated TIMESTAMPTZ NOT NULL
     );
     CREATE TABLE IF NOT EXISTS ingest_accounts_stage (
       job_id TEXT NOT NULL,
       client_id TEXT NOT NULL,
       account_id TEXT NOT NULL,
       account_type TEXT NOT NULL,
       custodian TEXT NOT NULL,
       opened_date DATE NOT NULL,
       status TEXT NOT NULL,
       cash_balance NUMERIC(18, 2) NOT NULL,
       total_value NUMERIC(18, 2) NOT NULL
     );
     CREATE TABLE IF NOT EXISTS ingest_holdings_stage (
       job_id TEXT NOT NULL,
       client_id TEXT NOT NULL,
       account_id TEXT NOT NULL,
       ticker TEXT NOT NULL,
       cusip TEXT,
       description TEXT,
       quantity NUMERIC(18, 6) NOT NULL,
       market_value NUMERIC(18, 2) NOT NULL,
       cost_basis NUMERIC(18, 2) NOT NULL,
       price NUMERIC(18, 6) NOT NULL,
       asset_class TEXT
     );
     CREATE INDEX IF NOT EXISTS ingest_files_stage_job_id_idx ON ingest_files_stage(job_id);
     CREATE INDEX IF NOT EXISTS ingest_accounts_stage_job_id_idx ON ingest_accounts_stage(job_id);
     CREATE INDEX IF NOT EXISTS ingest_holdings_stage_job_id_idx ON ingest_holdings_stage(job_id);`
  );
}

async function mergeStagingIntoNormalizedTables(client: PoolClient, jobId: string): Promise<void> {
  await client.query("BEGIN");

  try {
    await client.query(
      `INSERT INTO files (
         file_name,
         client_id,
         first_name,
         last_name,
         email,
         advisor_id,
         last_updated
       )
       SELECT DISTINCT
         s.file_name,
         s.client_id,
         s.first_name,
         s.last_name,
         s.email,
         s.advisor_id,
         s.last_updated
       FROM ingest_files_stage s
       WHERE s.job_id = $1
       ON CONFLICT (client_id)
       DO UPDATE SET
         file_name = EXCLUDED.file_name,
         first_name = EXCLUDED.first_name,
         last_name = EXCLUDED.last_name,
         email = EXCLUDED.email,
         advisor_id = EXCLUDED.advisor_id,
         last_updated = EXCLUDED.last_updated,
         updated_at = NOW()`,
      [jobId]
    );

    await client.query(
      `DELETE FROM accounts a
       USING files f
       WHERE a.file_id = f.id
         AND f.client_id IN (
           SELECT DISTINCT client_id
           FROM ingest_files_stage
           WHERE job_id = $1
         )`,
      [jobId]
    );

    await client.query(
      `INSERT INTO accounts (
         file_id,
         account_id,
         account_type,
         custodian,
         opened_date,
         status,
         cash_balance,
         total_value
       )
       SELECT
         f.id,
         a.account_id,
         a.account_type,
         a.custodian,
         a.opened_date,
         a.status,
         a.cash_balance,
         a.total_value
       FROM ingest_accounts_stage a
       JOIN files f ON f.client_id = a.client_id
       WHERE a.job_id = $1`,
      [jobId]
    );

    await client.query(
      `INSERT INTO holdings (
         account_id,
         ticker,
         cusip,
         description,
         quantity,
         market_value,
         cost_basis,
         price,
         asset_class
       )
       SELECT
         a.id,
         h.ticker,
         h.cusip,
         h.description,
         h.quantity,
         h.market_value,
         h.cost_basis,
         h.price,
         h.asset_class
       FROM ingest_holdings_stage h
       JOIN files f ON f.client_id = h.client_id
       JOIN accounts a ON a.file_id = f.id AND a.account_id = h.account_id
       WHERE h.job_id = $1`,
      [jobId]
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

async function cleanupStaging(client: PoolClient, jobId: string): Promise<void> {
  await client.query("DELETE FROM ingest_holdings_stage WHERE job_id = $1", [jobId]);
  await client.query("DELETE FROM ingest_accounts_stage WHERE job_id = $1", [jobId]);
  await client.query("DELETE FROM ingest_files_stage WHERE job_id = $1", [jobId]);
}

function originalFileNameFromUrl(downloadUrl: string): string {
  try {
    const url = new URL(downloadUrl);
    const segments = url.pathname.split("/").filter(Boolean);
    return segments[segments.length - 1] ?? "download.zip";
  } catch {
    return "download.zip";
  }
}

async function streamZipToConcurrentCopy(
  downloadUrl: string,
  originalFileName: string,
  jobId: string,
  filesInput: PassThrough,
  accountsInput: PassThrough,
  holdingsInput: PassThrough
): Promise<void> {
  await pipeline(
    got.stream(downloadUrl),
    unzipper.Parse({ forceStream: true }),
    async function* (entries: AsyncIterable<unzipper.Entry>): AsyncIterable<ParserToken> {
      for await (const entry of entries) {
        if (entry.type !== "File" || !entry.path.toLowerCase().endsWith(".json")) {
          entry.autodrain();
          continue;
        }

        yield { name: "__docBoundary" };

        const parserStream = entry.pipe(createJsonParser());
        for await (const token of parserStream as AsyncIterable<JsonToken>) {
          yield token;
        }
      }
    },
    new HoldingFlattenTransform(jobId, originalFileName),
    new CopyDispatchWritable(filesInput, accountsInput, holdingsInput)
  );
}

(async function run() {
  const jobId = randomUUID();
  if (!hasDatabaseConfig()) {
    parentPort?.postMessage({ error: "Postgres connection not configured." });
    process.exit(1);
  }

  const pool = getDbPool();
  const filesClient = await pool.connect();
  const accountsClient = await pool.connect();
  const holdingsClient = await pool.connect();
  const mergeClient = await pool.connect();

  try {
    const { downloadUrl, originalFileName } = workerData as WorkerPayload;
    if (!downloadUrl) {
      throw new Error("downloadUrl is required for unzipperStreamWorker.");
    }

    const fileName = originalFileName ?? originalFileNameFromUrl(downloadUrl);

    await ensureStagingTables(mergeClient);

    const filesInput = new PassThrough();
    const accountsInput = new PassThrough();
    const holdingsInput = new PassThrough();

    const filesCopyPromise = pipeline(
      filesInput,
      filesClient.query(
        copyFrom(
          `COPY ingest_files_stage (
             job_id,
             file_name,
             client_id,
             first_name,
             last_name,
             email,
             advisor_id,
             last_updated
           ) FROM STDIN WITH (FORMAT text)`
        )
      )
    );

    const accountsCopyPromise = pipeline(
      accountsInput,
      accountsClient.query(
        copyFrom(
          `COPY ingest_accounts_stage (
             job_id,
             client_id,
             account_id,
             account_type,
             custodian,
             opened_date,
             status,
             cash_balance,
             total_value
           ) FROM STDIN WITH (FORMAT text)`
        )
      )
    );

    const holdingsCopyPromise = pipeline(
      holdingsInput,
      holdingsClient.query(
        copyFrom(
          `COPY ingest_holdings_stage (
             job_id,
             client_id,
             account_id,
             ticker,
             cusip,
             description,
             quantity,
             market_value,
             cost_basis,
             price,
             asset_class
           ) FROM STDIN WITH (FORMAT text)`
        )
      )
    );

    try {
      await streamZipToConcurrentCopy(
        downloadUrl,
        fileName,
        jobId,
        filesInput,
        accountsInput,
        holdingsInput
      );
    } finally {
      filesInput.end();
      accountsInput.end();
      holdingsInput.end();
    }

    await Promise.all([filesCopyPromise, accountsCopyPromise, holdingsCopyPromise]);

    await mergeStagingIntoNormalizedTables(mergeClient, jobId);
    await cleanupStaging(mergeClient, jobId);

    parentPort?.postMessage({ ok: true, jobId });
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";

    try {
      await cleanupStaging(mergeClient, jobId);
    } catch {
      // Best-effort cleanup.
    }

    parentPort?.postMessage({ error: message });
    process.exit(1);
  } finally {
    filesClient.release();
    accountsClient.release();
    holdingsClient.release();
    mergeClient.release();
  }
})();

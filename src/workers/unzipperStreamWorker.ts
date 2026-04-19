import { Transform, TransformCallback, Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { parentPort, workerData } from "node:worker_threads";
import got from "got";
import { PoolClient } from "pg";
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

type HeaderEvent = { type: "header"; data: FileHeader };
type AccountEvent = { type: "account"; data: AccountPayload };
type StreamEvent = HeaderEvent | AccountEvent;

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

class StreamingJsonAccountExtractor extends Transform {
  private state: ParseState = PARSE_STATE.ROOT_FIELDS;
  private header: Partial<FileHeader> = {};
  private currentKey = "";
  private accountAssembler: AssemblerInstance | null = null;
  private accountDepth = 0;
  private emittedHeader = false;

  constructor() {
    super({ objectMode: true });
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
      callback(error instanceof Error ? error : new Error("Failed to process JSON token stream."));
    }
  }

  private resetState(): void {
    this.state = PARSE_STATE.ROOT_FIELDS;
    this.header = {};
    this.currentKey = "";
    this.accountAssembler = null;
    this.accountDepth = 0;
    this.emittedHeader = false;
  }

  private emitHeaderIfNeeded(): void {
    if (this.emittedHeader) {
      return;
    }

    const required: Array<keyof FileHeader> = [
      "client_id",
      "first_name",
      "last_name",
      "email",
      "advisor_id",
      "last_updated"
    ];

    for (const key of required) {
      if (typeof this.header[key] !== "string") {
        throw new Error(`Missing required JSON root field: ${key}`);
      }
    }

    this.push({ type: "header", data: this.header as FileHeader } satisfies HeaderEvent);
    this.emittedHeader = true;
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
          this.emitHeaderIfNeeded();
          this.state = PARSE_STATE.IN_ACCOUNTS_ARRAY;
        }
        break;
      case "endObject":
        this.emitHeaderIfNeeded();
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
      this.push({ type: "account", data: this.accountAssembler.current as AccountPayload } satisfies AccountEvent);
      this.accountAssembler = null;
      this.state = PARSE_STATE.IN_ACCOUNTS_ARRAY;
    }
  }
}

class DirectPersistenceWritable extends Writable {
  private readonly client: PoolClient;
  private readonly fileName: string;
  private currentClientId: string | null = null;

  constructor(client: PoolClient, fileName: string) {
    super({ objectMode: true });
    this.client = client;
    this.fileName = fileName;
  }

  _write(event: StreamEvent, _enc: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.persistEvent(event).then(() => callback()).catch((error) => {
      callback(error instanceof Error ? error : new Error("Failed to persist stream event."));
    });
  }

  private async persistEvent(event: StreamEvent): Promise<void> {
    if (event.type === "header") {
      const header = event.data;
      this.currentClientId = header.client_id;

      await this.client.query(
        `INSERT INTO files (
           file_name,
           client_id,
           first_name,
           last_name,
           email,
           advisor_id,
           last_updated
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (client_id)
         DO UPDATE SET
           file_name = EXCLUDED.file_name,
           first_name = EXCLUDED.first_name,
           last_name = EXCLUDED.last_name,
           email = EXCLUDED.email,
           advisor_id = EXCLUDED.advisor_id,
           last_updated = EXCLUDED.last_updated,
           updated_at = NOW()`,
        [
          this.fileName,
          header.client_id,
          header.first_name,
          header.last_name,
          header.email,
          header.advisor_id,
          header.last_updated
        ]
      );

      await this.client.query("DELETE FROM accounts WHERE client_id = $1", [header.client_id]);
      return;
    }

    if (!this.currentClientId) {
      throw new Error("Received account data before file header.");
    }

    const account = event.data;

    await this.client.query(
      `INSERT INTO accounts (
         client_id,
         account_id,
         account_type,
         custodian,
         opened_date,
         status,
         cash_balance,
         total_value
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (account_id)
       DO UPDATE SET
         client_id = EXCLUDED.client_id,
         account_type = EXCLUDED.account_type,
         custodian = EXCLUDED.custodian,
         opened_date = EXCLUDED.opened_date,
         status = EXCLUDED.status,
         cash_balance = EXCLUDED.cash_balance,
         total_value = EXCLUDED.total_value,
         updated_at = NOW()`,
      [
        this.currentClientId,
        account.account_id,
        account.account_type,
        account.custodian,
        account.opened_date,
        account.status,
        account.cash_balance,
        account.total_value
      ]
    );

    await this.client.query("DELETE FROM holdings WHERE account_id = $1", [account.account_id]);

    if (account.holdings.length > 0) {
      await this.client.query(
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
           $1,
           unnest($2::text[]),
           unnest($3::text[]),
           unnest($4::text[]),
           unnest($5::numeric[]),
           unnest($6::numeric[]),
           unnest($7::numeric[]),
           unnest($8::numeric[]),
           unnest($9::text[])`,
        [
          account.account_id,
          account.holdings.map((h) => h.ticker),
          account.holdings.map((h) => h.cusip),
          account.holdings.map((h) => h.description),
          account.holdings.map((h) => h.quantity),
          account.holdings.map((h) => h.market_value),
          account.holdings.map((h) => h.cost_basis),
          account.holdings.map((h) => h.price),
          account.holdings.map((h) => h.asset_class)
        ]
      );
    }
  }
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

async function streamZipToPersistence(
  downloadUrl: string,
  sink: DirectPersistenceWritable
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
    new StreamingJsonAccountExtractor(),
    sink
  );
}

(async function run() {
  if (!hasDatabaseConfig()) {
    parentPort?.postMessage({ error: "Postgres connection not configured." });
    process.exit(1);
  }

  const pool = getDbPool();
  const client = await pool.connect();

  try {
    const { downloadUrl, originalFileName } = workerData as WorkerPayload;
    if (!downloadUrl) {
      throw new Error("downloadUrl is required for unzipperStreamWorker.");
    }

    const fileName = originalFileName ?? originalFileNameFromUrl(downloadUrl);

    await client.query("BEGIN");
    try {
      await streamZipToPersistence(downloadUrl, new DirectPersistenceWritable(client, fileName));
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }

    parentPort?.postMessage({ ok: true });
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown worker error";
    parentPort?.postMessage({ error: message });
    process.exit(1);
  } finally {
    client.release();
  }
})();
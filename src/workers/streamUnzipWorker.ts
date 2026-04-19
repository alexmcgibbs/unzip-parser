import { Transform, TransformCallback } from "node:stream";
import * as unzipper from "unzipper";
import { PoolClient } from "pg";
import { getDbPool } from "../db";

// ─── Types ───────────────────────────────────────────────────────────────────

type FileHeader = {
  client_id: string;
  first_name: string;
  last_name: string;
  email: string;
  advisor_id: string;
  last_updated: string;
};

type HoldingRecord = {
  ticker: string;
  cusip: string | null;
  description: string | null;
  quantity: number;
  market_value: number;
  cost_basis: number;
  price: number;
  asset_class: string | null;
};

type AccountRecord = {
  account_id: string;
  account_type: string;
  custodian: string;
  opened_date: string;
  status: string;
  cash_balance: number;
  total_value: number;
  holdings: HoldingRecord[];
};

type HeaderEvent = { type: "header"; data: FileHeader };
type AccountEvent = { type: "account"; data: AccountRecord };
type StreamEvent = HeaderEvent | AccountEvent;

type JsonToken = { name: string; value?: unknown };

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

// ─── State machine constants ──────────────────────────────────────────────────

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

// ─── Streaming JSON Account Extractor ────────────────────────────────────────
//
// Consumes the stream-json token stream and emits two types of events (in object
// mode):
//   { type: "header", data: FileHeader }   — once, before any accounts
//   { type: "account", data: AccountRecord } — once per account in the array
//
// At any point only ONE account's data (plus a tiny header struct) is held in
// memory. The rest of the file has either already been persisted or not yet read.

class StreamingJsonAccountExtractor extends Transform {
  private state: ParseState = PARSE_STATE.ROOT_FIELDS;
  private header: Partial<FileHeader> = {};
  private currentKey = "";
  private accountAssembler: AssemblerInstance | null = null;
  private accountDepth = 0;

  constructor() {
    super({ objectMode: true });
  }

  _transform(token: JsonToken, _enc: BufferEncoding, callback: TransformCallback): void {
    try {
      switch (this.state) {
        case PARSE_STATE.ROOT_FIELDS:
          this._handleRootField(token);
          break;
        case PARSE_STATE.IN_ACCOUNTS_ARRAY:
          this._handleAccountsArray(token);
          break;
        case PARSE_STATE.READING_ACCOUNT:
          this._handleAccountToken(token);
          break;
      }
      callback();
    } catch (err) {
      callback(err instanceof Error ? err : new Error("Token processing error"));
    }
  }

  _flush(callback: TransformCallback): void {
    callback();
  }

  private _handleRootField(token: JsonToken): void {
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
          this.push({ type: "header", data: this.header as FileHeader } satisfies HeaderEvent);
          this.state = PARSE_STATE.IN_ACCOUNTS_ARRAY;
        }
        break;
    }
  }

  private _handleAccountsArray(token: JsonToken): void {
    switch (token.name) {
      case "startObject":
        // Beginning of one account object — start assembling it
        this.accountAssembler = new AssemblerClass();
        this.accountDepth = 1;
        this.accountAssembler.consume(token);
        this.state = PARSE_STATE.READING_ACCOUNT;
        break;
      case "endArray":
        // All accounts consumed
        this.state = PARSE_STATE.DONE;
        break;
    }
  }

  private _handleAccountToken(token: JsonToken): void {
    if (token.name === "startObject" || token.name === "startArray") {
      this.accountDepth++;
    } else if (token.name === "endObject" || token.name === "endArray") {
      this.accountDepth--;
    }

    this.accountAssembler!.consume(token);

    if (this.accountDepth === 0) {
      // Account object fully assembled — emit and release memory immediately
      this.push({ type: "account", data: this.accountAssembler!.current as AccountRecord } satisfies AccountEvent);
      this.accountAssembler = null;
      this.state = PARSE_STATE.IN_ACCOUNTS_ARRAY;
    }
  }
}

// ─── DB persistence helpers ───────────────────────────────────────────────────

async function persistHeader(
  header: FileHeader,
  fileName: string,
  client: PoolClient
): Promise<number> {
  const existing = await client.query<{ id: number }>(
    "SELECT id FROM files WHERE client_id = $1",
    [header.client_id]
  );

  if (existing.rows.length > 0) {
    await client.query("DELETE FROM accounts WHERE file_id = $1", [existing.rows[0].id]);
  }

  const result = await client.query<{ id: number }>(
    `INSERT INTO files (file_name, client_id, first_name, last_name, email, advisor_id, last_updated)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (client_id) DO UPDATE SET
       file_name    = EXCLUDED.file_name,
       first_name   = EXCLUDED.first_name,
       last_name    = EXCLUDED.last_name,
       email        = EXCLUDED.email,
       advisor_id   = EXCLUDED.advisor_id,
       last_updated = EXCLUDED.last_updated,
       updated_at   = NOW()
     RETURNING id`,
    [
      fileName,
      header.client_id,
      header.first_name,
      header.last_name,
      header.email,
      header.advisor_id,
      header.last_updated
    ]
  );

  return result.rows[0].id;
}

async function persistAccount(
  account: AccountRecord,
  fileId: number,
  client: PoolClient
): Promise<void> {
  const accountResult = await client.query<{ id: number }>(
    `INSERT INTO accounts
       (file_id, account_id, account_type, custodian, opened_date, status, cash_balance, total_value)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     RETURNING id`,
    [
      fileId,
      account.account_id,
      account.account_type,
      account.custodian,
      account.opened_date,
      account.status,
      account.cash_balance,
      account.total_value
    ]
  );

  const accountRowId = accountResult.rows[0].id;

  for (const holding of account.holdings) {
    await client.query(
      `INSERT INTO holdings
         (account_id, ticker, cusip, description, quantity, market_value, cost_basis, price, asset_class)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        accountRowId,
        holding.ticker,
        holding.cusip ?? null,
        holding.description ?? null,
        holding.quantity,
        holding.market_value,
        holding.cost_basis,
        holding.price,
        holding.asset_class ?? null
      ]
    );
  }
}

// ─── Main export ─────────────────────────────────────────────────────────────
//
// Streams the ZIP entry → stream-json tokenizer → account extractor transform.
// The `for await` loop naturally backpressures the whole pipeline: while a DB
// write is awaited, no new tokens are consumed from the JSON parser, which
// backpressures the unzipper entry, which backpressures the source stream.
// At most ONE account object (plus the small file header) is ever in memory.

export async function processStreamZipToPostgres(
  fileStream: NodeJS.ReadableStream,
  originalFileName: string
): Promise<void> {
  const pool = getDbPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    await new Promise<void>((resolve, reject) => {
      const zipParse = fileStream.pipe(unzipper.Parse({ forceStream: true }));
      let jsonFound = false;

      zipParse.on("entry", (entry: unzipper.Entry) => {
        if (!jsonFound && entry.path.toLowerCase().endsWith(".json")) {
          jsonFound = true;

          const extractor = new StreamingJsonAccountExtractor();
          entry.pipe(createJsonParser()).pipe(extractor);

          let fileId: number | null = null;

          const process = async () => {
            for await (const event of extractor as AsyncIterable<StreamEvent>) {
              if (event.type === "header") {
                fileId = await persistHeader(event.data, originalFileName, client);
              } else if (event.type === "account" && fileId !== null) {
                await persistAccount(event.data, fileId, client);
              }
            }
          };

          process().then(resolve).catch(reject);
        } else {
          entry.autodrain();
        }
      });

      zipParse.once("finish", () => {
        if (!jsonFound) {
          reject(new Error("No JSON file found in ZIP stream."));
        }
      });

      zipParse.once("error", reject);
    });

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

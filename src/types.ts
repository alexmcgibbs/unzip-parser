export type ZipEntryResult = {
  name: string;
  size: number;
  compressedSize: number;
  isDirectory: boolean;
};

export type WorkerSuccess = {
  archivePath: string;
  extractedTo: string;
  totalEntries: number;
  entries: ZipEntryResult[];
  extractedFiles: string[];
  jsonFilePath: string;
  jsonContents: unknown;
};

export type WorkerError = {
  error: string;
};

export type HoldingPayload = {
  ticker: string;
  cusip: string | null;
  description: string | null;
  quantity: number;
  market_value: number;
  cost_basis: number;
  price: number;
  asset_class: string | null;
};

export type AccountPayload = {
  account_id: string;
  account_type: string;
  custodian: string;
  opened_date: string;
  status: string;
  cash_balance: number;
  total_value: number;
  holdings: HoldingPayload[];
};

export type FilePayload = {
  client_id: string;
  first_name: string;
  last_name: string;
  email: string;
  advisor_id: string;
  last_updated: string;
  accounts: AccountPayload[];
};

export type PersistenceSummary = {
  clientId: string;
  accountsInserted: number;
  holdingsInserted: number;
};

function asObject(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string.`);
  }
  return value;
}

function asNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    throw new Error(`${label} must be a number.`);
  }
  return value;
}

function asArray(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array.`);
  }
  return value;
}

function asOptionalString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function parseHoldingPayload(value: unknown): HoldingPayload {
  const row = asObject(value, "holding");
  return {
    ticker: asString(row.ticker, "holding.ticker"),
    cusip: asOptionalString(row.cusip),
    description: asOptionalString(row.description),
    quantity: asNumber(row.quantity, "holding.quantity"),
    market_value: asNumber(row.market_value, "holding.market_value"),
    cost_basis: asNumber(row.cost_basis, "holding.cost_basis"),
    price: asNumber(row.price, "holding.price"),
    asset_class: asOptionalString(row.asset_class)
  };
}

function parseAccountPayload(value: unknown): AccountPayload {
  const row = asObject(value, "account");
  const holdings = asArray(row.holdings, "account.holdings").map(parseHoldingPayload);

  return {
    account_id: asString(row.account_id, "account.account_id"),
    account_type: asString(row.account_type, "account.account_type"),
    custodian: asString(row.custodian, "account.custodian"),
    opened_date: asString(row.opened_date, "account.opened_date"),
    status: asString(row.status, "account.status"),
    cash_balance: asNumber(row.cash_balance, "account.cash_balance"),
    total_value: asNumber(row.total_value, "account.total_value"),
    holdings
  };
}

export function parseFilePayload(value: unknown): FilePayload {
  const row = asObject(value, "json payload");
  const accounts = asArray(row.accounts, "accounts").map(parseAccountPayload);

  return {
    client_id: asString(row.client_id, "client_id"),
    first_name: asString(row.first_name, "first_name"),
    last_name: asString(row.last_name, "last_name"),
    email: asString(row.email, "email"),
    advisor_id: asString(row.advisor_id, "advisor_id"),
    last_updated: asString(row.last_updated, "last_updated"),
    accounts
  };
}

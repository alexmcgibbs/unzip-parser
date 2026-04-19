CREATE TABLE IF NOT EXISTS accounts (
  client_id TEXT NOT NULL REFERENCES files(client_id) ON DELETE CASCADE,
  account_id TEXT NOT NULL PRIMARY KEY,
  account_type TEXT NOT NULL,
  custodian TEXT NOT NULL,
  opened_date DATE NOT NULL,
  status TEXT NOT NULL,
  holdings JSONB NOT NULL DEFAULT '[]'::jsonb,
  cash_balance NUMERIC(18, 2) NOT NULL,
  total_value NUMERIC(18, 2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS accounts_client_id_idx ON accounts (client_id);
CREATE INDEX IF NOT EXISTS accounts_account_id_idx ON accounts (account_id);

CREATE TABLE IF NOT EXISTS accounts (
  id BIGSERIAL PRIMARY KEY,
  file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  account_type TEXT NOT NULL,
  custodian TEXT NOT NULL,
  opened_date DATE NOT NULL,
  status TEXT NOT NULL,
  holdings JSONB NOT NULL DEFAULT '[]'::jsonb,
  cash_balance NUMERIC(18, 2) NOT NULL,
  total_value NUMERIC(18, 2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT accounts_file_id_account_id_uq UNIQUE (file_id, account_id)
);

CREATE INDEX IF NOT EXISTS accounts_file_id_idx ON accounts (file_id);
CREATE INDEX IF NOT EXISTS accounts_account_id_idx ON accounts (account_id);

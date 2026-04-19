CREATE TABLE IF NOT EXISTS holdings (
  id BIGSERIAL PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  ticker TEXT NOT NULL,
  cusip TEXT,
  description TEXT,
  quantity NUMERIC(18, 6) NOT NULL,
  market_value NUMERIC(18, 2) NOT NULL,
  cost_basis NUMERIC(18, 2) NOT NULL,
  price NUMERIC(18, 6) NOT NULL,
  asset_class TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS holdings_account_id_idx ON holdings (account_id);
CREATE INDEX IF NOT EXISTS holdings_ticker_idx ON holdings (ticker);
CREATE INDEX IF NOT EXISTS holdings_cusip_idx ON holdings (cusip);
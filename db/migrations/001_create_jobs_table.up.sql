CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  pgboss_job_id UUID,
  status TEXT NOT NULL,
  retries INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status);
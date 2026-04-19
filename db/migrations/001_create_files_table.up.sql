CREATE TABLE IF NOT EXISTS files (
  file_name TEXT NOT NULL,
  client_id TEXT NOT NULL PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT NOT NULL,
  advisor_id TEXT NOT NULL,
  last_updated TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS files_client_id_uq ON files (client_id);
CREATE INDEX IF NOT EXISTS files_advisor_id_idx ON files (advisor_id);
CREATE INDEX IF NOT EXISTS files_file_name_idx ON files (file_name);

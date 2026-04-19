#!/bin/sh
set -eu

echo "Running SQL up migrations from /migrations"

found=0
for migration in /migrations/*.up.sql; do
  if [ ! -f "$migration" ]; then
    continue
  fi

  found=1
  echo "Applying migration: $migration"
  psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --file "$migration"
done

if [ "$found" -eq 0 ]; then
  echo "No .up.sql migration files found under /migrations"
fi

echo "Migration initialization complete"

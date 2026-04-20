# Webhook ZIP Worker Service

Express + TypeScript service that accepts a ZIP file URL on a webhook endpoint, downloads and processes it asynchronously in a Poolifier worker thread pool, and persists results to Postgres.

## What it does

- Accepts `POST /webhook` with a JSON body containing a `fileUrl`
- Creates a job record in Postgres and enqueues it via pg-boss
- A pool of worker threads downloads the ZIP from the URL, streams and extracts entries, parses JSON payloads, and persists accounts/holdings to Postgres
- Failed jobs are retried up to 3 times (network and transient errors only)
- Job status is queryable at any time via `GET /job/:id`

## Architecture

- **Express** — HTTP layer (routes only in `src/index.ts`)
- **pg-boss** — durable job queue backed by Postgres; workers aligned to thread pool size
- **Poolifier `FixedThreadPool`** — parallel ZIP processing; pool size defaults to `min(availableParallelism(), 4)`, overridable via `WORKER_CONCURRENCY`
- **got** — streams the ZIP file from the remote URL inside each worker thread
- **unzipper** — parses the ZIP stream entry-by-entry without buffering the full archive to disk
- **Postgres** — job tracking (`jobs`, `files`, `accounts`, `holdings` tables); migrations in `db/migrations/`

## Setup

```bash
npm install
npm run build
npm start
```

Server starts on port `3000` by default.

Copy `.env.example` to `.env` for local Postgres configuration.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `WORKER_CONCURRENCY` | `min(availableParallelism(), 4)` | Worker thread pool size |
| `DB_HOST` | — | Postgres host |
| `DB_PORT` | `5432` | Postgres port |
| `DB_NAME` | — | Postgres database name |
| `DB_USER` | — | Postgres user |
| `DB_PASSWORD` | — | Postgres password |

## Docker Setup

Start the API and Postgres together:

```bash
docker compose up --build
```

Services:

- API: `http://localhost:3000`
- Postgres: `localhost:5432`

On first Postgres container creation (empty data volume), the container automatically runs all `db/migrations/*.up.sql` files.

To reset the database and start fresh:

```bash
docker compose down -v
docker compose up --build
```

## Endpoints

### `POST /webhook`

Enqueue a ZIP file for processing.

**Request:**
```json
{ "fileUrl": "https://example.com/data.zip" }
```

**Response:**
```json
{
  "message": "ZIP received and queued for processing.",
  "jobId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### `GET /job/:id`

Poll job status.

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "complete",
  "url": "https://example.com/data.zip",
  "retries": 0,
  "error": null
}
```

Possible `status` values: `queued`, `started`, `complete`, `error`.

## Testing

Run all tests (build + test runner):

```bash
npm test
```

Run individual test files:

```bash
node --test tests/webhook-upload.test.js
node --test tests/unzipper-worker-http-url.test.js
node --test tests/webhook-db-persistence.test.js
```

The DB persistence test is automatically skipped if Postgres is not reachable or if required tables do not exist.

To run DB-backed tests with Docker from a clean database state:

```bash
docker compose down -v
docker compose up --build -d
npm test
```

## Test with curl

Enqueue a ZIP for processing:

```bash
curl -X POST http://localhost:3000/webhook \
  -H "Content-Type: application/json" \
  -d '{"fileUrl": "http://localhost:3000/test"}'
```

Check job status (replace `<jobId>` with the ID returned above):

```bash
curl http://localhost:3000/job/cf4be850-f1a9-481d-831f-b90a3ee6c5ff
```



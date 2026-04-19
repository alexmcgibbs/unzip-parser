# Webhook ZIP Worker Service

Express + TypeScript service that accepts a ZIP upload on a webhook endpoint, then unzips and analyzes it in a worker thread.

The project also includes Docker support for running the API alongside a separate Postgres container.

## What it does

- Exposes `POST /webhook`
- Accepts `multipart/form-data` with a file field named `file`
- Saves uploaded ZIP into `uploads/`
- Uses a worker thread to:
  - unzip archive contents
  - list ZIP entries and extracted files
  - print results to stdout
- Returns processing result JSON in HTTP response

## Setup

```bash
npm install
npm run build
npm start
```

Server starts on port `3000` by default.

Additional environment variables:

- `FILE_LIMIT`: max accepted upload size for Multer. Defaults to `2gb`.
- `LARGE_FILE_LIMIT`: threshold for switching from the regular unzip worker to the child-process-based large worker. Defaults to `50mb`.
  If the `unzip` binary is unavailable, the app falls back to the regular worker.

## Docker Setup

Start the API and Postgres together:

```bash
docker compose up --build
```

Services:

- API: `http://localhost:3000`
- Postgres: `localhost:5432`

The API image installs the `unzip` binary so large-file processing works inside Docker.

On first Postgres container creation (empty data volume), the container automatically runs all `db/migrations/*.up.sql` files.

If you need to re-run initialization migrations from scratch, remove the Postgres volume and recreate containers:

```bash
docker compose down -v
docker compose up --build
```

The app container receives these database environment variables:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

You can copy `.env.example` for local non-Docker development if you want the service to connect to a local Postgres instance.

## Testing

Run all tests (build + test runner):

```bash
npm test
```

Run only the webhook response test:

```bash
node --test tests/webhook-upload.test.js
```

Run only the DB persistence test:

```bash
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

```bash
curl -X POST http://localhost:3000/webhook \
  -F "file=@/mnt/c/Users/Alex/GitHub/fartherTS/sample.zip"
```

## Response example

```json
{
  "message": "ZIP received and processed successfully.",
  "result": {
    "archivePath": "...",
    "extractedTo": "...",
    "totalEntries": 3,
    "entries": [
      {
        "name": "docs/readme.txt",
        "size": 120,
        "compressedSize": 84,
        "isDirectory": false
      }
    ],
    "extractedFiles": [
      "docs/readme.txt"
    ]
  }
}
```

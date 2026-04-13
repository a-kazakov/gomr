# gomr-vis-cloudflare

Cloudflare Pages deployment of the gomr pipeline visualization tool. Uses Pages Advanced Mode (`_worker.js`) with Durable Objects for real-time status updates via WebSocket, and KV for cold storage of archived jobs.

## Architecture

- **Pages + Advanced Mode** — Static assets served by Pages; a bundled `_worker.js` handles all API routes.
- **Durable Object (`JobStatus`)** — One instance per job. Stores current status, broadcasts to WebSocket clients, manages lifecycle via alarms.
- **KV (`GOMR_VIS`)** — Persistent storage. The DO archives its final status here on destruction, and hydrates from it when waking up for the first time.

The Worker source lives in `src/` and is bundled into `client/dist/_worker.js` via esbuild during the build step. Pages detects `_worker.js` in the output directory and enters Advanced Mode.

## Prerequisites

- Node.js
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) (`npm install -g wrangler`)
- A Cloudflare account (`wrangler login`)

## Setup

1. Create the KV namespace:

```bash
wrangler kv namespace create GOMR_VIS
```

2. Copy the returned namespace ID into `wrangler.toml`:

```toml
[[kv_namespaces]]
binding = "GOMR_VIS"
id = "<your-namespace-id>"
```

3. Install dependencies:

```bash
npm install
```

## Local development

```bash
npm run dev
```

This builds the client and Worker, then starts `wrangler pages dev` with local Durable Object and KV emulation.

## Deploy

```bash
npm run deploy
```

This builds the client and Worker, then deploys to Cloudflare Pages via `wrangler pages deploy`.

You can also deploy with a custom project name:

```bash
npm run build && wrangler pages deploy client/dist --project-name my-project
```

## Configuration

Environment variables (set via `wrangler secret` or dashboard):

| Variable | Default | Description |
|---|---|---|
| `PUSH_AUTH_TOKEN` | — | Bearer token required for push endpoints. Omit to disable auth. |
| `VIEW_BASIC_AUTH` | — | `user:password` for basic auth on read endpoints. |
| `SELF_DESTRUCT_MS` | `31536000000` | Hard TTL (1 year) after which DOs always self-destruct. |
| `IDLE_QUIET_MS` | `3600000` | Time (1 hour) with no updates and no clients before early cleanup. |
| `HOUSEKEEP_MS` | `86400000` | Interval (24 hours) between housekeeping alarm checks. |

## API

### `POST /status`

Upsert job status. Creates or updates the Durable Object for the given job.

```json
{
  "jobId": "pipeline-abc-123",
  "status": {
    "operations": { ... },
    "collections": { ... },
    "values": { ... }
  }
}
```

Response: `204 No Content`

### `GET /status/ws?jobId=pipeline-abc-123`

WebSocket endpoint. Receives real-time status pushes.

Messages from server:
```json
{ "type": "status", "jobId": "...", "status": { ... }, "updatedAt": 1713000000000 }
{ "type": "closed", "reason": "idle_timeout" }
```

### `GET /status/:jobId`

Returns current status as JSON. Checks the live Durable Object first, falls back to KV archive.

```json
{
  "jobId": "pipeline-abc-123",
  "status": { ... },
  "updatedAt": 1713000000000,
  "source": "live"
}
```

### Legacy endpoints

These are preserved for backward compatibility during migration:

- `POST /push/:jobId` — Push pipeline metrics (raw body, jobId in URL).
- `GET /job/:jobId` — Retrieve the latest enriched snapshot (unwrapped format).

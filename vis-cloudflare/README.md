# gomr-vis-cloudflare

Cloudflare Pages deployment of the gomr pipeline visualization tool. Uses Cloudflare KV for storage and Pages Functions for the API.

## Prerequisites

- Node.js
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) (`npm install -g wrangler`)
- A Cloudflare account (`wrangler login`)

## Setup

1. Create a KV namespace:

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

This builds the client and starts `wrangler pages dev` with a local KV emulation on port 8788.

## Deploy

```bash
npm run deploy
```

This builds the client and deploys to Cloudflare Pages via `wrangler pages deploy`.

After the first deploy, bind the KV namespace to the Pages project in the Cloudflare dashboard:
**Pages project > Settings > Functions > KV namespace bindings** -- bind `GOMR_VIS` to the namespace you created.

## API

Identical to gomr-vis:

- `POST /sink/:jobId` -- push a pipeline metrics snapshot
- `GET /job/:jobId` -- retrieve the latest enriched snapshot

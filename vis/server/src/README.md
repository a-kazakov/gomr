# Pipeline Metrics Server

A Node.js/Express server that serves the React frontend and handles pipeline metrics data ingestion and retrieval.

## Features

- **API Endpoints**: 
  - `POST /push/:jobId` - Save pipeline metrics data
  - `GET /job/:jobId` - Retrieve pipeline metrics data
- **Static File Serving**: Serves the built React app with SPA routing support
- **Storage Abstraction**: Pluggable storage drivers (currently filesystem only)

## Configuration

Environment variables:

- `PORT` - Server port (default: `3000`)
- `STORAGE_URI` - Storage driver URI (default: `fs:./server-data`)
  - Format: `fs:/path/to/directory`
- `CLIENT_DIST_PATH` - Path to React build output (default: `../client/dist` relative to project root)

## Usage

```bash
# Install dependencies (if not already done)
npm install

# Build the React frontend
npm run build

# Start the server
npm run server

# Or with environment variables
PORT=8080 STORAGE_URI=fs:./data npm run server
```

## Storage

The filesystem storage driver saves data as JSON files:
- Location: specified by `STORAGE_URI` (e.g., `fs:./server-data`)
- Format: `{jobId}.json` files
- Data persists across server restarts

## API Examples

### Save metrics
```bash
curl -X POST http://localhost:3000/push/01ABC123 \
  -H "Content-Type: application/json" \
  -d '{"operations": {...}, "collections": {...}}'
```

### Retrieve metrics
```bash
curl http://localhost:3000/job/01ABC123
```

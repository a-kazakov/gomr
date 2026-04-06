import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import { join, resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createStorageDriver, StorageDriver } from './storage.js';
import { enrichWithSpeed } from './speedCalculator.js';
import { ServerResponse } from './types/pipeline.js';

// ES module __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration from environment variables
const PORT = parseInt(process.env.PORT || '3000', 10);
const STORAGE_URI = process.env.STORAGE_URI || 'fs:./server-data';
const PUSH_AUTH_TOKEN = process.env.PUSH_AUTH_TOKEN || '';
const VIEW_BASIC_AUTH = process.env.VIEW_BASIC_AUTH || ''; // "user:password"

// Client dist path: resolve relative to server directory
// This ensures the path is correct regardless of where the process starts
// Path: server/src/server.ts -> server/ -> ../client/dist
const CLIENT_DIST_PATH = process.env.CLIENT_DIST_PATH
  ? resolve(process.cwd(), process.env.CLIENT_DIST_PATH)
  : resolve(__dirname, '../../client/dist');

// Global storage driver instance
let storage: StorageDriver;

/**
 * Error handler middleware
 */
function errorHandler(err: Error, _req: Request, res: Response, _next: NextFunction): void {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error', message: err.message });
}

/**
 * Initialize the server
 */
async function initializeServer(): Promise<void> {
  try {
    // Initialize storage driver
    console.log(`Initializing storage with URI: ${STORAGE_URI}`);
    storage = await createStorageDriver(STORAGE_URI);
    console.log('Storage initialized successfully');

    // Create Express app
    const app = express();

    // Middleware
    app.use(cors()); // Enable CORS for all routes
    app.use(express.json({ limit: '50mb' })); // Parse JSON bodies (up to 50MB)

    // Auth middleware for push endpoint
    function requirePushAuth(req: Request, res: Response, next: NextFunction): void {
      if (!PUSH_AUTH_TOKEN) {
        next();
        return;
      }
      const authHeader = req.headers.authorization;
      if (!authHeader || authHeader !== `Bearer ${PUSH_AUTH_TOKEN}`) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
      }
      next();
    }

    // Basic auth middleware for viewing
    function requireViewAuth(req: Request, res: Response, next: NextFunction): void {
      if (!VIEW_BASIC_AUTH) {
        next();
        return;
      }
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Basic ')) {
        res.set('WWW-Authenticate', 'Basic realm="gomr-vis"');
        res.status(401).json({ error: 'Unauthorized' });
        return;
      }
      const credentials = Buffer.from(authHeader.slice(6), 'base64').toString();
      if (credentials !== VIEW_BASIC_AUTH) {
        res.set('WWW-Authenticate', 'Basic realm="gomr-vis"');
        res.status(401).json({ error: 'Unauthorized' });
        return;
      }
      next();
    }

    // API Routes

    /**
     * POST /push/:jobId
     * Receives pipeline metrics data, enriches it with speed metrics, and saves it to storage.
     */
    app.post('/push/:jobId', requirePushAuth, async (req: Request, res: Response, next: NextFunction) => {
      try {
        const { jobId } = req.params;
        const rawData = req.body as ServerResponse;

        if (!jobId) {
          return res.status(400).json({ error: 'Job ID is required' });
        }

        // Enrich data with speed metrics using 5-second moving window
        const enrichedData = enrichWithSpeed(jobId, rawData);

        // Save enriched data to storage
        await storage.save(jobId, enrichedData);
        res.status(200).json({ success: true, message: `Data saved for job ${jobId}` });
      } catch (error) {
        next(error);
      }
    });

    /**
     * GET /job/:jobId
     * Retrieves pipeline metrics data for a given job ID.
     */
    app.get('/job/:jobId', requireViewAuth, async (req: Request, res: Response, next: NextFunction) => {
      try {
        const { jobId } = req.params;

        if (!jobId) {
          return res.status(400).json({ error: 'Job ID is required' });
        }

        const data = await storage.load(jobId);

        if (data === null) {
          return res.status(404).json({ error: `Job ${jobId} not found` });
        }

        res.status(200).json(data);
      } catch (error) {
        next(error);
      }
    });

    // Static file serving for React app
    app.use(requireViewAuth, express.static(CLIENT_DIST_PATH, {
      // Don't send index.html for API routes
      index: false,
    }));

    // SPA Fallback: Send index.html for any unknown GET request
    // This allows React Router to handle client-side routing
    app.get('*', requireViewAuth, (req: Request, res: Response) => {
      // Only handle GET requests that aren't API routes
      if (req.method === 'GET' && !req.path.startsWith('/push/') && !req.path.startsWith('/job/')) {
        res.sendFile(join(CLIENT_DIST_PATH, 'index.html'));
      } else {
        res.status(404).json({ error: 'Not found' });
      }
    });

    // Error handling middleware (must be last)
    app.use(errorHandler);

    // Start server
    app.listen(PORT, () => {
      console.log(`Server running on http://localhost:${PORT}`);
      console.log(`Serving static files from: ${CLIENT_DIST_PATH}`);
      console.log(`Storage URI: ${STORAGE_URI}`);
    });
  } catch (error) {
    console.error('Failed to initialize server:', error);
    process.exit(1);
  }
}

// Start the server
initializeServer().catch((error) => {
  console.error('Fatal error during initialization:', error);
  process.exit(1);
});

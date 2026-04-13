import { Env } from './types';

export { JobStatus } from './jobStatus';

const corsHeaders: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

function addCorsHeaders(response: Response): Response {
  const newResponse = new Response(response.body, response);
  for (const [key, value] of Object.entries(corsHeaders)) {
    newResponse.headers.set(key, value);
  }
  return newResponse;
}

function sanitizeJobId(jobId: string): string {
  return jobId.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function checkPushAuth(request: Request, env: Env): Response | null {
  const expectedToken = env.PUSH_AUTH_TOKEN;
  if (expectedToken) {
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || authHeader !== `Bearer ${expectedToken}`) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }
  }
  return null;
}

function checkViewAuth(request: Request, env: Env): Response | null {
  const expectedAuth = env.VIEW_BASIC_AUTH;
  if (expectedAuth) {
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Basic ')) {
      return new Response(JSON.stringify({ error: 'Unauthorized' }), {
        status: 401,
        headers: {
          'Content-Type': 'application/json',
          'WWW-Authenticate': 'Basic realm="gomr-vis"',
        },
      });
    }
    const credentials = atob(authHeader.slice(6));
    if (credentials !== expectedAuth) {
      return new Response(JSON.stringify({ error: 'Unauthorized' }), {
        status: 401,
        headers: {
          'Content-Type': 'application/json',
          'WWW-Authenticate': 'Basic realm="gomr-vis"',
        },
      });
    }
  }
  return null;
}

function getDOStub(env: Env, jobId: string): DurableObjectStub {
  const id = env.JOB_STATUS.idFromName(sanitizeJobId(jobId));
  return env.JOB_STATUS.get(id);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // --- New API endpoints ---

      // POST /status — upsert job status via DO
      if (url.pathname === '/status' && request.method === 'POST') {
        const authError = checkPushAuth(request, env);
        if (authError) return addCorsHeaders(authError);

        const body = (await request.json()) as {
          jobId: string;
          status: unknown;
        };
        if (!body.jobId) {
          return addCorsHeaders(
            Response.json({ error: 'jobId is required' }, { status: 400 }),
          );
        }

        const stub = getDOStub(env, body.jobId);
        const doResponse = await stub.fetch(
          new Request(
            `${url.origin}/post?jobId=${encodeURIComponent(body.jobId)}`,
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(body),
            },
          ),
        );

        return addCorsHeaders(doResponse);
      }

      // GET /status/ws?jobId=X — WebSocket upgrade via DO
      // Match by path alone; the DO checks the Upgrade header itself.
      if (url.pathname === '/status/ws') {
        const jobId = url.searchParams.get('jobId');
        if (!jobId) {
          return addCorsHeaders(
            Response.json(
              { error: 'jobId query parameter is required' },
              { status: 400 },
            ),
          );
        }

        const stub = getDOStub(env, jobId);
        // Forward the full original request to preserve WebSocket upgrade semantics
        return stub.fetch(
          new Request(
            `${url.origin}/ws?jobId=${encodeURIComponent(jobId)}`,
            request,
          ),
        );
      }

      // GET /status/:jobId — read status from DO (live) or KV (cold)
      const statusMatch = url.pathname.match(/^\/status\/([^/]+)$/);
      if (statusMatch && request.method === 'GET') {
        const authError = checkViewAuth(request, env);
        if (authError) return addCorsHeaders(authError);

        const jobId = decodeURIComponent(statusMatch[1]);

        // Try live DO first (DO also hydrates from KV internally)
        const stub = getDOStub(env, jobId);
        const doResponse = await stub.fetch(
          new Request(
            `${url.origin}/get?jobId=${encodeURIComponent(jobId)}`,
          ),
        );

        if (doResponse.ok) {
          return addCorsHeaders(doResponse);
        }

        // Fall back to KV directly
        if (env.GOMR_VIS) {
          const kvData = await env.GOMR_VIS.get(
            `job:${sanitizeJobId(jobId)}`,
            'json',
          );
          if (kvData) {
            return addCorsHeaders(
              Response.json({ jobId, status: kvData, source: 'kv' }),
            );
          }
        }

        return addCorsHeaders(
          Response.json({ error: `Job ${jobId} not found` }, { status: 404 }),
        );
      }

      // --- Original endpoints ---

      // POST /push/:jobId — upsert (jobId in URL, raw body)
      const pushMatch = url.pathname.match(/^\/push\/([^/]+)$/);
      if (pushMatch && request.method === 'POST') {
        const authError = checkPushAuth(request, env);
        if (authError) return addCorsHeaders(authError);

        const jobId = decodeURIComponent(pushMatch[1]);
        if (!jobId) {
          return addCorsHeaders(
            Response.json({ error: 'Job ID is required' }, { status: 400 }),
          );
        }

        const rawBody = await request.json();

        const stub = getDOStub(env, jobId);
        await stub.fetch(
          new Request(
            `${url.origin}/post?jobId=${encodeURIComponent(jobId)}`,
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ jobId, status: rawBody }),
            },
          ),
        );

        return addCorsHeaders(
          Response.json({
            success: true,
            message: `Data saved for job ${jobId}`,
          }),
        );
      }

      // GET /job/:jobId — read (unwrapped format)
      const jobMatch = url.pathname.match(/^\/job\/([^/]+)$/);
      if (jobMatch && request.method === 'GET') {
        const authError = checkViewAuth(request, env);
        if (authError) return addCorsHeaders(authError);

        const jobId = decodeURIComponent(jobMatch[1]);

        // Try live DO
        const stub = getDOStub(env, jobId);
        const doResponse = await stub.fetch(
          new Request(
            `${url.origin}/get?jobId=${encodeURIComponent(jobId)}`,
          ),
        );

        if (doResponse.ok) {
          // Return unwrapped status (original format)
          const data = (await doResponse.json()) as { status: unknown };
          return addCorsHeaders(Response.json(data.status));
        }

        // Fall back to KV
        if (env.GOMR_VIS) {
          const kvData = await env.GOMR_VIS.get(
            `job:${sanitizeJobId(jobId)}`,
            'json',
          );
          if (kvData) {
            return addCorsHeaders(Response.json(kvData));
          }
        }

        return addCorsHeaders(
          Response.json({ error: `Job ${jobId} not found` }, { status: 404 }),
        );
      }

      // --- Static assets (Pages Advanced Mode provides env.ASSETS) ---
      if (env.ASSETS) {
        const assetResponse = await env.ASSETS.fetch(request);
        if (assetResponse.status !== 404) {
          return assetResponse;
        }

        // SPA fallback: serve index.html for unmatched routes
        return env.ASSETS.fetch(new Request(`${url.origin}/index.html`));
      }

      return new Response('Not Found', { status: 404 });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : 'Unknown error';
      return addCorsHeaders(
        Response.json(
          { error: 'Internal server error', message },
          { status: 500 },
        ),
      );
    }
  },
};

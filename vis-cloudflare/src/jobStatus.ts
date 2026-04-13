import { DurableObject } from 'cloudflare:workers';
import { Env, ServerResponse } from './types';
import { HistoryEntry, enrichWithSpeed, pruneHistory } from './speedCalculator';

const DEFAULT_SELF_DESTRUCT_MS = 365 * 24 * 60 * 60 * 1000; // 1 year
const DEFAULT_IDLE_QUIET_MS = 3600 * 1000; // 1 hour
const DEFAULT_HOUSEKEEP_MS = 24 * 60 * 60 * 1000; // 24 hours

export class JobStatus extends DurableObject<Env> {
  // In-memory speed calculation history. Lost on hibernation, which is fine —
  // the calculator gracefully returns zero speeds until enough data accumulates.
  private history: HistoryEntry[] = [];

  private get selfDestructMs(): number {
    return parseInt(this.env.SELF_DESTRUCT_MS || '') || DEFAULT_SELF_DESTRUCT_MS;
  }

  private get idleQuietMs(): number {
    return parseInt(this.env.IDLE_QUIET_MS || '') || DEFAULT_IDLE_QUIET_MS;
  }

  private get housekeepMs(): number {
    return parseInt(this.env.HOUSEKEEP_MS || '') || DEFAULT_HOUSEKEEP_MS;
  }

  async fetch(request: Request): Promise<Response> {
    if (request.method === 'POST') {
      return this.handleStatusPost(request);
    }

    if (request.headers.get('Upgrade')?.toLowerCase() === 'websocket') {
      return this.handleWebSocket(request);
    }

    return this.handleGetStatus(request);
  }

  private async handleStatusPost(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      jobId: string;
      status: ServerResponse;
    };
    const url = new URL(request.url);
    const jobId = body.jobId || url.searchParams.get('jobId') || '';
    const rawStatus = body.status;

    // Enrich with speed calculations
    this.history = pruneHistory(this.history, Date.now());
    const enrichedStatus = enrichWithSpeed(this.history, rawStatus);
    this.history.push({ timestamp: Date.now(), data: rawStatus });

    const now = Date.now();

    // Persist current status
    await this.ctx.storage.put({
      status: JSON.stringify(enrichedStatus),
      updatedAt: now,
      jobId,
    });

    // Broadcast to all connected WebSocket clients
    const message = JSON.stringify({
      type: 'status',
      jobId,
      status: enrichedStatus,
      updatedAt: now,
    });

    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(message);
      } catch {
        // Closed sockets are cleaned up by the runtime
      }
    }

    // Reset housekeeping alarm
    await this.ctx.storage.setAlarm(now + this.housekeepMs);

    return new Response(null, { status: 204 });
  }

  /**
   * If the DO has no stored status, check KV. On hit, persist into DO
   * storage so subsequent reads are fast and the alarm lifecycle kicks in.
   */
  private async hydrateFromKV(
    jobId: string,
  ): Promise<{ status: string; updatedAt: number } | null> {
    if (!jobId || !this.env.GOMR_VIS) return null;

    const sanitized = jobId.replace(/[^a-zA-Z0-9_-]/g, '_');
    const kvData = await this.env.GOMR_VIS.get(`job:${sanitized}`, 'text');
    if (!kvData) return null;

    const updatedAt = Date.now();
    await this.ctx.storage.put({ status: kvData, updatedAt, jobId });
    await this.ctx.storage.setAlarm(updatedAt + this.housekeepMs);
    return { status: kvData, updatedAt };
  }

  private async handleWebSocket(request: Request): Promise<Response> {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);

    const url = new URL(request.url);
    const reqJobId = url.searchParams.get('jobId') || '';

    // Send current status immediately so the client renders without waiting
    const data = await this.ctx.storage.get(['status', 'updatedAt', 'jobId']);
    let status = data.get('status') as string | undefined;
    let updatedAt = data.get('updatedAt') as number | undefined;
    const jobId = (data.get('jobId') as string) || reqJobId;

    // Hydrate from KV if DO has no stored status
    if (!status) {
      const hydrated = await this.hydrateFromKV(jobId);
      if (hydrated) {
        status = hydrated.status;
        updatedAt = hydrated.updatedAt;
      }
    }

    if (status) {
      pair[1].send(
        JSON.stringify({
          type: 'status',
          jobId,
          status: JSON.parse(status),
          updatedAt: updatedAt || 0,
        }),
      );
    }

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  private async handleGetStatus(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const reqJobId = url.searchParams.get('jobId') || '';

    const data = await this.ctx.storage.get(['status', 'updatedAt', 'jobId']);
    let status = data.get('status') as string | undefined;
    let updatedAt = data.get('updatedAt') as number | undefined;
    const jobId = (data.get('jobId') as string) || reqJobId;

    // Hydrate from KV if DO has no stored status
    if (!status) {
      const hydrated = await this.hydrateFromKV(jobId);
      if (hydrated) {
        status = hydrated.status;
        updatedAt = hydrated.updatedAt;
      }
    }

    if (status) {
      return Response.json({
        jobId,
        status: JSON.parse(status),
        updatedAt: updatedAt || 0,
        source: 'live',
      });
    }

    return new Response(null, { status: 404 });
  }

  // --- Hibernation API hooks ---

  async webSocketClose(
    _ws: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    // Runtime automatically removes closed sockets from getWebSockets()
  }

  async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    ws.close(1011, 'unexpected error');
  }

  async webSocketMessage(
    ws: WebSocket,
    _message: string | ArrayBuffer,
  ): Promise<void> {
    // Clients are read-only
    ws.send(JSON.stringify({ type: 'error', message: 'read-only' }));
  }

  // --- Lifecycle management via Alarm ---

  async alarm(): Promise<void> {
    const updatedAt =
      ((await this.ctx.storage.get('updatedAt')) as number) || 0;
    const age = Date.now() - updatedAt;

    // 1. Hard TTL: destroy after configured lifetime no matter what
    if (age >= this.selfDestructMs) {
      await this.archiveToKV();
      await this.notifyAndClose('expired');
      await this.ctx.storage.deleteAll();
      return;
    }

    // 2. Soft cleanup: no clients watching and no updates for IDLE_QUIET_MS
    const clients = this.ctx.getWebSockets();
    if (clients.length === 0 && age >= this.idleQuietMs) {
      await this.archiveToKV();
      await this.notifyAndClose('idle_timeout');
      await this.ctx.storage.deleteAll();
      return;
    }

    // 3. Otherwise, schedule next housekeeping check
    const nextCheck = Math.min(
      updatedAt + this.selfDestructMs, // don't overshoot the hard TTL
      Date.now() + this.housekeepMs,
    );
    await this.ctx.storage.setAlarm(nextCheck);
  }

  private async archiveToKV(): Promise<void> {
    const data = await this.ctx.storage.get(['status', 'updatedAt', 'jobId']);
    const status = data.get('status') as string | undefined;
    const jobId = data.get('jobId') as string | undefined;

    if (status && jobId) {
      const sanitized = jobId.replace(/[^a-zA-Z0-9_-]/g, '_');
      await this.env.GOMR_VIS.put(`job:${sanitized}`, status);
    }
  }

  private async notifyAndClose(reason: string): Promise<void> {
    const message = JSON.stringify({ type: 'closed', reason });
    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(message);
        ws.close(1000, reason);
      } catch {
        // Already closed
      }
    }
  }
}

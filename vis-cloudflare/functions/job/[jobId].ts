import { Env } from '../types';

export const onRequestGet: PagesFunction<Env, 'jobId'> = async (context) => {
  const expectedAuth = context.env.VIEW_BASIC_AUTH;
  if (expectedAuth) {
    const authHeader = context.request.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Basic ')) {
      return new Response(JSON.stringify({ error: 'Unauthorized' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', 'WWW-Authenticate': 'Basic realm="gomr-vis"' },
      });
    }
    const credentials = atob(authHeader.slice(6));
    if (credentials !== expectedAuth) {
      return new Response(JSON.stringify({ error: 'Unauthorized' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json', 'WWW-Authenticate': 'Basic realm="gomr-vis"' },
      });
    }
  }

  const jobId = context.params.jobId as string;

  if (!jobId) {
    return Response.json({ error: 'Job ID is required' }, { status: 400 });
  }

  try {
    const data = await context.env.GOMR_VIS.get(
      `job:${sanitizeJobId(jobId)}`,
      'json'
    );

    if (data === null) {
      return Response.json({ error: `Job ${jobId} not found` }, { status: 404 });
    }

    return Response.json(data);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error';
    return Response.json({ error: 'Internal server error', message }, { status: 500 });
  }
};

function sanitizeJobId(jobId: string): string {
  return jobId.replace(/[^a-zA-Z0-9_-]/g, '_');
}

import { Env, ServerResponse } from '../types';
import { enrichWithSpeed } from '../speedCalculator';

function sanitizeJobId(jobId: string): string {
  return jobId.replace(/[^a-zA-Z0-9_-]/g, '_');
}

export const onRequestPost: PagesFunction<Env, 'jobId'> = async (context) => {
  const expectedToken = context.env.PUSH_AUTH_TOKEN;
  if (expectedToken) {
    const authHeader = context.request.headers.get('Authorization');
    if (!authHeader || authHeader !== `Bearer ${expectedToken}`) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }
  }

  const jobId = context.params.jobId as string;

  if (!jobId) {
    return Response.json({ error: 'Job ID is required' }, { status: 400 });
  }

  try {
    const rawData = await context.request.json() as ServerResponse;
    const enrichedData = enrichWithSpeed(jobId, rawData);

    await context.env.GOMR_VIS.put(
      `job:${sanitizeJobId(jobId)}`,
      JSON.stringify(enrichedData)
    );

    return Response.json({ success: true, message: `Data saved for job ${jobId}` });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error';
    return Response.json({ error: 'Internal server error', message }, { status: 500 });
  }
};

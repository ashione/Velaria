import { useCallback } from 'react';
import type { ServiceInfo } from '../types';

/**
 * useService exposes the `api` helper and bootstrap logic used by App-level
 * code and view components.  The hook is intentionally thin: it wraps raw
 * fetch with the service base URL and standard JSON headers.
 */
export function useService(serviceInfo: ServiceInfo | null) {
  const api = useCallback(
    async (path: string, options: RequestInit = {}) => {
      if (!serviceInfo) throw new Error('service unavailable');
      const response = await fetch(`${serviceInfo.baseUrl}${path}`, {
        headers: {
          'Content-Type': 'application/json',
          ...(options.headers || {}),
        },
        ...options,
      });
      const rawText = await response.text();
      const isJson = (response.headers.get('content-type') || '').includes('application/json');
      let payload: any = {};
      if (rawText) {
        if (isJson) {
          try {
            payload = JSON.parse(rawText);
          } catch {
            payload = { raw: rawText };
          }
        } else {
          payload = { raw: rawText };
        }
      }
      if (!response.ok || payload?.ok === false) {
        const errorMessage =
          (typeof payload?.error === 'string' && payload.error) ||
          (typeof payload?.message === 'string' && payload.message) ||
          (typeof payload?.raw === 'string' && payload.raw.trim()) ||
          `request failed: ${response.status}`;
        throw new Error(errorMessage);
      }
      return payload;
    },
    [serviceInfo],
  );

  const waitForRunCompletion = useCallback(
    async (runId: string, timeoutMs = 120_000) => {
      const startedAt = Date.now();
      while (Date.now() - startedAt < timeoutMs) {
        const runPayload = await api(`/api/v1/runs/${encodeURIComponent(runId)}`);
        const run =
          runPayload.run && typeof runPayload.run === 'object' && !Array.isArray(runPayload.run)
            ? (runPayload.run as Record<string, unknown>)
            : null;
        const status = typeof run?.status === 'string' ? run.status : '';
        if (status === 'succeeded') {
          return api(`/api/v1/runs/${encodeURIComponent(runId)}/result?limit=50`);
        }
        if (status === 'failed' || status === 'cancelled') {
          throw new Error(typeof run?.error === 'string' && run.error ? run.error : `run ${runId} failed`);
        }
        await new Promise((resolve) => window.setTimeout(resolve, 1000));
      }
      throw new Error(`run ${runId} timed out`);
    },
    [api],
  );

  return { api, waitForRunCompletion };
}

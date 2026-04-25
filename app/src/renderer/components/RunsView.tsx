import { useEffect, useMemo, useState } from 'react';
import type {
  DatasetRecord,
  RunDetailPayload,
  RunSummary,
  TFunction,
  ViewKey,
} from '../types';
import {
  asRecord,
  decodeFileUri,
  isDatasetBackedByRun,
  isTerminalRunStatus,
  renderPreviewTable,
} from '../types';

export type RunsViewProps = {
  t: TFunction;
  api: (path: string, options?: RequestInit) => Promise<any>;
  runs: RunSummary[];
  runsPage: number;
  setRunsPage: React.Dispatch<React.SetStateAction<number>>;
  selectedRunId: string | null;
  setSelectedRunId: (id: string | null) => void;
  datasets: DatasetRecord[];
  setDatasets: React.Dispatch<React.SetStateAction<DatasetRecord[]>>;
  refreshRuns: (id?: string | null) => void;
  saveRunDetailAsDataset: (detail: RunDetailPayload, nextView: ViewKey) => void;
};

export function RunsView(props: RunsViewProps) {
  const {
    t, api,
    runs, runsPage, setRunsPage,
    selectedRunId, setSelectedRunId,
    datasets, setDatasets,
    refreshRuns, saveRunDetailAsDataset,
  } = props;

  const [selectedRunDetail, setSelectedRunDetail] = useState<RunDetailPayload | null>(null);
  const [runMessage, setRunMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);

  const pagedRuns = useMemo(() => {
    const start = (runsPage - 1) * 8;
    return runs.slice(start, start + 8);
  }, [runs, runsPage]);

  async function loadRunDetail(runId: string) {
    try {
      const runPayload = await api(`/api/v1/runs/${encodeURIComponent(runId)}`);
      const runRecord = asRecord(runPayload.run);
      const runStatus = typeof runRecord?.status === 'string' ? runRecord.status : '';
      if (!isTerminalRunStatus(runStatus)) {
        setSelectedRunDetail({
          run: ((runPayload.run as RunSummary & Record<string, unknown>) || { run_id: runId, status: runStatus || 'running', action: typeof runRecord?.action === 'string' ? runRecord.action : 'run' }),
          artifact: null,
          preview: { schema: [], rows: [], row_count: 0 },
        });
        setRunMessage({ kind: 'info', text: t('run_detail_running') });
        return;
      }
      const payload = (await api(`/api/v1/runs/${encodeURIComponent(runId)}/result?limit=20`)) as RunDetailPayload;
      setSelectedRunDetail(payload);
      setRunMessage(null);
    } catch (error) {
      setSelectedRunDetail(null);
      setRunMessage({ kind: 'error', text: t('run_detail_failed', { error: String(error) }) });
    }
  }

  useEffect(() => {
    if (selectedRunId) void loadRunDetail(selectedRunId);
  }, [selectedRunId]);

  useEffect(() => {
    if (!selectedRunId) return;
    if (selectedRunDetail?.run?.run_id !== selectedRunId) return;
    if (isTerminalRunStatus(selectedRunDetail.run.status)) return;
    const timer = window.setTimeout(() => {
      refreshRuns(selectedRunId);
      void loadRunDetail(selectedRunId);
    }, 1000);
    return () => window.clearTimeout(timer);
  }, [selectedRunDetail, selectedRunId]);

  async function deleteRun(runId: string) {
    const target = runs.find((r) => r.run_id === runId);
    if (!window.confirm(t('confirm_delete_run', { name: target?.run_name || runId }))) return;
    try {
      const rp = await api(`/api/v1/runs/${encodeURIComponent(runId)}`);
      const runDir = String(rp.run?.run_dir || '');
      const removedCount = runDir ? datasets.filter((d) => isDatasetBackedByRun(d, runDir)).length : 0;
      await api(`/api/v1/runs/${encodeURIComponent(runId)}`, { method: 'DELETE' });
      if (runDir) setDatasets((c) => c.filter((d) => !isDatasetBackedByRun(d, runDir)));
      refreshRuns(selectedRunId === runId ? null : selectedRunId);
      if (selectedRunId === runId) setSelectedRunDetail(null);
      setRunMessage({ kind: 'info', text: t('run_deleted_message', { runId, count: removedCount }) });
    } catch (error) {
      setRunMessage({ kind: 'error', text: t('run_delete_failed', { error: String(error) }) });
    }
  }

  async function exportCurrentRunArtifact() {
    if (!selectedRunDetail?.artifact?.uri) return;
    await window.velariaShell.exportFile({ sourcePath: decodeFileUri(selectedRunDetail.artifact.uri) });
  }

  return (
    <section className="section active">
      <section className="panel">
        <div className="panel-head">
          <h2>{t('run_history')}</h2>
          <div className="actions"><button className="ghost" onClick={() => refreshRuns()}>{t('refresh')}</button></div>
        </div>
        <div className="panel-body stack">
          {runMessage && <div className={`notice ${runMessage.kind === 'error' ? 'error' : ''}`}>{runMessage.text}</div>}
          <div className="list">
            {pagedRuns.map((run) => {
              const expanded = run.run_id === selectedRunId;
              return (
                <div key={run.run_id} className={`list-item ${expanded ? 'active' : ''}`}>
                  <div onClick={() => setSelectedRunId(expanded ? null : run.run_id)} style={{ cursor: 'pointer' }}>
                    <div className="item-head">
                      <h4>{run.run_name || run.run_id}</h4>
                      <button type="button" className="ghost danger-button" onClick={(e) => { e.stopPropagation(); void deleteRun(run.run_id); }}>{t('delete_run')}</button>
                    </div>
                    <div className="meta"><span>{run.status}</span><span>{run.action}</span><span>{`${run.artifact_count ?? 0} ${t('label_artifacts')}`}</span></div>
                  </div>
                  {expanded && selectedRunDetail && selectedRunDetail.run.run_id === run.run_id && (
                    <div className="result-box" style={{ marginTop: 14 }}>
                      <div className="meta"><span>{t('rows_count', { count: selectedRunDetail.preview.row_count ?? '\u2014' })}</span></div>
                      {!isTerminalRunStatus(String(selectedRunDetail.run.status)) && (
                        <div className="notice"><div>{t('run_detail_running')}</div><div>{t('run_detail_running_rows', { count: typeof asRecord(selectedRunDetail.run.details)?.fetched_rows === 'number' ? Number(asRecord(selectedRunDetail.run.details)?.fetched_rows) : '\u2014' })}</div></div>
                      )}
                      <div className="mono"><strong>{t('field_run_id')}:</strong> {String(selectedRunDetail.run.run_id)}</div>
                      {selectedRunDetail.artifact ? (
                        <>
                          <div className="mono"><strong>{t('field_artifact_id')}:</strong> {selectedRunDetail.artifact.artifact_id}</div>
                          <div className="mono"><strong>{t('field_artifact_uri')}:</strong> {selectedRunDetail.artifact.uri}</div>
                          <div className="actions">
                            <button className="ghost" onClick={() => void exportCurrentRunArtifact()}>{t('export_file')}</button>
                            <button className="ghost" onClick={() => saveRunDetailAsDataset(selectedRunDetail, 'data')}>{t('save_result_action')}</button>
                            <button className="ghost" onClick={() => saveRunDetailAsDataset(selectedRunDetail, 'analyze')}>{t('analyze_action')}</button>
                            <button className="ghost danger-button" onClick={() => void deleteRun(run.run_id)}>{t('delete_run')}</button>
                          </div>
                          <div dangerouslySetInnerHTML={{ __html: renderPreviewTable(selectedRunDetail.preview, t('no_preview_rows')) }} />
                        </>
                      ) : (
                        <div className="actions"><button className="ghost danger-button" onClick={() => void deleteRun(run.run_id)}>{t('delete_run')}</button></div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
            {!runs.length && <div className="empty">{t('no_runs_yet')}</div>}
          </div>
          <div className="actions" style={{ marginTop: 14, justifyContent: 'space-between', alignItems: 'center' }}>
            <div className="helper">{t('page_status', { page: runs.length ? runsPage : 0, total: Math.max(1, Math.ceil(runs.length / 8)) })}</div>
            <div className="actions">
              <button className="ghost" disabled={runsPage <= 1} onClick={() => setRunsPage((p) => Math.max(1, p - 1))}>{t('page_prev')}</button>
              <button className="ghost" disabled={runsPage >= Math.max(1, Math.ceil(runs.length / 8))} onClick={() => setRunsPage((p) => Math.min(Math.max(1, Math.ceil(runs.length / 8)), p + 1))}>{t('page_next')}</button>
            </div>
          </div>
        </div>
      </section>
    </section>
  );
}

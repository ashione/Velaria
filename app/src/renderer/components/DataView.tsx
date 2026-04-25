import { useState } from 'react';
import type {
  AppConfig,
  DatasetRecord,
  EmbeddingBuildState,
  ImportFormState,
  ImportOptions,
  ImportPreviewPayload,
  TFunction,
  ViewKey,
} from '../types';
import {
  EMBEDDING_MODEL_OPTIONS,
  EMBEDDING_TEMPLATE_OPTIONS,
  buildSavedDatasetName,
  createDatasetRecord,
  defaultImportForm,
  embeddingFormToConfig,
  extractEmbeddingDatasetFromRunPayload,
  extractKeywordIndexFromRunPayload,
  importPreviewFromRunPayload,
  keywordFormToConfig,
  renderPreviewTable,
} from '../types';

export type DataViewProps = {
  t: TFunction;
  api: (path: string, options?: RequestInit) => Promise<any>;
  waitForRunCompletion: (runId: string, timeoutMs?: number) => Promise<any>;
  datasets: DatasetRecord[];
  setDatasets: React.Dispatch<React.SetStateAction<DatasetRecord[]>>;
  selectedDatasetId: string | null;
  setSelectedDatasetId: (id: string | null) => void;
  currentDataset: DatasetRecord | null;
  configForm: AppConfig;
  embeddingBuilds: Record<string, EmbeddingBuildState>;
  datasetMessage: { kind: 'info' | 'error'; text: string } | null;
  setDatasetMessage: (msg: { kind: 'info' | 'error'; text: string } | null) => void;
  removeDataset: (datasetId: string) => void;
  exportCurrentDataset: () => void;
  buildEmbeddingDataset: (datasetId: string, snap?: DatasetRecord) => void;
  buildKeywordIndexDataset: (datasetId: string, snap?: DatasetRecord) => void;
  setView: (view: ViewKey) => void;
  getEmbeddingStatusLabel: (dataset: DatasetRecord) => string;
  getEmbeddingUiStatus: (dataset: DatasetRecord | null) => 'disabled' | 'configured' | 'building' | 'failed' | 'ready';
  getKeywordUiStatus: (dataset: DatasetRecord | null) => 'disabled' | 'configured' | 'building' | 'failed' | 'ready';
  refreshRuns: (runId?: string | null) => void;
};

export function DataView(props: DataViewProps) {
  const {
    t, api, waitForRunCompletion,
    datasets, setDatasets, selectedDatasetId, setSelectedDatasetId, currentDataset,
    configForm, embeddingBuilds, datasetMessage, setDatasetMessage,
    removeDataset, exportCurrentDataset, buildEmbeddingDataset, buildKeywordIndexDataset,
    setView, getEmbeddingStatusLabel, getEmbeddingUiStatus, getKeywordUiStatus, refreshRuns,
  } = props;

  const [importForm, setImportForm] = useState<ImportFormState>({
    ...defaultImportForm,
    bitableAppId: configForm.bitableAppId,
    bitableAppSecret: configForm.bitableAppSecret,
  });
  const [pendingImport, setPendingImport] = useState<ImportPreviewPayload | null>(null);
  const [importMessage, setImportMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [importRunId, setImportRunId] = useState<string | null>(null);
  const [savingDataset, setSavingDataset] = useState(false);

  const currentEmbeddingStatus = getEmbeddingUiStatus(currentDataset);
  const currentEmbeddingBuild = currentDataset ? embeddingBuilds[currentDataset.datasetId] : null;
  const currentKeywordStatus = getKeywordUiStatus(currentDataset);
  const currentKeywordBuild = currentDataset ? embeddingBuilds[`${currentDataset.datasetId}::keyword`] : null;
  const datasetKindLabel = currentDataset ? t(currentDataset.kind === 'result' ? 'kind_result' : 'kind_imported') : '\u2014';
  const currentEmbeddingSummary = currentDataset?.embeddingDataset?.datasetPath
    ? `${currentDataset.embeddingConfig.textColumns.join(', ') || '\u2014'} \u00b7 ${currentDataset.embeddingConfig.provider || '\u2014'} \u00b7 ${currentDataset.embeddingConfig.model || '\u2014'}`
    : currentEmbeddingStatus === 'building'
      ? `${t('embedding_building_short')} \u00b7 ${currentDataset?.embeddingConfig.textColumns.join(', ') || '\u2014'}`
      : currentEmbeddingStatus === 'failed'
        ? `${t('embedding_build_failed_short')} \u00b7 ${currentDataset?.embeddingConfig.textColumns.join(', ') || '\u2014'}`
        : currentDataset?.embeddingConfig.enabled
          ? `${t('embedding_configured_only')} \u00b7 ${currentDataset.embeddingConfig.textColumns.join(', ') || '\u2014'}`
          : t('embedding_disabled');
  const currentKeywordSummary = currentDataset?.keywordIndex?.indexPath
    ? `${currentDataset.keywordConfig.textColumns.join(', ') || '\u2014'} \u00b7 ${currentDataset.keywordConfig.analyzer || 'builtin'}`
    : currentKeywordStatus === 'building'
      ? `${t('keyword_building_short')} \u00b7 ${currentDataset?.keywordConfig.textColumns.join(', ') || '\u2014'}`
      : currentKeywordStatus === 'failed'
        ? `${t('keyword_build_failed_short')} \u00b7 ${currentDataset?.keywordConfig.textColumns.join(', ') || '\u2014'}`
        : currentDataset?.keywordConfig.enabled
          ? `${t('keyword_configured_only')} \u00b7 ${currentDataset.keywordConfig.textColumns.join(', ') || '\u2014'}`
          : t('keyword_disabled');
  const showBuildEmbeddingAction = currentDataset != null && (currentEmbeddingStatus === 'configured' || currentEmbeddingStatus === 'failed');
  const isBuildingEmbedding = currentEmbeddingStatus === 'building';
  const showBuildKeywordAction = currentDataset != null && (currentKeywordStatus === 'configured' || currentKeywordStatus === 'failed');
  const isBuildingKeyword = currentKeywordStatus === 'building';

  async function previewImport(event: React.FormEvent) {
    event.preventDefault();
    setImportMessage({ kind: 'info', text: t('loading_preview') });
    setDatasetMessage(null);
    setPendingImport(null);
    try {
      if (importForm.inputType === 'bitable') {
        const payload: Record<string, unknown> = {
          input_type: 'bitable', bitable_url: importForm.inputPath.trim(),
          app_id: importForm.bitableAppId.trim() || configForm.bitableAppId.trim() || undefined,
          app_secret: importForm.bitableAppSecret.trim() || configForm.bitableAppSecret.trim() || undefined,
          dataset_name: importForm.datasetName.trim(), limit: 100,
        };
        const result = (await api('/api/v1/import/preview', { method: 'POST', body: JSON.stringify(payload) })) as ImportPreviewPayload;
        setPendingImport(result); setImportMessage(null); return;
      }
      const payload: Record<string, unknown> = {
        input_path: importForm.inputPath.trim(), input_type: importForm.inputType,
        delimiter: importForm.delimiter, dataset_name: importForm.datasetName.trim(),
      };
      const columns = importForm.columns.trim();
      const regexPattern = importForm.regexPattern.trim();
      if (importForm.inputType === 'json' && columns) payload.columns = columns;
      if (importForm.inputType === 'line') {
        payload.mappings = columns; payload.regex_pattern = regexPattern;
        payload.line_mode = regexPattern ? 'regex' : 'split';
      }
      if (importForm.embeddingEnabled) payload.embedding_config = embeddingFormToConfig(importForm);
      if (importForm.keywordEnabled) payload.keyword_index_config = keywordFormToConfig(importForm);
      const result = (await api('/api/v1/import/preview', { method: 'POST', body: JSON.stringify(payload) })) as ImportPreviewPayload;
      setPendingImport(result); setImportMessage(null);
    } catch (error) {
      setImportRunId(null); setPendingImport(null);
      setImportMessage({ kind: 'error', text: t('preview_failed', { error: String(error) }) });
    }
  }

  async function saveImportDataset() {
    if (!pendingImport) return;
    const embeddingConfig = embeddingFormToConfig(importForm);
    const keywordConfig = keywordFormToConfig(importForm);
    let handedOff = false;
    const importOptions: ImportOptions = {
      delimiter: importForm.delimiter,
      columns: importForm.inputType === 'json' ? importForm.columns.trim() : '',
      mappings: importForm.inputType === 'line' ? importForm.columns.trim() : '',
      regexPattern: importForm.regexPattern.trim(),
      lineMode: importForm.regexPattern.trim() ? 'regex' : 'split',
      jsonFormat: importForm.jsonFormat,
    };
    setSavingDataset(true); setDatasetMessage(null);
    setImportMessage({ kind: 'info', text: embeddingConfig.enabled || keywordConfig.enabled ? t('dataset_building_background') : t('dataset_saving') });
    try {
      if (pendingImport.dataset.source_type === 'bitable') {
        const datasetName = buildSavedDatasetName(pendingImport.dataset);
        const payload: Record<string, unknown> = {
          bitable_url: pendingImport.dataset.source_path || pendingImport.dataset.source_label || importForm.inputPath.trim(),
          app_id: importForm.bitableAppId.trim() || configForm.bitableAppId.trim() || undefined,
          app_secret: importForm.bitableAppSecret.trim() || configForm.bitableAppSecret.trim() || undefined,
          dataset_name: datasetName, run_name: `bitable-import-${new Date().toISOString()}`, description: 'Desktop workbench bitable import',
        };
        if (embeddingConfig.enabled) payload.embedding_config = { enabled: true, text_columns: embeddingConfig.textColumns, provider: embeddingConfig.provider || undefined, model: embeddingConfig.model || undefined, template_version: embeddingConfig.templateVersion || undefined, vector_column: embeddingConfig.vectorColumn || 'embedding' };
        if (keywordConfig.enabled) payload.keyword_index_config = { enabled: true, text_columns: keywordConfig.textColumns, analyzer: keywordConfig.analyzer || 'jieba' };
        const started = await api('/api/v1/runs/bitable-import', { method: 'POST', body: JSON.stringify(payload) });
        const runId = typeof started?.run_id === 'string' ? started.run_id : '';
        if (!runId) throw new Error('bitable import response missing run_id');
        setImportRunId(runId); setPendingImport(null);
        setImportMessage({ kind: 'info', text: t('bitable_import_started') });
        refreshRuns(runId); handedOff = true;
        void (async () => {
          try {
            const runResult = await waitForRunCompletion(runId);
            const completed = importPreviewFromRunPayload(runResult);
            const embeddingDataset = extractEmbeddingDatasetFromRunPayload(runResult);
            const keywordIndex = extractKeywordIndexFromRunPayload(runResult);
            const record = createDatasetRecord({ name: completed.dataset.name, sourceType: completed.dataset.source_type, sourcePath: completed.dataset.source_path, sourceLabel: completed.dataset.source_label, preview: completed.preview, kind: 'imported', importOptions, embeddingConfig, embeddingDataset, keywordConfig, keywordIndex });
            setDatasets((c) => [record, ...c]); setSelectedDatasetId(record.datasetId);
            setDatasetMessage({ kind: 'info', text: embeddingConfig.enabled ? t('dataset_saved_with_embedding_message') : keywordConfig.enabled ? t('dataset_saved_with_keyword_message') : t('dataset_saved_message') });
            setView('analyze');
            if (embeddingConfig.enabled && !embeddingDataset?.datasetPath) buildEmbeddingDataset(record.datasetId, record);
            if (keywordConfig.enabled && !keywordIndex?.indexPath) buildKeywordIndexDataset(record.datasetId, record);
            refreshRuns(runId);
          } catch (error) { setImportMessage({ kind: 'error', text: t('preview_failed', { error: String(error) }) }); }
          finally { setImportRunId(null); setSavingDataset(false); }
        })();
        return;
      }
      const record = createDatasetRecord({ name: buildSavedDatasetName(pendingImport.dataset), sourceType: pendingImport.dataset.source_type, sourcePath: pendingImport.dataset.source_path, sourceLabel: pendingImport.dataset.source_label, preview: pendingImport.preview, kind: 'imported', importOptions, embeddingConfig, embeddingDataset: null, keywordConfig, keywordIndex: null });
      setDatasets((c) => [record, ...c]); setSelectedDatasetId(record.datasetId);
      setPendingImport(null); setImportMessage(null);
      setDatasetMessage({ kind: 'info', text: embeddingConfig.enabled ? t('dataset_saved_with_embedding_message') : keywordConfig.enabled ? t('dataset_saved_with_keyword_message') : t('dataset_saved_message') });
      setView('analyze');
      if (embeddingConfig.enabled) buildEmbeddingDataset(record.datasetId, record);
      if (keywordConfig.enabled) buildKeywordIndexDataset(record.datasetId, record);
    } catch (error) {
      setImportMessage({ kind: 'error', text: embeddingFormToConfig(importForm).enabled ? t('embedding_build_failed', { error: String(error) }) : t('dataset_save_failed', { error: String(error) }) });
    } finally { if (!handedOff) setSavingDataset(false); }
  }

  return (
    <section className="section active">
      <div className="split">
        <section className="panel">
          <div className="panel-head"><h2>{t('import_wizard')}</h2></div>
          <div className="panel-body">
            <form onSubmit={previewImport}>
              <label>
                <span>{importForm.inputType === 'bitable' ? t('bitable_url') : t('input_path')}</span>
                <input value={importForm.inputPath} onChange={(e) => setImportForm((c) => ({ ...c, inputPath: e.target.value }))} placeholder={importForm.inputType === 'bitable' ? t('bitable_url_placeholder') : t('input_placeholder')} required />
              </label>
              <div className="actions">
                {importForm.inputType !== 'bitable' && (
                  <button type="button" className="ghost" onClick={async () => { const path = await window.velariaShell.pickFile(); if (path) setImportForm((c) => ({ ...c, inputPath: path })); }}>{t('choose_file')}</button>
                )}
                <button type="submit" disabled={importRunId !== null}>{t('preview_import')}</button>
              </div>
              <div className="field-grid">
                <label>
                  <span>{t('input_type')}</span>
                  <select value={importForm.inputType} onChange={(e) => setImportForm((c) => ({ ...c, inputType: e.target.value }))}>
                    <option value="auto">auto</option><option value="csv">csv</option><option value="json">json</option><option value="line">line</option><option value="bitable">bitable</option><option value="excel">excel</option><option value="parquet">parquet</option><option value="arrow">arrow</option>
                  </select>
                </label>
                {importForm.inputType !== 'bitable' && (
                  <label><span>{t('delimiter_label')}</span><input value={importForm.delimiter} onChange={(e) => setImportForm((c) => ({ ...c, delimiter: e.target.value }))} /></label>
                )}
              </div>
              {importForm.inputType === 'bitable' ? (
                <div className="field-grid">
                  <label><span>{t('bitable_app_id')}</span><input value={importForm.bitableAppId} onChange={(e) => setImportForm((c) => ({ ...c, bitableAppId: e.target.value }))} placeholder={t('bitable_app_id_placeholder')} /></label>
                  <label><span>{t('bitable_app_secret')}</span><input type="password" value={importForm.bitableAppSecret} onChange={(e) => setImportForm((c) => ({ ...c, bitableAppSecret: e.target.value }))} placeholder={t('bitable_app_secret_placeholder')} /></label>
                </div>
              ) : (
                <div className="field-grid">
                  <label><span>{t('columns_or_mappings')}</span><input value={importForm.columns} onChange={(e) => setImportForm((c) => ({ ...c, columns: e.target.value }))} placeholder={t('columns_placeholder')} /></label>
                  <label><span>{t('regex_pattern')}</span><input value={importForm.regexPattern} onChange={(e) => setImportForm((c) => ({ ...c, regexPattern: e.target.value }))} placeholder={t('regex_placeholder')} /></label>
                </div>
              )}
              <label>
                <span>{t('dataset_name')}</span>
                <input value={importForm.datasetName} onChange={(e) => setImportForm((c) => ({ ...c, datasetName: e.target.value }))} placeholder={importForm.inputType === 'bitable' ? t('bitable_dataset_name_placeholder') : t('dataset_name_placeholder')} />
              </label>

              <div className="subsection-card">
                <div className="subsection-head">
                  <div><h3>{t('import_embedding_title')}</h3><div className="helper">{t('import_embedding_hint')}</div></div>
                  <label className="toggle-row"><input type="checkbox" checked={importForm.embeddingEnabled} onChange={(e) => setImportForm((c) => ({ ...c, embeddingEnabled: e.target.checked }))} /><span>{t('embedding_enable')}</span></label>
                </div>
                <div className="field-grid">
                  <label><span>{t('embedding_text_columns')}</span><input value={importForm.embeddingTextColumns} onChange={(e) => setImportForm((c) => ({ ...c, embeddingTextColumns: e.target.value }))} placeholder={t('embedding_columns_placeholder')} disabled={!importForm.embeddingEnabled} /></label>
                  <label><span>{t('embedding_provider')}</span><input value={importForm.embeddingProvider} onChange={(e) => setImportForm((c) => ({ ...c, embeddingProvider: e.target.value }))} placeholder={t('embedding_provider_placeholder')} disabled={!importForm.embeddingEnabled} /></label>
                </div>
                <div className="field-grid">
                  <label><span>{t('embedding_model')}</span><select value={importForm.embeddingModel} onChange={(e) => setImportForm((c) => ({ ...c, embeddingModel: e.target.value }))} disabled={!importForm.embeddingEnabled}>{EMBEDDING_MODEL_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}</select></label>
                  <label><span>{t('embedding_template_version')}</span><select value={importForm.embeddingTemplateVersion} onChange={(e) => setImportForm((c) => ({ ...c, embeddingTemplateVersion: e.target.value }))} disabled={!importForm.embeddingEnabled}>{EMBEDDING_TEMPLATE_OPTIONS.map((v) => <option key={v} value={v}>{v}</option>)}</select></label>
                </div>
                <label><span>{t('embedding_vector_column')}</span><input value={importForm.embeddingVectorColumn} onChange={(e) => setImportForm((c) => ({ ...c, embeddingVectorColumn: e.target.value }))} placeholder={t('embedding_vector_placeholder')} disabled={!importForm.embeddingEnabled} /></label>
              </div>

              <div className="subsection-card">
                <div className="subsection-head">
                  <div><h3>{t('import_keyword_title')}</h3><div className="helper">{t('import_keyword_hint')}</div></div>
                  <label className="toggle-row"><input type="checkbox" checked={importForm.keywordEnabled} onChange={(e) => setImportForm((c) => ({ ...c, keywordEnabled: e.target.checked }))} /><span>{t('keyword_enable')}</span></label>
                </div>
                <div className="field-grid">
                  <label><span>{t('keyword_text_columns')}</span><input value={importForm.keywordTextColumns} onChange={(e) => setImportForm((c) => ({ ...c, keywordTextColumns: e.target.value }))} placeholder={t('keyword_columns_placeholder')} disabled={!importForm.keywordEnabled} /></label>
                  <label><span>{t('keyword_analyzer')}</span><input value={importForm.keywordAnalyzer} onChange={(e) => setImportForm((c) => ({ ...c, keywordAnalyzer: e.target.value }))} placeholder={t('keyword_analyzer_placeholder')} disabled={!importForm.keywordEnabled} /></label>
                </div>
              </div>
            </form>
            <div className="notice">{t('import_hint')}</div>
            <div className="notice subtle">{t('dataset_delete_hint')}</div>
            {importMessage && <div className={`notice ${importMessage.kind === 'error' ? 'error' : ''}`}>{importMessage.text}</div>}
            <div className="result-box">
              {pendingImport ? (
                <>
                  <div className="meta">
                    <span>{pendingImport.dataset.name}</span><span>{pendingImport.dataset.source_type}</span>
                    <span>{t('rows_count', { count: pendingImport.preview.row_count ?? '\u2014' })}</span>
                    <span>{importForm.embeddingEnabled ? t('embedding_configured_only') : t('embedding_disabled_short')}</span>
                  </div>
                  <div dangerouslySetInnerHTML={{ __html: renderPreviewTable(pendingImport.preview, t('no_preview_rows')) }} />
                </>
              ) : (<div className="empty">{t('no_preview_rows')}</div>)}
            </div>
            <div className="actions"><button onClick={() => void saveImportDataset()} disabled={!pendingImport || savingDataset}>{t('save_as_dataset')}</button></div>
          </div>
        </section>

        <section className="panel">
          <div className="panel-head"><h2>{t('datasets')}</h2></div>
          <div className="panel-body stack">
            {datasetMessage && <div className={`notice ${datasetMessage.kind === 'error' ? 'error' : ''}`}>{datasetMessage.text}</div>}
            <div className="list">
              {datasets.map((dataset) => (
                <div key={dataset.datasetId} className={`list-item ${dataset.datasetId === selectedDatasetId ? 'active' : ''}`} onClick={() => setSelectedDatasetId(dataset.datasetId)}>
                  <div className="item-head">
                    <h4>{dataset.name}</h4>
                    <div className="inline-actions">
                      <span className="badge">{getEmbeddingStatusLabel(dataset)}</span>
                      <button type="button" className="ghost danger-button" onClick={(e) => { e.stopPropagation(); removeDataset(dataset.datasetId); }}>{t('remove_dataset')}</button>
                    </div>
                  </div>
                  <div className="meta"><span>{dataset.sourceType}</span><span>{t('rows_count', { count: dataset.preview?.row_count ?? '\u2014' })}</span><span>{t(dataset.kind === 'result' ? 'kind_result' : 'kind_imported')}</span></div>
                </div>
              ))}
              {!datasets.length && <div className="empty">{t('no_datasets_available')}</div>}
            </div>
            {currentDataset && (
              <div className="result-box">
                <div className="item-head">
                  <h4>{currentDataset.name}</h4>
                  <div className="inline-actions">
                    <button className="ghost" type="button" onClick={exportCurrentDataset}>{t('export_file')}</button>
                    <button className="ghost" type="button" onClick={() => setView('analyze')}>{t('analyze_action')}</button>
                    <button className="ghost danger-button" type="button" onClick={() => removeDataset(currentDataset.datasetId)}>{t('remove_dataset')}</button>
                  </div>
                </div>
                <div className="compact-grid">
                  <div className="compact-card"><strong>{t('field_source')}</strong><div className="mono">{currentDataset.sourcePath}</div></div>
                  <div className="compact-card"><strong>{t('field_schema')}</strong><div>{currentDataset.schema.join(', ') || '\u2014'}</div></div>
                  <div className="compact-card"><strong>{t('field_embedding')}</strong><div>{currentEmbeddingSummary}</div></div>
                  <div className="compact-card"><strong>{t('field_keyword_index')}</strong><div>{currentKeywordSummary}</div></div>
                  <div className="compact-card"><strong>{t('field_kind')}</strong><div>{datasetKindLabel}</div></div>
                </div>
                {showBuildEmbeddingAction && (<div className="notice"><div>{t('embedding_build_needed_hint')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildEmbeddingDataset(currentDataset.datasetId)}>{t(currentEmbeddingStatus === 'failed' ? 'embedding_build_retry' : 'embedding_build_action')}</button></div></div>)}
                {isBuildingEmbedding && <div className="notice">{t('embedding_building_hint')}</div>}
                {currentEmbeddingStatus === 'failed' && currentEmbeddingBuild?.error && <div className="notice error">{t('embedding_build_failed', { error: currentEmbeddingBuild.error })}</div>}
                {showBuildKeywordAction && (<div className="notice"><div>{t('keyword_build_needed_hint')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildKeywordIndexDataset(currentDataset.datasetId)}>{t(currentKeywordStatus === 'failed' ? 'keyword_build_retry' : 'keyword_build_action')}</button></div></div>)}
                {isBuildingKeyword && <div className="notice">{t('keyword_building_hint')}</div>}
                {currentKeywordStatus === 'failed' && currentKeywordBuild?.error && <div className="notice error">{t('keyword_build_failed', { error: currentKeywordBuild.error })}</div>}
                <div dangerouslySetInnerHTML={{ __html: renderPreviewTable(currentDataset.preview, t('no_preview_rows')) }} />
              </div>
            )}
          </div>
        </section>
      </div>
    </section>
  );
}

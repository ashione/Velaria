import { useEffect, useMemo, useState } from 'react';
import type {
  AnalyzeState,
  DatasetRecord,
  EmbeddingBuildState,
  FilterBuilderState,
  HybridSearchState,
  KeywordSearchState,
  LastResultState,
  RunDetailPayload,
  TFunction,
} from '../types';
import {
  asRecord,
  defaultAnalyzeState,
  defaultHybridSearchState,
  defaultKeywordSearchState,
  escapeHtml,
  extractHybridExplain,
  extractHybridPreview,
  highlightSql,
  listToCsv,
  normalizePreview,
  parseSqlStructure,
  quoteIdentifier,
  quoteLiteral,
  renderHybridPreviewTable,
  renderPreviewTable,
} from '../types';

export type AnalyzeViewProps = {
  t: TFunction;
  api: (path: string, options?: RequestInit) => Promise<any>;
  datasets: DatasetRecord[];
  selectedDatasetId: string | null;
  setSelectedDatasetId: (id: string | null) => void;
  currentDataset: DatasetRecord | null;
  embeddingBuilds: Record<string, EmbeddingBuildState>;
  removeDataset: (datasetId: string) => void;
  buildEmbeddingDataset: (datasetId: string) => void;
  buildKeywordIndexDataset: (datasetId: string) => void;
  getEmbeddingStatusLabel: (dataset: DatasetRecord) => string;
  getEmbeddingUiStatus: (dataset: DatasetRecord | null) => 'disabled' | 'configured' | 'building' | 'failed' | 'ready';
  getKeywordUiStatus: (dataset: DatasetRecord | null) => 'disabled' | 'configured' | 'building' | 'failed' | 'ready';
  setSelectedRunId: (id: string | null) => void;
  refreshRuns: (id?: string | null) => void;
  aiSessionId: string | null;
  setAiSessionId: (id: string | null) => void;
};

export function AnalyzeView(props: AnalyzeViewProps) {
  const {
    t, api,
    datasets, selectedDatasetId, setSelectedDatasetId, currentDataset,
    embeddingBuilds,
    removeDataset, buildEmbeddingDataset, buildKeywordIndexDataset,
    getEmbeddingStatusLabel, getEmbeddingUiStatus, getKeywordUiStatus,
    setSelectedRunId, refreshRuns,
    aiSessionId, setAiSessionId,
  } = props;

  /* ---------- local state ---------- */
  const [datasetSearch, setDatasetSearch] = useState('');
  const [analysisState, setAnalysisState] = useState<AnalyzeState>(defaultAnalyzeState);
  const [filterBuilder, setFilterBuilder] = useState<FilterBuilderState>({ column: '', operator: '=', value: '' });
  const [hybridSearch, setHybridSearch] = useState<HybridSearchState>(defaultHybridSearchState);
  const [keywordSearch, setKeywordSearch] = useState<KeywordSearchState>(defaultKeywordSearchState);
  const [hybridMessage, setHybridMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [keywordMessage, setKeywordMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [lastResult, setLastResult] = useState<LastResultState>({ kind: 'sql', title: 'SQL', html: '<div class="empty">No run yet.</div>' });
  const [sqlUnderstandingExpanded, setSqlUnderstandingExpanded] = useState(false);
  const [aiPrompt, setAiPrompt] = useState('');
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState<string | null>(null);

  /* ---------- sync from current dataset ---------- */
  useEffect(() => {
    if (!currentDataset) return;
    setAnalysisState((prev) => ({
      ...prev,
      inputPath: currentDataset.sourcePath, inputType: currentDataset.sourceType,
      delimiter: currentDataset.importOptions.delimiter, columns: currentDataset.importOptions.columns,
      mappings: currentDataset.importOptions.mappings, regexPattern: currentDataset.importOptions.regexPattern,
      lineMode: currentDataset.importOptions.lineMode, jsonFormat: currentDataset.importOptions.jsonFormat,
    }));
    setHybridSearch((prev) => ({
      ...prev,
      textColumns: listToCsv(currentDataset.embeddingConfig.textColumns),
      provider: currentDataset.embeddingConfig.provider, model: currentDataset.embeddingConfig.model,
      templateVersion: currentDataset.embeddingConfig.templateVersion, vectorColumn: currentDataset.embeddingConfig.vectorColumn || 'embedding',
    }));
    setFilterBuilder((prev) => ({ ...prev, column: currentDataset.schema.includes(prev.column) ? prev.column : '' }));
  }, [currentDataset]);

  /* ---------- derived ---------- */
  const visibleDatasets = useMemo(() => {
    const kw = datasetSearch.trim().toLowerCase();
    if (!kw) return datasets;
    return datasets.filter((d) => d.name.toLowerCase().includes(kw) || d.sourceType.toLowerCase().includes(kw) || d.sourcePath.toLowerCase().includes(kw));
  }, [datasets, datasetSearch]);

  const sqlStructure = useMemo(() => parseSqlStructure(analysisState.query), [analysisState.query]);
  const highlightedSql = useMemo(() => highlightSql(analysisState.query), [analysisState.query]);
  const schemaColumns = currentDataset?.schema || [];

  const currentEmbeddingStatus = getEmbeddingUiStatus(currentDataset);
  const currentEmbeddingBuild = currentDataset ? embeddingBuilds[currentDataset.datasetId] : null;
  const currentKeywordStatus = getKeywordUiStatus(currentDataset);
  const hybridSearchReady = currentEmbeddingStatus === 'ready';

  const currentEmbeddingDatasetPath =
    currentEmbeddingStatus === 'ready' ? (currentDataset?.embeddingDataset?.datasetPath || '')
    : currentEmbeddingStatus === 'building' ? t('embedding_dataset_building')
    : currentEmbeddingStatus === 'failed' ? t('embedding_dataset_failed')
    : t('embedding_dataset_missing');
  const datasetKindLabel = currentDataset ? t(currentDataset.kind === 'result' ? 'kind_result' : 'kind_imported') : '\u2014';
  const currentEmbeddingSummary = currentDataset?.embeddingDataset?.datasetPath
    ? `${currentDataset.embeddingConfig.textColumns.join(', ') || '\u2014'} \u00b7 ${currentDataset.embeddingConfig.provider || '\u2014'} \u00b7 ${currentDataset.embeddingConfig.model || '\u2014'}`
    : currentEmbeddingStatus === 'building' ? `${t('embedding_building_short')} \u00b7 ${currentDataset?.embeddingConfig.textColumns.join(', ') || '\u2014'}`
    : currentEmbeddingStatus === 'failed' ? `${t('embedding_build_failed_short')} \u00b7 ${currentDataset?.embeddingConfig.textColumns.join(', ') || '\u2014'}`
    : currentDataset?.embeddingConfig.enabled ? `${t('embedding_configured_only')} \u00b7 ${currentDataset.embeddingConfig.textColumns.join(', ') || '\u2014'}`
    : t('embedding_disabled');
  const showBuildEmbeddingAction = currentDataset != null && (currentEmbeddingStatus === 'configured' || currentEmbeddingStatus === 'failed');
  const isBuildingEmbedding = currentEmbeddingStatus === 'building';

  /* ---------- handlers ---------- */
  function setPreset(preset: AnalyzeState['preset']) {
    const table = analysisState.tableName || 'input_table';
    let query = analysisState.query;
    if (preset === 'preview') query = `SELECT * FROM ${table} LIMIT 20`;
    if (preset === 'filter') query = `SELECT * FROM ${table} WHERE score > 20 LIMIT 50`;
    if (preset === 'aggregate') query = `SELECT score, COUNT(*) AS cnt FROM ${table} GROUP BY score ORDER BY cnt DESC LIMIT 20`;
    setAnalysisState((c) => ({ ...c, preset, query }));
  }

  function applyFilterBuilder() {
    if (!filterBuilder.column || !filterBuilder.value.trim()) return;
    const table = analysisState.tableName || 'input_table';
    setAnalysisState((c) => ({ ...c, query: `SELECT * FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(filterBuilder.column)} ${filterBuilder.operator} ${quoteLiteral(filterBuilder.value)} LIMIT 50`, preset: 'filter' }));
  }

  async function startAiSession() {
    if (!currentDataset) return;
    try {
      const result = await api('/api/v1/ai/sessions', {
        method: 'POST',
        body: JSON.stringify({
          dataset_context: {
            schema: currentDataset.schema,
            source_path: currentDataset.sourcePath,
            table_name: analysisState.tableName || 'input_table',
          },
        }),
      });
      if (result.ok && result.session_id) setAiSessionId(result.session_id);
    } catch { /* session creation is optional, ignore */ }
  }

  async function closeAiSession() {
    if (!aiSessionId) return;
    try {
      await api(`/api/v1/ai/sessions/${aiSessionId}`, { method: 'DELETE' });
    } catch { /* ignore */ }
    setAiSessionId(null);
  }

  async function generateSqlWithAi() {
    if (!aiPrompt.trim() || !currentDataset) return;
    setAiLoading(true);
    setAiError(null);
    try {
      const endpoint = aiSessionId
        ? `/api/v1/ai/sessions/${aiSessionId}/generate-sql`
        : '/api/v1/ai/generate-sql';
      const result = await api(endpoint, {
        method: 'POST',
        body: JSON.stringify({
          prompt: aiPrompt.trim(),
          schema: currentDataset.schema,
          table_name: analysisState.tableName || 'input_table',
          sample_rows: (currentDataset.preview?.rows || []).slice(0, 3),
        }),
      });
      if (result.ok && result.sql) {
        setAnalysisState((c) => ({ ...c, query: result.sql }));
        setAiPrompt('');
      } else {
        setAiError(t('ai_generate_failed', { error: result.error || 'Unknown error' }));
      }
    } catch (error) {
      setAiError(t('ai_generate_failed', { error: String(error) }));
    } finally {
      setAiLoading(false);
    }
  }

  async function runAnalysis() {
    setLastResult({ kind: 'sql', title: t('run_detail'), html: `<div class="empty">${escapeHtml(t('running_analysis'))}</div>` });
    try {
      const payload = { input_path: analysisState.inputPath, input_type: analysisState.inputType, delimiter: analysisState.delimiter, line_mode: analysisState.lineMode, regex_pattern: analysisState.regexPattern || undefined, mappings: analysisState.mappings || undefined, columns: analysisState.columns || undefined, json_format: analysisState.jsonFormat, table: analysisState.tableName, query: analysisState.query, run_name: `analysis-${new Date().toISOString()}`, description: 'Desktop workbench analysis run' };
      const rp = await api('/api/v1/runs/file-sql', { method: 'POST', body: JSON.stringify(payload) });
      setSelectedRunId(rp.run_id);
      const rd = (await api(`/api/v1/runs/${encodeURIComponent(rp.run_id)}/result?limit=20`)) as RunDetailPayload;
      const html = `<div class="meta"><span>${escapeHtml(rp.run.status)}</span><span>${escapeHtml(rp.run.run_id)}</span><span>${escapeHtml(t('rows_count', { count: rd.preview.row_count || '\u2014' }))}</span></div>${renderPreviewTable(rd.preview, t('no_preview_rows'))}`;
      setLastResult({ kind: 'sql', title: t('run_detail'), html });
      refreshRuns(rp.run_id);
    } catch (error) {
      setLastResult({ kind: 'sql', title: t('run_detail'), html: `<div class="empty">${escapeHtml(t('run_failed', { error: String(error) }))}</div>` });
    }
  }

  async function runHybridSearch() {
    if (!currentDataset) { const html = `<div class="empty">${escapeHtml(t('no_dataset_for_analyze'))}</div>`; setLastResult({ kind: 'hybrid', title: t('hybrid_results'), html }); return; }
    const dp = currentDataset.embeddingDataset?.datasetPath || '';
    if (!hybridSearchReady || !dp) { const msg = t('hybrid_requires_embedding_dataset'); setLastResult({ kind: 'hybrid', title: t('hybrid_results'), html: `<div class="empty">${escapeHtml(msg)}</div>` }); setHybridMessage({ kind: 'error', text: msg }); return; }
    const qt = hybridSearch.queryText.trim();
    if (!qt) { const msg = t('hybrid_query_required'); setLastResult({ kind: 'hybrid', title: t('hybrid_results'), html: `<div class="empty">${escapeHtml(msg)}</div>` }); setHybridMessage({ kind: 'error', text: msg }); return; }
    setHybridMessage({ kind: 'info', text: t('hybrid_loading') });
    try {
      const ws = sqlStructure.where?.trim() || '';
      const result = await api('/api/v1/runs/hybrid-search', { method: 'POST', body: JSON.stringify({ dataset_path: dp, index_path: currentDataset?.keywordIndex?.indexPath || undefined, query_text: qt, provider: hybridSearch.provider.trim() || undefined, model: hybridSearch.model.trim() || undefined, template_version: hybridSearch.templateVersion.trim() || undefined, top_k: Number(hybridSearch.topK) || 10, where_sql: ws || undefined, vector_column: hybridSearch.vectorColumn.trim() || 'embedding' }) });
      const preview = extractHybridPreview(result); const explain = extractHybridExplain(result);
      const metric = String((asRecord(result.result)?.metric as string | undefined) || 'cosine');
      const ew = String((asRecord(result.result)?.where_sql as string | undefined) || ws);
      const html = `<div class="meta"><span>${escapeHtml(t('rows_count', { count: preview.row_count ?? preview.rows?.length ?? '\u2014' }))}</span><span>${escapeHtml(metric)}</span><span>${escapeHtml(ew ? t('hybrid_filter_applied') : t('hybrid_filter_none'))}</span><span>${escapeHtml(hybridSearch.vectorColumn.trim() || 'embedding')}</span><span>${escapeHtml(hybridSearch.provider.trim() || '\u2014')}</span></div>${ew ? `<div class="helper">${escapeHtml(t('hybrid_filter_sql', { where: ew }))}</div>` : ''}${explain ? `<pre class="explain-block">${escapeHtml(explain)}</pre>` : ''}${renderHybridPreviewTable(preview, t('hybrid_no_result'), metric)}`;
      setLastResult({ kind: 'hybrid', title: t('hybrid_results'), html }); setHybridMessage(null);
    } catch (error) {
      const html = `<div class="empty">${escapeHtml(t('hybrid_failed', { error: String(error) }))}</div>`;
      setLastResult({ kind: 'hybrid', title: t('hybrid_results'), html }); setHybridMessage({ kind: 'error', text: t('hybrid_failed', { error: String(error) }) });
    }
  }

  async function runKeywordSearch() {
    if (!currentDataset) { const html = `<div class="empty">${escapeHtml(t('no_dataset_for_analyze'))}</div>`; setLastResult({ kind: 'keyword', title: t('keyword_results'), html }); setKeywordMessage({ kind: 'error', text: t('no_dataset_for_analyze') }); return; }
    const ip = currentDataset.keywordIndex?.indexPath || '';
    if (!ip) { const msg = t('keyword_requires_index'); setLastResult({ kind: 'keyword', title: t('keyword_results'), html: `<div class="empty">${escapeHtml(msg)}</div>` }); setKeywordMessage({ kind: 'error', text: msg }); return; }
    const qt = keywordSearch.queryText.trim();
    if (!qt) { const msg = t('keyword_query_required'); setLastResult({ kind: 'keyword', title: t('keyword_results'), html: `<div class="empty">${escapeHtml(msg)}</div>` }); setKeywordMessage({ kind: 'error', text: msg }); return; }
    setKeywordMessage({ kind: 'info', text: t('keyword_loading') });
    try {
      const ws = sqlStructure.where?.trim() || '';
      const result = await api('/api/v1/runs/keyword-search', { method: 'POST', body: JSON.stringify({ index_path: ip, query_text: qt, top_k: Number(keywordSearch.topK) || 10, where_sql: ws || undefined }) });
      const preview = normalizePreview(asRecord(result)?.preview);
      setLastResult({ kind: 'keyword', title: t('keyword_results'), html: renderPreviewTable(preview, t('keyword_no_result')) }); setKeywordMessage(null);
    } catch (error) {
      const msg = t('keyword_failed', { error: String(error) });
      setLastResult({ kind: 'keyword', title: t('keyword_results'), html: `<div class="empty">${escapeHtml(msg)}</div>` }); setKeywordMessage({ kind: 'error', text: msg });
    }
  }

  /* ---------- derived JSX ---------- */
  const sqlCards = [['sql_part_select', sqlStructure.select], ['sql_part_from', sqlStructure.from], ['sql_part_where', sqlStructure.where], ['sql_part_group_by', sqlStructure.groupBy], ['sql_part_order_by', sqlStructure.orderBy], ['sql_part_limit', sqlStructure.limit]] as const;

  const datasetCards = visibleDatasets.map((dataset) => {
    const embStatus = getEmbeddingStatusLabel(dataset);
    const kwStatus = getKeywordUiStatus(dataset);
    return (
      <div key={dataset.datasetId} className={`analyze-dataset-item ${dataset.datasetId === selectedDatasetId ? 'active' : ''}`} onClick={() => setSelectedDatasetId(dataset.datasetId)}>
        <div className="item-head"><h4>{dataset.name}</h4><button type="button" className="ghost danger-button" onClick={(e) => { e.stopPropagation(); removeDataset(dataset.datasetId); }}>{t('remove_dataset')}</button></div>
        <div className="meta"><span>{dataset.sourceType}</span><span>{t('rows_count', { count: dataset.preview?.row_count ?? '\u2014' })}</span><span>{embStatus}</span><span>{kwStatus === 'ready' ? t('keyword_ready') : kwStatus === 'configured' ? t('keyword_configured_only') : kwStatus === 'building' ? t('keyword_building_short') : kwStatus === 'failed' ? t('keyword_build_failed_short') : t('keyword_disabled_short')}</span></div>
      </div>
    );
  });

  return (
    <section className="section active">
      <div className="stack">
        <section className="panel">
          <div className="panel-head"><h2>{t('analyze_workspace')}</h2><div className="meta"><span>{t('meta_sql_mode')}</span><span>{t('meta_run_tracked')}</span></div></div>
          <div className="panel-body stack">
            {datasets.length ? (
              <div className="summary-band analyze-summary-band">
                <div className="subsection-card">
                  <div className="subsection-head"><h3>{t('datasets')}</h3></div>
                  <div className="stack">
                    <input className="analyze-search" placeholder={t('analysis_dataset_search')} value={datasetSearch} onChange={(e) => setDatasetSearch(e.target.value)} />
                    <div className="analyze-dataset-list" style={{ maxHeight: 220, overflow: 'auto' }}>
                      {datasetCards}
                      {!datasetCards.length && <div className="empty">{t('no_dataset_for_analyze')}</div>}
                    </div>
                  </div>
                </div>
                <div className="subsection-card">
                  <div className="subsection-head"><h3>{t('dataset_context')}</h3><div className="actions"><button type="button" className="ghost" onClick={() => setSqlUnderstandingExpanded((v) => !v)} disabled={!currentDataset}>{t(sqlUnderstandingExpanded ? 'collapse' : 'expand')}</button></div></div>
                  <div className="stack">
                    <label><span>{t('dataset_label')}</span><select value={selectedDatasetId || ''} onChange={(e) => setSelectedDatasetId(e.target.value)} disabled={!visibleDatasets.length}>{visibleDatasets.length ? visibleDatasets.map((d) => <option key={d.datasetId} value={d.datasetId}>{d.name}</option>) : <option value="">{t('no_dataset_option')}</option>}</select></label>
                    {currentDataset ? (
                      <>
                        <div className="compact-grid">
                          <div className="compact-card"><strong>{t('field_source')}</strong><div className="mono">{currentDataset.sourcePath}</div></div>
                          <div className="compact-card"><strong>{t('field_embedding_dataset')}</strong><div className="mono">{currentEmbeddingDatasetPath || '\u2014'}</div></div>
                          <div className="compact-card"><strong>{t('field_schema')}</strong><div>{currentDataset.schema.join(', ') || '\u2014'}</div></div>
                          <div className="compact-card"><strong>{t('field_embedding')}</strong><div>{currentEmbeddingSummary}</div></div>
                          <div className="compact-card"><strong>{t('field_kind')}</strong><div>{datasetKindLabel}</div></div>
                        </div>
                        {showBuildEmbeddingAction && <div className="notice"><div>{t('embedding_build_needed_hint')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildEmbeddingDataset(currentDataset.datasetId)}>{t(currentEmbeddingStatus === 'failed' ? 'embedding_build_retry' : 'embedding_build_action')}</button></div></div>}
                        {isBuildingEmbedding && <div className="notice">{t('embedding_building_hint')}</div>}
                        {currentEmbeddingStatus === 'failed' && currentEmbeddingBuild?.error && <div className="notice error">{t('embedding_build_failed', { error: currentEmbeddingBuild.error })}</div>}
                      </>
                    ) : (<div className="compact-empty-card"><div className="empty">{t('dataset_detail_empty')}</div></div>)}
                    {sqlUnderstandingExpanded && currentDataset && <div className="compact-grid">{sqlCards.map(([labelKey, value]) => <div key={labelKey} className="compact-card"><strong>{t(labelKey)}</strong><div>{value || t('sql_part_not_set')}</div></div>)}</div>}
                  </div>
                </div>
              </div>
            ) : (<div className="compact-empty-card"><div className="empty">{t('no_dataset_for_analyze')}</div></div>)}

            <div className="workbench-stack">
            <div className="workbench-form">
              <div className="query-toolbar">
                <label className="field"><span>{t('input_path')}</span><input value={analysisState.inputPath} onChange={(e) => setAnalysisState((c) => ({ ...c, inputPath: e.target.value }))} /></label>
                <label className="field"><span>{t('input_type')}</span><select value={analysisState.inputType} onChange={(e) => setAnalysisState((c) => ({ ...c, inputType: e.target.value }))}><option value="auto">auto</option><option value="csv">csv</option><option value="json">json</option><option value="line">line</option><option value="bitable">bitable</option><option value="excel">excel</option><option value="parquet">parquet</option><option value="arrow">arrow</option></select></label>
                <label className="field"><span>{t('table_name')}</span><input value={analysisState.tableName} onChange={(e) => setAnalysisState((c) => ({ ...c, tableName: e.target.value }))} /></label>
                <label className="field"><span>{t('query_preset')}</span><select value={analysisState.preset} onChange={(e) => setPreset(e.target.value as AnalyzeState['preset'])}><option value="preview">{t('preset_preview')}</option><option value="filter">{t('preset_filter')}</option><option value="aggregate">{t('preset_aggregate')}</option></select></label>
              </div>
              <div className="query-toolbar">
                <label className="field"><span>{t('filter_column')}</span><select value={filterBuilder.column} onChange={(e) => setFilterBuilder((c) => ({ ...c, column: e.target.value }))}><option value="">{t('filter_column_placeholder')}</option>{schemaColumns.map((col) => <option key={col} value={col}>{col}</option>)}</select></label>
                <label className="field"><span>{t('filter_operator')}</span><select value={filterBuilder.operator} onChange={(e) => setFilterBuilder((c) => ({ ...c, operator: e.target.value as FilterBuilderState['operator'] }))}><option value="=">=</option><option value="!=">!=</option><option value=">">&gt;</option><option value=">=">&gt;=</option><option value="<">&lt;</option><option value="<=">&lt;=</option></select></label>
                <label className="field"><span>{t('filter_value')}</span><input value={filterBuilder.value} onChange={(e) => setFilterBuilder((c) => ({ ...c, value: e.target.value }))} /></label>
                <div className="field"><span>{t('filter_quick_action')}</span><button type="button" className="ghost" onClick={applyFilterBuilder}>{t('filter_generate')}</button></div>
              </div>
              <div className="hybrid-inline-block">
                <div className="helper">{t('hybrid_search_inline_hint')}</div>
                {hybridMessage && <div className={`notice ${hybridMessage.kind === 'error' ? 'error' : ''}`}>{hybridMessage.text}</div>}
                {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'disabled' && <div className="notice error">{t('hybrid_requires_embedding_enabled')}</div>}
                {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'configured' && <div className="notice"><div>{t('hybrid_requires_embedding_dataset')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildEmbeddingDataset(currentDataset.datasetId)}>{t('embedding_build_action')}</button></div></div>}
                {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'building' && <div className="notice">{t('embedding_building_hint')}</div>}
                {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'failed' && <div className="notice error"><div>{t('hybrid_requires_embedding_dataset')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildEmbeddingDataset(currentDataset.datasetId)}>{t('embedding_build_retry')}</button></div></div>}
                <div className="hybrid-inline-form">
                  <label><span>{t('hybrid_query')}</span><input value={hybridSearch.queryText} onChange={(e) => setHybridSearch((c) => ({ ...c, queryText: e.target.value }))} placeholder={t('hybrid_query_placeholder')} disabled={!currentDataset} /></label>
                  <div className="helper">{t('hybrid_locked_config_hint', { columns: currentDataset?.embeddingConfig.textColumns.join(', ') || '\u2014', provider: currentDataset?.embeddingConfig.provider || '\u2014', model: currentDataset?.embeddingConfig.model || '\u2014' })}</div>
                  <div className="field-grid"><label><span>{t('hybrid_top_k')}</span><input value={hybridSearch.topK} onChange={(e) => setHybridSearch((c) => ({ ...c, topK: e.target.value }))} disabled={!currentDataset} /></label></div>
                </div>
              </div>
              <div className="hybrid-inline-block">
                <div className="helper">{t('keyword_search_inline_hint')}</div>
                {keywordMessage && <div className={`notice ${keywordMessage.kind === 'error' ? 'error' : ''}`}>{keywordMessage.text}</div>}
                {!currentDataset?.keywordConfig.enabled && currentDataset && <div className="notice error">{t('keyword_requires_enabled')}</div>}
                {currentDataset && currentKeywordStatus === 'configured' && <div className="notice"><div>{t('keyword_requires_index')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildKeywordIndexDataset(currentDataset.datasetId)}>{t('keyword_build_action')}</button></div></div>}
                {currentDataset && currentKeywordStatus === 'building' && <div className="notice">{t('keyword_building_hint')}</div>}
                {currentDataset && currentKeywordStatus === 'failed' && <div className="notice error"><div>{t('keyword_requires_index')}</div><div className="actions notice-actions"><button type="button" className="ghost" onClick={() => buildKeywordIndexDataset(currentDataset.datasetId)}>{t('keyword_build_retry')}</button></div></div>}
                <div className="hybrid-inline-form">
                  <label><span>{t('keyword_query')}</span><input value={keywordSearch.queryText} onChange={(e) => setKeywordSearch((c) => ({ ...c, queryText: e.target.value }))} placeholder={t('keyword_query_placeholder')} disabled={!currentDataset} /></label>
                  <div className="helper">{t('keyword_locked_config_hint', { columns: currentDataset?.keywordConfig.textColumns.join(', ') || '\u2014', analyzer: currentDataset?.keywordConfig.analyzer || '\u2014' })}</div>
                  <div className="field-grid"><label><span>{t('keyword_top_k')}</span><input value={keywordSearch.topK} onChange={(e) => setKeywordSearch((c) => ({ ...c, topK: e.target.value }))} disabled={!currentDataset} /></label></div>
                </div>
              </div>
              <div className="ai-assist">
                <label>
                  <span>{t('ai_prompt_label')}</span>
                  <div className="ai-input-row">
                    <input
                      value={aiPrompt}
                      onChange={(e) => setAiPrompt(e.target.value)}
                      placeholder={t('ai_prompt_placeholder')}
                      disabled={aiLoading || !currentDataset}
                      onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); generateSqlWithAi(); } }}
                    />
                    <button
                      type="button"
                      onClick={() => void generateSqlWithAi()}
                      disabled={aiLoading || !aiPrompt.trim() || !currentDataset}
                    >
                      {aiLoading ? t('ai_generating') : t('ai_generate_sql')}
                    </button>
                  </div>
                </label>
                {aiError && <div className="notice error">{aiError}</div>}
                <div className="ai-session-controls">
                  {aiSessionId ? (
                    <>
                      <span className="ai-session-badge">{t('ai_session_active')}: {aiSessionId.slice(-8)}</span>
                      <button type="button" className="ghost" onClick={() => void closeAiSession()}>{t('ai_session_close')}</button>
                    </>
                  ) : (
                    <button type="button" className="ghost" onClick={() => void startAiSession()} disabled={!currentDataset}>{t('ai_session_start')}</button>
                  )}
                </div>
              </div>
              <label><span>{t('sql_query')}</span><div className="editor-shell"><pre className="editor-highlight" dangerouslySetInnerHTML={{ __html: highlightedSql }} /><textarea className="editor-input" spellCheck={false} value={analysisState.query} onChange={(e) => setAnalysisState((c) => ({ ...c, query: e.target.value }))} /></div></label>
              <div className="actions primary-actions">
                <button type="button" onClick={() => void runAnalysis()}>{t('run_analysis')}</button>
                <button type="button" onClick={() => void runKeywordSearch()} disabled={!currentDataset || !currentDataset.keywordIndex?.indexPath}>{t('run_keyword_search')}</button>
                <button type="button" onClick={() => void runHybridSearch()} disabled={!currentDataset || !hybridSearchReady}>{t('run_hybrid_search')}</button>
              </div>
            </div>
            </div>

            <div className="result-stack">
              <div className="helper">{lastResult.title}</div>
              <div className="result-box" dangerouslySetInnerHTML={{ __html: lastResult.html }} />
            </div>
          </div>
        </section>
      </div>
    </section>
  );
}

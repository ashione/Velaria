import { useEffect, useMemo, useState } from 'react';
import { DATASETS_KEY, I18N, LOCALE_KEY, type Locale } from './i18n';

type DatasetRecord = {
  datasetId: string;
  name: string;
  sourceType: string;
  sourcePath: string;
  preview: {
    schema?: string[];
    rows?: Record<string, unknown>[];
    row_count?: number;
    truncated?: boolean;
  };
  schema: string[];
  kind: 'imported' | 'result';
  createdAt: string;
  description: string;
  sourceLabel: string;
};

type RunSummary = {
  run_id: string;
  status: string;
  action: string;
  artifact_count?: number;
  run_name?: string | null;
};

type RunDetailPayload = {
  run: RunSummary & Record<string, unknown>;
  artifact: {
    artifact_id: string;
    uri: string;
    format: string;
    schema_json?: string[];
  };
  preview: {
    schema?: string[];
    rows?: Record<string, unknown>[];
    row_count?: number;
    truncated?: boolean;
  };
};

type ImportPreviewPayload = {
  dataset: {
    name: string;
    source_type: string;
    source_path: string;
  };
  preview: DatasetRecord['preview'];
};

type ServiceInfo = {
  baseUrl: string;
  packaged: boolean;
};

type ViewKey = 'home' | 'data' | 'analyze' | 'runs';

type AnalyzeState = {
  inputPath: string;
  inputType: string;
  tableName: string;
  preset: 'preview' | 'filter' | 'aggregate';
  query: string;
};

const defaultAnalyzeState: AnalyzeState = {
  inputPath: '',
  inputType: 'auto',
  tableName: 'input_table',
  preset: 'preview',
  query: 'SELECT * FROM input_table LIMIT 20',
};

const viewMeta = {
  home: { titleKey: 'view_home_title', subtitleKey: 'view_home_subtitle' },
  data: { titleKey: 'view_data_title', subtitleKey: 'view_data_subtitle' },
  analyze: { titleKey: 'view_analyze_title', subtitleKey: 'view_analyze_subtitle' },
  runs: { titleKey: 'view_runs_title', subtitleKey: 'view_runs_subtitle' },
} as const;

function decodeFileUri(uri: string): string {
  try {
    if (!uri.startsWith('file://')) return uri;
    return decodeURIComponent(new URL(uri).pathname);
  } catch {
    return uri;
  }
}

function createDatasetRecord(payload: {
  name: string;
  sourceType: string;
  sourcePath: string;
  preview: DatasetRecord['preview'];
  kind: DatasetRecord['kind'];
  description?: string;
}): DatasetRecord {
  return {
    datasetId: `dataset_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
    name: payload.name,
    sourceType: payload.sourceType,
    sourcePath: payload.sourcePath,
    preview: payload.preview,
    schema: payload.preview.schema || [],
    kind: payload.kind,
    createdAt: new Date().toISOString(),
    description: payload.description || '',
    sourceLabel: '',
  };
}

function loadDatasets(): DatasetRecord[] {
  try {
    const raw = window.localStorage.getItem(DATASETS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveDatasets(datasets: DatasetRecord[]) {
  window.localStorage.setItem(DATASETS_KEY, JSON.stringify(datasets));
}

function highlightSql(sql: string): string {
  let html = sql
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
  html = html.replace(/(--.*)$/gm, '<span class="sql-token-comment">$1</span>');
  html = html.replace(/('(?:''|[^'])*')/g, '<span class="sql-token-string">$1</span>');
  html = html.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="sql-token-number">$1</span>');
  html = html.replace(
    /\b(SELECT|FROM|WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|INSERT\s+INTO|CREATE\s+TABLE|CREATE\s+SOURCE\s+TABLE|CREATE\s+SINK\s+TABLE|VALUES|AND|OR|AS|JOIN|LEFT|RIGHT|INNER|OUTER|ON|ASC|DESC|COUNT|SUM|AVG|MIN|MAX)\b/gi,
    '<span class="sql-token-keyword">$1</span>'
  );
  html = html.replace(/\b([A-Za-z_][A-Za-z0-9_]*)(?=\s*\()/g, '<span class="sql-token-identifier">$1</span>');
  return html || '&nbsp;';
}

function extractClause(sql: string, startPattern: RegExp, endPatterns: RegExp[] = []): string | null {
  const source = sql.replace(/\s+/g, ' ').trim();
  const start = source.search(startPattern);
  if (start === -1) return null;
  const afterStart = source.slice(start).replace(startPattern, '').trim();
  let endIndex = afterStart.length;
  for (const pattern of endPatterns) {
    const matchIndex = afterStart.search(pattern);
    if (matchIndex !== -1) endIndex = Math.min(endIndex, matchIndex);
  }
  return afterStart.slice(0, endIndex).trim() || null;
}

function parseSqlStructure(sql: string) {
  return {
    select: extractClause(sql, /\bSELECT\b/i, [/\bFROM\b/i]),
    from: extractClause(sql, /\bFROM\b/i, [/\bWHERE\b/i, /\bGROUP\s+BY\b/i, /\bORDER\s+BY\b/i, /\bLIMIT\b/i]),
    where: extractClause(sql, /\bWHERE\b/i, [/\bGROUP\s+BY\b/i, /\bORDER\s+BY\b/i, /\bLIMIT\b/i]),
    groupBy: extractClause(sql, /\bGROUP\s+BY\b/i, [/\bORDER\s+BY\b/i, /\bLIMIT\b/i]),
    orderBy: extractClause(sql, /\bORDER\s+BY\b/i, [/\bLIMIT\b/i]),
    limit: extractClause(sql, /\bLIMIT\b/i),
  };
}

export function App() {
  const [locale, setLocale] = useState<Locale>(() => (window.localStorage.getItem(LOCALE_KEY) as Locale) || 'zh');
  const [view, setView] = useState<ViewKey>('home');
  const [serviceInfo, setServiceInfo] = useState<ServiceInfo | null>(null);
  const [serviceStatus, setServiceStatus] = useState('status_bootstrapping');
  const [serviceMeta, setServiceMeta] = useState('status_waiting');
  const [datasets, setDatasets] = useState<DatasetRecord[]>(() => loadDatasets());
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null);
  const [datasetSearch, setDatasetSearch] = useState('');
  const [pendingImport, setPendingImport] = useState<ImportPreviewPayload | null>(null);
  const [importMessage, setImportMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [runsPage, setRunsPage] = useState(1);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedRunDetail, setSelectedRunDetail] = useState<RunDetailPayload | null>(null);
  const [analysisState, setAnalysisState] = useState<AnalyzeState>(defaultAnalyzeState);
  const [analysisResultHtml, setAnalysisResultHtml] = useState('<div class="empty">No run yet.</div>');
  const [sqlUnderstandingExpanded, setSqlUnderstandingExpanded] = useState(true);

  const t = (key: string, vars: Record<string, string | number> = {}) => {
    const dict = I18N[locale] || I18N.en;
    const raw = dict[key] || I18N.en[key] || key;
    return Object.entries(vars).reduce(
      (acc, [name, value]) => acc.replaceAll(`{${name}}`, String(value)),
      raw
    );
  };

  useEffect(() => {
    window.localStorage.setItem(LOCALE_KEY, locale);
    document.title = t('app_title');
    document.documentElement.lang = locale === 'zh' ? 'zh-CN' : 'en';
  }, [locale]);

  useEffect(() => {
    saveDatasets(datasets);
    if (!selectedDatasetId && datasets[0]) {
      setSelectedDatasetId(datasets[0].datasetId);
    }
  }, [datasets, selectedDatasetId]);

  const currentDataset = useMemo(
    () => datasets.find((dataset) => dataset.datasetId === selectedDatasetId) || null,
    [datasets, selectedDatasetId]
  );

  const visibleDatasets = useMemo(() => {
    const keyword = datasetSearch.trim().toLowerCase();
    if (!keyword) return datasets;
    return datasets.filter((dataset) => {
      return (
        dataset.name.toLowerCase().includes(keyword) ||
        dataset.sourceType.toLowerCase().includes(keyword) ||
        dataset.sourcePath.toLowerCase().includes(keyword)
      );
    });
  }, [datasets, datasetSearch]);

  const sqlStructure = useMemo(() => parseSqlStructure(analysisState.query), [analysisState.query]);
  const highlightedSql = useMemo(() => highlightSql(analysisState.query), [analysisState.query]);

  const pagedRuns = useMemo(() => {
    const start = (runsPage - 1) * 8;
    return runs.slice(start, start + 8);
  }, [runs, runsPage]);

  async function api(path: string, options: RequestInit = {}) {
    if (!serviceInfo) throw new Error('service unavailable');
    const response = await fetch(`${serviceInfo.baseUrl}${path}`, {
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
      ...options,
    });
    const payload = await response.json();
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || `request failed: ${response.status}`);
    }
    return payload;
  }

  async function bootstrap() {
    try {
      const info = await window.velariaShell.getServiceInfo();
      setServiceInfo(info);
      const health = await fetch(`${info.baseUrl}/health`).then((r) => r.json());
      setServiceStatus(t('status_ready_on', { port: health.port }));
      setServiceMeta(t('status_packaged', { packaged: info.packaged, version: health.version }));
      const runsPayload = await fetch(`${info.baseUrl}/api/runs?limit=100`).then((r) => r.json());
      setRuns(runsPayload.runs || []);
      if (!selectedRunId && runsPayload.runs?.[0]) {
        setSelectedRunId(runsPayload.runs[0].run_id);
      }
    } catch (error) {
      setServiceStatus(t('status_bootstrap_failed'));
      setServiceMeta(String(error));
    }
  }

  useEffect(() => {
    void bootstrap();
  }, []);

  useEffect(() => {
    if (currentDataset) {
      setAnalysisState((prev) => ({
        ...prev,
        inputPath: currentDataset.sourcePath,
        inputType: currentDataset.sourceType,
      }));
    }
  }, [currentDataset]);

  async function refreshRuns() {
    const payload = await api('/api/runs?limit=100');
    setRuns(payload.runs || []);
    setRunsPage(1);
    if (!selectedRunId && payload.runs?.[0]) {
      setSelectedRunId(payload.runs[0].run_id);
    }
  }

  async function previewImport(event: React.FormEvent) {
    event.preventDefault();
    setImportMessage({ kind: 'info', text: t('loading_preview') });
    try {
      const payload: Record<string, string> = {
        input_path: (document.getElementById('import-path') as HTMLInputElement).value.trim(),
        input_type: (document.getElementById('import-type') as HTMLSelectElement).value,
        delimiter: (document.getElementById('import-delimiter') as HTMLInputElement).value,
        dataset_name: (document.getElementById('import-dataset-name') as HTMLInputElement).value.trim(),
      };
      const columns = (document.getElementById('import-columns') as HTMLInputElement).value.trim();
      const regexPattern = (document.getElementById('import-regex-pattern') as HTMLInputElement).value.trim();
      if (payload.input_type === 'json') payload.columns = columns;
      if (payload.input_type === 'line') {
        payload.mappings = columns;
        payload.regex_pattern = regexPattern;
        payload.line_mode = regexPattern ? 'regex' : 'split';
      }
      const result = (await api('/api/import/preview', {
        method: 'POST',
        body: JSON.stringify(payload),
      })) as ImportPreviewPayload;
      setPendingImport(result);
      setImportMessage(null);
    } catch (error) {
      setPendingImport(null);
      setImportMessage({ kind: 'error', text: t('preview_failed', { error: String(error) }) });
    }
  }

  function saveImportDataset() {
    if (!pendingImport) return;
    const record = createDatasetRecord({
      name: pendingImport.dataset.name,
      sourceType: pendingImport.dataset.source_type,
      sourcePath: pendingImport.dataset.source_path,
      preview: pendingImport.preview,
      kind: 'imported',
    });
    setDatasets((current) => [record, ...current]);
    setSelectedDatasetId(record.datasetId);
    setPendingImport(null);
    setView('analyze');
  }

  function setPreset(preset: AnalyzeState['preset']) {
    const table = analysisState.tableName || 'input_table';
    let query = analysisState.query;
    if (preset === 'preview') query = `SELECT * FROM ${table} LIMIT 20`;
    if (preset === 'filter') query = `SELECT * FROM ${table} WHERE score > 20 LIMIT 50`;
    if (preset === 'aggregate') {
      query = `SELECT score, COUNT(*) AS cnt FROM ${table} GROUP BY score ORDER BY cnt DESC LIMIT 20`;
    }
    setAnalysisState((current) => ({ ...current, preset, query }));
  }

  async function runAnalysis(event: React.FormEvent) {
    event.preventDefault();
    setAnalysisResultHtml(`<div class="empty">${t('running_analysis')}</div>`);
    try {
      const payload = {
        input_path: analysisState.inputPath,
        input_type: analysisState.inputType,
        table: analysisState.tableName,
        query: analysisState.query,
        run_name: `analysis-${new Date().toISOString()}`,
        description: 'Desktop workbench analysis run',
      };
      const runPayload = await api('/api/runs/file-sql', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      setSelectedRunId(runPayload.run_id);
      const runDetail = (await api(`/api/runs/${encodeURIComponent(runPayload.run_id)}/result?limit=20`)) as RunDetailPayload;
      setSelectedRunDetail(runDetail);
      setAnalysisResultHtml(`
        <div class="meta">
          <span>${runPayload.run.status}</span>
          <span>${runPayload.run.run_id}</span>
          <span>${t('rows_count', { count: runDetail.preview.row_count || '—' })}</span>
        </div>
        <div class="preview-wrap"><table><thead><tr>${(runDetail.preview.schema || []).map((name: string) => `<th>${name}</th>`).join('')}</tr></thead><tbody>${(runDetail.preview.rows || [])
          .map((row: Record<string, unknown>) => `<tr>${(runDetail.preview.schema || []).map((name: string) => `<td>${String(row[name] ?? '')}</td>`).join('')}</tr>`)
          .join('')}</tbody></table></div>
      `);
      await refreshRuns();
    } catch (error) {
      setSelectedRunDetail(null);
      setAnalysisResultHtml(`<div class="empty">${t('run_failed', { error: String(error) })}</div>`);
    }
  }

  async function loadRunDetail(runId: string) {
    try {
      const payload = (await api(`/api/runs/${encodeURIComponent(runId)}/result?limit=20`)) as RunDetailPayload;
      setSelectedRunDetail(payload);
    } catch {
      setSelectedRunDetail(null);
    }
  }

  useEffect(() => {
    if (selectedRunId && view === 'runs') {
      void loadRunDetail(selectedRunId);
    }
  }, [selectedRunId, view]);

  async function exportPath(sourcePath: string) {
    return window.velariaShell.exportFile({ sourcePath });
  }

  async function exportCurrentDataset() {
    if (!currentDataset) return;
    await exportPath(currentDataset.sourcePath);
  }

  async function exportCurrentRunArtifact() {
    if (!selectedRunDetail?.artifact?.uri) return;
    await exportPath(decodeFileUri(selectedRunDetail.artifact.uri));
  }

  function saveResultAsDataset() {
    if (!selectedRunDetail) return;
    const record = createDatasetRecord({
      name: `result-${selectedRunDetail.run.run_id.slice(0, 12)}`,
      sourceType:
        selectedRunDetail.artifact.format === 'arrow'
          ? 'arrow'
          : selectedRunDetail.artifact.format,
      sourcePath: decodeFileUri(selectedRunDetail.artifact.uri),
      preview: selectedRunDetail.preview,
      kind: 'result',
      description: `Saved from run ${selectedRunDetail.run.run_id}`,
    });
    setDatasets((current) => [record, ...current]);
    setSelectedDatasetId(record.datasetId);
    setView('data');
  }

  const datasetCards = visibleDatasets.map((dataset) => (
    <div
      key={dataset.datasetId}
      className={`analyze-dataset-item ${dataset.datasetId === selectedDatasetId ? 'active' : ''}`}
      onClick={() => setSelectedDatasetId(dataset.datasetId)}
    >
      <h4>{dataset.name}</h4>
      <div className="meta">
        <span>{dataset.sourceType}</span>
        <span>{t('rows_count', { count: dataset.preview?.row_count ?? '—' })}</span>
      </div>
    </div>
  ));

  const sqlCards = [
    ['sql_part_select', sqlStructure.select],
    ['sql_part_from', sqlStructure.from],
    ['sql_part_where', sqlStructure.where],
    ['sql_part_group_by', sqlStructure.groupBy],
    ['sql_part_order_by', sqlStructure.orderBy],
    ['sql_part_limit', sqlStructure.limit],
  ] as const;

  return (
    <div className="shell">
      <aside className="sidebar">
        <h1 className="brand">Velaria</h1>
        <p className="brand-sub">{t('brand_sub')}</p>
        <div className="lang-switch">
          <button className={locale === 'en' ? 'active' : ''} onClick={() => setLocale('en')}>
            English
          </button>
          <button className={locale === 'zh' ? 'active' : ''} onClick={() => setLocale('zh')}>
            中文
          </button>
        </div>
        <nav className="nav">
          {(['home', 'data', 'analyze', 'runs'] as ViewKey[]).map((key) => (
            <button
              key={key}
              className={view === key ? 'active' : ''}
              onClick={() => setView(key)}
            >
              {t(`nav_${key}`)}
            </button>
          ))}
        </nav>
        <div className="sidebar-stack">
          <section className="status-card">
            <h3>{t('service_title')}</h3>
            <div className={`status-pill ${serviceInfo ? '' : 'warn'}`}>{serviceStatus}</div>
            <p className="brand-sub" style={{ marginTop: 12 }}>{serviceMeta}</p>
          </section>
          <section className="side-note">
            <h3>{t('scope_title')}</h3>
            <div className="brand-sub" style={{ margin: 0 }}>{t('scope_body')}</div>
          </section>
        </div>
      </aside>

      <main className="main">
        <section className="hero">
          <div>
            <h1>{t(viewMeta[view].titleKey)}</h1>
            <p>{t(viewMeta[view].subtitleKey)}</p>
          </div>
        </section>

        {view === 'home' && (
          <section className="section active">
            <div className="card-grid">
              <div className="metric">
                <div className="metric-label">{t('metric_datasets')}</div>
                <div className="metric-value">{datasets.length}</div>
              </div>
              <div className="metric">
                <div className="metric-label">{t('metric_saved_results')}</div>
                <div className="metric-value">
                  {datasets.filter((dataset) => dataset.kind === 'result').length}
                </div>
              </div>
              <div className="metric">
                <div className="metric-label">{t('metric_runs')}</div>
                <div className="metric-value">{runs.length}</div>
              </div>
              <div className="metric">
                <div className="metric-label">{t('metric_last_status')}</div>
                <div className="metric-value">{runs[0]?.status || '—'}</div>
              </div>
            </div>
            <div className="grid">
              <section className="panel half">
                <div className="panel-head">
                  <h2>{t('quick_actions')}</h2>
                </div>
                <div className="panel-body stack">
                  <div className="actions">
                    <button onClick={() => setView('data')}>{t('quick_import')}</button>
                    <button className="ghost" onClick={() => setView('analyze')}>
                      {t('quick_analyze')}
                    </button>
                    <button className="ghost" onClick={() => setView('runs')}>
                      {t('quick_runs')}
                    </button>
                  </div>
                  <div className="helper">{t('quick_helper')}</div>
                </div>
              </section>
              <section className="panel half">
                <div className="panel-head">
                  <h2>{t('recent_datasets')}</h2>
                </div>
                <div className="panel-body">
                  <div className="list">
                    {datasets.slice(0, 4).map((dataset) => (
                      <div
                        key={dataset.datasetId}
                        className="list-item"
                        onClick={() => {
                          setSelectedDatasetId(dataset.datasetId);
                          setView('data');
                        }}
                      >
                        <h4>{dataset.name}</h4>
                        <div className="meta">
                          <span>{dataset.sourceType}</span>
                          <span>{t('rows_count', { count: dataset.preview?.row_count ?? '—' })}</span>
                        </div>
                      </div>
                    ))}
                    {!datasets.length && <div className="empty">{t('no_datasets_yet')}</div>}
                  </div>
                </div>
              </section>
            </div>
          </section>
        )}

        {view === 'data' && (
          <section className="section active">
            <div className="split">
              <section className="panel">
                <div className="panel-head">
                  <h2>{t('import_wizard')}</h2>
                </div>
                <div className="panel-body">
                  <form onSubmit={previewImport}>
                    <label>
                      <span>{t('input_path')}</span>
                      <input id="import-path" placeholder={t('input_placeholder')} required />
                    </label>
                    <div className="actions">
                      <button
                        type="button"
                        className="ghost"
                        onClick={async () => {
                          const path = await window.velariaShell.pickFile();
                          if (path) {
                            (document.getElementById('import-path') as HTMLInputElement).value = path;
                          }
                        }}
                      >
                        {t('choose_file')}
                      </button>
                      <button type="submit">{t('preview_import')}</button>
                    </div>
                    <div className="field-grid">
                      <label>
                        <span>{t('input_type')}</span>
                        <select id="import-type" defaultValue="auto">
                          <option value="auto">auto</option>
                          <option value="csv">csv</option>
                          <option value="json">json</option>
                          <option value="line">line</option>
                          <option value="excel">excel</option>
                          <option value="parquet">parquet</option>
                          <option value="arrow">arrow</option>
                        </select>
                      </label>
                      <label>
                        <span>{t('delimiter_label')}</span>
                        <input id="import-delimiter" defaultValue="," />
                      </label>
                    </div>
                    <div className="field-grid">
                      <label>
                        <span>{t('columns_or_mappings')}</span>
                        <input id="import-columns" placeholder={t('columns_placeholder')} />
                      </label>
                      <label>
                        <span>{t('regex_pattern')}</span>
                        <input id="import-regex-pattern" placeholder={t('regex_placeholder')} />
                      </label>
                    </div>
                    <label>
                      <span>{t('dataset_name')}</span>
                      <input id="import-dataset-name" placeholder={t('dataset_name_placeholder')} />
                    </label>
                  </form>
                  <div className="notice">{t('import_hint')}</div>
                  {importMessage && (
                    <div className={`notice ${importMessage.kind === 'error' ? 'error' : ''}`}>
                      {importMessage.text}
                    </div>
                  )}
                  <div className="result-box">
                    {pendingImport ? (
                      <>
                        <div className="meta">
                          <span>{pendingImport.dataset.name}</span>
                          <span>{pendingImport.dataset.source_type}</span>
                          <span>{t('rows_count', { count: pendingImport.preview.row_count ?? '—' })}</span>
                        </div>
                        <div
                          dangerouslySetInnerHTML={{
                            __html: renderPreviewTable(pendingImport.preview),
                          }}
                        />
                      </>
                    ) : (
                      <div className="empty">{t('no_preview_rows')}</div>
                    )}
                  </div>
                  <div className="actions">
                    <button onClick={saveImportDataset} disabled={!pendingImport}>
                      {t('save_as_dataset')}
                    </button>
                  </div>
                </div>
              </section>

              <section className="panel">
                <div className="panel-head">
                  <h2>{t('datasets')}</h2>
                </div>
                <div className="panel-body">
                  <div className="list">
                    {datasets.map((dataset) => (
                      <div
                        key={dataset.datasetId}
                        className={`list-item ${dataset.datasetId === selectedDatasetId ? 'active' : ''}`}
                        onClick={() => setSelectedDatasetId(dataset.datasetId)}
                      >
                        <h4>{dataset.name}</h4>
                        <div className="meta">
                          <span>{dataset.sourceType}</span>
                          <span>{t('rows_count', { count: dataset.preview?.row_count ?? '—' })}</span>
                          <span>{dataset.kind}</span>
                        </div>
                      </div>
                    ))}
                    {!datasets.length && <div className="empty">{t('no_datasets_available')}</div>}
                  </div>
                </div>
              </section>
            </div>
          </section>
        )}

        {view === 'analyze' && (
          <section className="section active">
            <div className="analyze-workbench">
              <section className="workbench-panel">
                <div className="workbench-head">
                  <h3>{t('datasets')}</h3>
                </div>
                <div className="workbench-body">
                  <div className="notice">{t('analyze_hint')}</div>
                  <input
                    className="analyze-search"
                    placeholder={t('analysis_dataset_search')}
                    value={datasetSearch}
                    onChange={(event) => setDatasetSearch(event.target.value)}
                  />
                  <div className="analyze-dataset-list">
                    {datasetCards}
                    {!datasetCards.length && <div className="empty">{t('no_datasets_available')}</div>}
                  </div>
                </div>
              </section>

              <section className="workbench-panel">
                <div className="workbench-head">
                  <h2>{t('analyze_workspace')}</h2>
                  <div className="meta">
                    <span>{t('meta_sql_mode')}</span>
                    <span>{t('meta_run_tracked')}</span>
                  </div>
                </div>
                <div className="workbench-body">
                  <form onSubmit={runAnalysis}>
                    <div className="query-toolbar">
                      <label className="field">
                        <span>{t('input_path')}</span>
                        <input
                          value={analysisState.inputPath}
                          onChange={(event) =>
                            setAnalysisState((current) => ({ ...current, inputPath: event.target.value }))
                          }
                        />
                      </label>
                      <label className="field">
                        <span>{t('input_type')}</span>
                        <select
                          value={analysisState.inputType}
                          onChange={(event) =>
                            setAnalysisState((current) => ({ ...current, inputType: event.target.value }))
                          }
                        >
                          <option value="auto">auto</option>
                          <option value="csv">csv</option>
                          <option value="json">json</option>
                          <option value="line">line</option>
                          <option value="excel">excel</option>
                          <option value="parquet">parquet</option>
                          <option value="arrow">arrow</option>
                        </select>
                      </label>
                      <label className="field">
                        <span>{t('table_name')}</span>
                        <input
                          value={analysisState.tableName}
                          onChange={(event) =>
                            setAnalysisState((current) => ({ ...current, tableName: event.target.value }))
                          }
                        />
                      </label>
                      <label className="field">
                        <span>{t('query_preset')}</span>
                        <select
                          value={analysisState.preset}
                          onChange={(event) => setPreset(event.target.value as AnalyzeState['preset'])}
                        >
                          <option value="preview">{t('preset_preview')}</option>
                          <option value="filter">{t('preset_filter')}</option>
                          <option value="aggregate">{t('preset_aggregate')}</option>
                        </select>
                      </label>
                    </div>
                    <label>
                      <span>{t('sql_query')}</span>
                      <div className="editor-shell">
                        <pre
                          className="editor-highlight"
                          dangerouslySetInnerHTML={{ __html: highlightedSql }}
                        />
                        <textarea
                          className="editor-input"
                          spellCheck={false}
                          value={analysisState.query}
                          onChange={(event) =>
                            setAnalysisState((current) => ({ ...current, query: event.target.value }))
                          }
                        />
                      </div>
                    </label>
                    <div className="actions">
                      <button
                        type="button"
                        className="ghost"
                        onClick={async () => {
                          const path = await window.velariaShell.pickFile();
                          if (path) {
                            setAnalysisState((current) => ({ ...current, inputPath: path }));
                          }
                        }}
                      >
                        {t('choose_file')}
                      </button>
                      <button type="submit">{t('run_analysis')}</button>
                      <button
                        type="button"
                        className="ghost"
                        disabled={!selectedRunId}
                        onClick={() => setView('runs')}
                      >
                        {t('open_run_detail')}
                      </button>
                      <button
                        type="button"
                        className="ghost"
                        disabled={!selectedRunDetail}
                        onClick={exportCurrentRunArtifact}
                      >
                        {t('export_result_file')}
                      </button>
                      <button
                        type="button"
                        className="ghost"
                        disabled={!selectedRunDetail}
                        onClick={saveResultAsDataset}
                      >
                        {t('save_result_as_dataset')}
                      </button>
                    </div>
                  </form>

                  <div className="result-stack">
                    <div className="helper">{t('run_detail')}</div>
                    <div
                      className="result-box"
                      dangerouslySetInnerHTML={{ __html: analysisResultHtml }}
                    />
                  </div>
                </div>
              </section>

              <aside className="inspector-stack">
                <section className="workbench-panel">
                  <div className="workbench-head">
                    <h3>{t('dataset_context')}</h3>
                  </div>
                  <div className="workbench-body">
                    <label>
                      <span>{t('dataset_label')}</span>
                      <select
                        value={selectedDatasetId || ''}
                        onChange={(event) => setSelectedDatasetId(event.target.value)}
                      >
                        {visibleDatasets.map((dataset) => (
                          <option key={dataset.datasetId} value={dataset.datasetId}>
                            {dataset.name}
                          </option>
                        ))}
                      </select>
                    </label>
                    <div className="result-box">
                      {currentDataset ? (
                        <>
                          <div className="meta">
                            <span>{currentDataset.sourceType}</span>
                            <span>{t('rows_count', { count: currentDataset.preview?.row_count ?? '—' })}</span>
                            <span>{currentDataset.kind}</span>
                          </div>
                          <div className="mono">
                            <strong>{t('field_source')}:</strong> {currentDataset.sourcePath}
                          </div>
                          <div className="mono">
                            <strong>{t('field_schema')}:</strong> {currentDataset.schema.join(', ') || '—'}
                          </div>
                          <div className="mono">
                            <strong>{t('field_dataset_id')}:</strong> {currentDataset.datasetId}
                          </div>
                        </>
                      ) : (
                        <div className="empty">{t('no_dataset_for_analyze')}</div>
                      )}
                    </div>
                  </div>
                </section>

                <section className="workbench-panel">
                  <div className="workbench-head">
                    <h3>{t('sql_understanding_title')}</h3>
                    <button
                      type="button"
                      className="ghost"
                      onClick={() => setSqlUnderstandingExpanded((value) => !value)}
                    >
                      {t(sqlUnderstandingExpanded ? 'collapse' : 'expand')}
                    </button>
                  </div>
                  {sqlUnderstandingExpanded && (
                    <div className="workbench-body">
                      <div className="sql-understanding-grid">
                        {sqlCards.map(([labelKey, value]) => (
                          <div key={labelKey} className="sql-understanding-card">
                            <strong>{t(labelKey)}</strong>
                            <div>{value || t('sql_part_not_set')}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </section>
              </aside>
            </div>
          </section>
        )}

        {view === 'runs' && (
          <section className="section active">
            <div className="split">
              <section className="panel">
                <div className="panel-head">
                  <h2>{t('run_history')}</h2>
                  <div className="actions">
                    <button className="ghost" onClick={() => void refreshRuns()}>
                      {t('refresh')}
                    </button>
                  </div>
                </div>
                <div className="panel-body">
                  <div className="list">
                    {pagedRuns.map((run) => (
                      <div
                        key={run.run_id}
                        className={`list-item ${run.run_id === selectedRunId ? 'active' : ''}`}
                        onClick={() => setSelectedRunId(run.run_id)}
                      >
                        <h4>{run.run_name || run.run_id}</h4>
                        <div className="meta">
                          <span>{run.status}</span>
                          <span>{run.action}</span>
                          <span>{`${run.artifact_count ?? 0} ${t('label_artifacts')}`}</span>
                        </div>
                      </div>
                    ))}
                    {!runs.length && <div className="empty">{t('no_runs_yet')}</div>}
                  </div>
                  <div className="actions" style={{ marginTop: 14, justifyContent: 'space-between', alignItems: 'center' }}>
                    <div className="helper">{t('page_status', { page: runs.length ? runsPage : 0, total: Math.max(1, Math.ceil(runs.length / 8)) })}</div>
                    <div className="actions">
                      <button className="ghost" disabled={runsPage <= 1} onClick={() => setRunsPage((page) => Math.max(1, page - 1))}>
                        {t('page_prev')}
                      </button>
                      <button
                        className="ghost"
                        disabled={runsPage >= Math.max(1, Math.ceil(runs.length / 8))}
                        onClick={() => setRunsPage((page) => Math.min(Math.max(1, Math.ceil(runs.length / 8)), page + 1))}
                      >
                        {t('page_next')}
                      </button>
                    </div>
                  </div>
                </div>
              </section>

              <section className="panel">
                <div className="panel-head">
                  <h2>{t('run_detail')}</h2>
                </div>
                <div className="panel-body">
                  <div className="result-box">
                    {selectedRunDetail ? (
                      <>
                        <div className="meta">
                          <span>{String(selectedRunDetail.run.status)}</span>
                          <span>{String(selectedRunDetail.run.action)}</span>
                          <span>{t('rows_count', { count: selectedRunDetail.preview.row_count ?? '—' })}</span>
                        </div>
                        <div className="mono">
                          <strong>{t('field_run_id')}:</strong> {String(selectedRunDetail.run.run_id)}
                        </div>
                        <div className="mono">
                          <strong>{t('field_artifact_id')}:</strong> {selectedRunDetail.artifact.artifact_id}
                        </div>
                        <div className="mono">
                          <strong>{t('field_artifact_uri')}:</strong> {selectedRunDetail.artifact.uri}
                        </div>
                        <div className="actions">
                          <button className="ghost" onClick={exportCurrentRunArtifact}>
                            {t('export_file')}
                          </button>
                          <button className="ghost" onClick={saveResultAsDataset}>
                            {t('save_result_action')}
                          </button>
                          <button
                            className="ghost"
                            onClick={() => {
                              if (selectedRunDetail) {
                                const record = createDatasetRecord({
                                  name: `result-${selectedRunDetail.run.run_id.slice(0, 12)}`,
                                  sourceType:
                                    selectedRunDetail.artifact.format === 'arrow'
                                      ? 'arrow'
                                      : selectedRunDetail.artifact.format,
                                  sourcePath: decodeFileUri(selectedRunDetail.artifact.uri),
                                  preview: selectedRunDetail.preview,
                                  kind: 'result',
                                });
                                setDatasets((current) => [record, ...current]);
                                setSelectedDatasetId(record.datasetId);
                                setView('analyze');
                              }
                            }}
                          >
                            {t('analyze_action')}
                          </button>
                        </div>
                        <div
                          dangerouslySetInnerHTML={{
                            __html: renderPreviewTable(selectedRunDetail.preview),
                          }}
                        />
                      </>
                    ) : (
                      <div className="empty">{t('run_detail_empty')}</div>
                    )}
                  </div>
                </div>
              </section>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}

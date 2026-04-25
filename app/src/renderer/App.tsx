import { useEffect, useMemo, useState } from 'react';
import { I18N, LOCALE_KEY, type Locale } from './i18n';
import type {
  AppConfig,
  DatasetRecord,
  EmbeddingBuildState,
  ExternalSourceRecord,
  FocusEventRecord,
  MonitorFormState,
  MonitorRecord,
  RunDetailPayload,
  RunSummary,
  ServiceInfo,
  SourceFormState,
  TFunction,
  ViewKey,
} from './types';
import {
  asRecord,
  createDatasetRecord,
  createEmbeddingBuildPayload,
  createKeywordIndexBuildPayload,
  decodeFileUri,
  defaultEmbeddingConfig,
  defaultMonitorForm,
  defaultSourceForm,
  extractEmbeddingDatasetRecord,
  extractKeywordIndexRecord,
  loadDatasets,
  saveDatasets,
  viewMeta,
} from './types';
import { useService } from './hooks/useService';
import { DataView } from './components/DataView';
import { AnalyzeView } from './components/AnalyzeView';
import { RunsView } from './components/RunsView';
import { SettingsPanel } from './components/SettingsPanel';
import { MonitorsView } from './components/MonitorsView';

export function App() {
  /* ---------- locale ---------- */
  const [locale, setLocale] = useState<Locale>(() => (window.localStorage.getItem(LOCALE_KEY) as Locale) || 'zh');
  const t: TFunction = (key, vars = {}) => {
    const dict = I18N[locale] || I18N.en;
    const raw = dict[key] || I18N.en[key] || key;
    return Object.entries(vars).reduce((acc, [n, v]) => acc.replaceAll(`{${n}}`, String(v)), raw);
  };

  /* ---------- top-level state ---------- */
  const [view, setView] = useState<ViewKey>('data');
  const [serviceInfo, setServiceInfo] = useState<ServiceInfo | null>(null);
  const [serviceStatus, setServiceStatus] = useState('status_bootstrapping');
  const [serviceMeta, setServiceMeta] = useState('status_waiting');
  const [configForm, setConfigForm] = useState<AppConfig>({ bitableAppId: '', bitableAppSecret: '' });
  const [configMessage, setConfigMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [datasets, setDatasets] = useState<DatasetRecord[]>(() => loadDatasets());
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [sources, setSources] = useState<ExternalSourceRecord[]>([]);
  const [monitors, setMonitors] = useState<MonitorRecord[]>([]);
  const [focusEvents, setFocusEvents] = useState<FocusEventRecord[]>([]);
  const [sourceForm, setSourceForm] = useState<SourceFormState>(defaultSourceForm);
  const [monitorForm, setMonitorForm] = useState<MonitorFormState>(defaultMonitorForm);
  const [monitorMessage, setMonitorMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [datasetMessage, setDatasetMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [embeddingBuilds, setEmbeddingBuilds] = useState<Record<string, EmbeddingBuildState>>({});

  const { api, waitForRunCompletion } = useService(serviceInfo);

  /* ---------- derived ---------- */
  const currentDataset = useMemo(
    () => datasets.find((d) => d.datasetId === selectedDatasetId) || null,
    [datasets, selectedDatasetId],
  );

  /* ---------- side-effects ---------- */
  useEffect(() => {
    window.localStorage.setItem(LOCALE_KEY, locale);
    document.title = t('app_title');
    document.documentElement.lang = locale === 'zh' ? 'zh-CN' : 'en';
  }, [locale]);

  useEffect(() => {
    saveDatasets(datasets);
    if (selectedDatasetId && !datasets.some((d) => d.datasetId === selectedDatasetId)) {
      setSelectedDatasetId(datasets[0]?.datasetId ?? null);
      return;
    }
    if (!selectedDatasetId && datasets[0]) setSelectedDatasetId(datasets[0].datasetId);
  }, [datasets, selectedDatasetId]);

  /* ---------- bootstrap ---------- */
  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;
    const attempt = async () => {
      try {
        const info = await window.velariaShell.getServiceInfo();
        const cfg = await window.velariaShell.getConfig();
        setServiceInfo(info);
        setConfigForm({ bitableAppId: cfg.bitableAppId || '', bitableAppSecret: cfg.bitableAppSecret || '' });
        const health = await fetch(`${info.baseUrl}/health`).then((r) => r.json());
        setServiceStatus(t('status_ready_on', { port: health.port }));
        setServiceMeta(t('status_packaged', { packaged: String(info.packaged), version: health.version }));
        const rp = await fetch(`${info.baseUrl}/api/v1/runs?limit=100`).then((r) => r.json());
        setRuns(rp.runs || []);
        const [sp, mp, ep] = await Promise.all([
          fetch(`${info.baseUrl}/api/v1/external-events/sources`).then((r) => r.json()),
          fetch(`${info.baseUrl}/api/v1/monitors`).then((r) => r.json()),
          fetch(`${info.baseUrl}/api/v1/focus-events?limit=50`).then((r) => r.json()),
        ]);
        setSources(sp.sources || []); setMonitors(mp.monitors || []); setFocusEvents(ep.focus_events || []);
        if (rp.runs?.[0]) setSelectedRunId(rp.runs[0].run_id);
      } catch (error) {
        setServiceStatus(t('status_waiting'));
        setServiceMeta(String(error));
        if (!cancelled) timer = window.setTimeout(() => void attempt(), 1000);
      }
    };
    void attempt();
    return () => { cancelled = true; if (timer !== null) window.clearTimeout(timer); };
  }, []);

  /* ---------- monitors polling ---------- */
  useEffect(() => {
    if (view !== 'monitors' || !serviceInfo) return;
    void refreshMonitors();
    const timer = window.setInterval(() => void refreshMonitors(), 3000);
    return () => window.clearInterval(timer);
  }, [view, serviceInfo]);

  /* ---------- shared handlers ---------- */
  const [runsPage, setRunsPage] = useState(1);

  async function refreshRuns(nextId: string | null = selectedRunId) {
    const p = await api('/api/v1/runs?limit=100');
    const next: RunSummary[] = p.runs || [];
    setRuns(next); setRunsPage(1);
    const fallback = nextId && next.some((r) => r.run_id === nextId) ? nextId : next[0]?.run_id ?? null;
    setSelectedRunId(fallback);
  }

  async function refreshMonitors() {
    const [s, m, e] = await Promise.all([
      api('/api/v1/external-events/sources'),
      api('/api/v1/monitors'),
      api('/api/v1/focus-events?limit=50'),
    ]);
    setSources(s.sources || []); setMonitors(m.monitors || []); setFocusEvents(e.focus_events || []);
  }

  function removeDataset(datasetId: string) {
    const target = datasets.find((d) => d.datasetId === datasetId);
    if (!target || !window.confirm(t('confirm_remove_dataset', { name: target.name }))) return;
    setDatasets((c) => c.filter((d) => d.datasetId !== datasetId));
    setEmbeddingBuilds((c) => { const n = { ...c }; delete n[datasetId]; return n; });
    setDatasetMessage({ kind: 'info', text: t('dataset_removed_message', { name: target.name }) });
  }

  function getEmbeddingUiStatus(ds: DatasetRecord | null): 'disabled' | 'configured' | 'building' | 'failed' | 'ready' {
    if (!ds) return 'disabled';
    if (ds.embeddingDataset?.datasetPath) return 'ready';
    const tr = embeddingBuilds[ds.datasetId];
    if (tr?.status === 'building') return 'building';
    if (tr?.status === 'failed') return 'failed';
    if (ds.embeddingConfig.enabled) return 'configured';
    return 'disabled';
  }

  function getKeywordUiStatus(ds: DatasetRecord | null): 'disabled' | 'configured' | 'building' | 'failed' | 'ready' {
    if (!ds) return 'disabled';
    if (ds.keywordIndex?.indexPath) return 'ready';
    const tr = embeddingBuilds[`${ds.datasetId}::keyword`];
    if (tr?.status === 'building') return 'building';
    if (tr?.status === 'failed') return 'failed';
    if (ds.keywordConfig.enabled) return 'configured';
    return 'disabled';
  }

  function getEmbeddingStatusLabel(ds: DatasetRecord | null) {
    const s = getEmbeddingUiStatus(ds);
    if (s === 'ready') return t('embedding_ready');
    if (s === 'building') return t('embedding_building_short');
    if (s === 'failed') return t('embedding_build_failed_short');
    if (s === 'configured') return t('embedding_configured_only');
    return t('embedding_disabled_short');
  }

  async function buildEmbeddingDataset(datasetId: string, snap?: DatasetRecord) {
    const target = snap || datasets.find((d) => d.datasetId === datasetId);
    if (!target || !target.embeddingConfig.enabled || target.embeddingDataset?.datasetPath || embeddingBuilds[datasetId]?.status === 'building') return;
    setEmbeddingBuilds((c) => ({ ...c, [datasetId]: { status: 'building' } }));
    setDatasetMessage({ kind: 'info', text: t('embedding_build_started', { name: target.name }) });
    try {
      const br = await api('/api/v1/runs/embedding-build', { method: 'POST', body: JSON.stringify(createEmbeddingBuildPayload(target)) });
      const ed = extractEmbeddingDatasetRecord(br); const res = asRecord(br.result);
      setDatasets((c) => c.map((d) => d.datasetId !== datasetId ? d : {
        ...d, embeddingDataset: ed, embeddingConfig: {
          ...d.embeddingConfig,
          provider: typeof res?.provider === 'string' ? res.provider : d.embeddingConfig.provider,
          model: typeof res?.model === 'string' ? res.model : d.embeddingConfig.model,
          templateVersion: typeof res?.template_version === 'string' ? res.template_version : d.embeddingConfig.templateVersion,
          vectorColumn: typeof res?.vector_column === 'string' ? res.vector_column : d.embeddingConfig.vectorColumn,
        },
      }));
      setEmbeddingBuilds((c) => { const n = { ...c }; delete n[datasetId]; return n; });
      setDatasetMessage({ kind: 'info', text: t('embedding_build_succeeded', { name: target.name }) });
    } catch (error) {
      const msg = String(error);
      setEmbeddingBuilds((c) => ({ ...c, [datasetId]: { status: 'failed', error: msg } }));
      setDatasetMessage({ kind: 'error', text: t('embedding_build_failed', { error: msg }) });
    }
  }

  async function buildKeywordIndexDataset(datasetId: string, snap?: DatasetRecord) {
    const target = snap || datasets.find((d) => d.datasetId === datasetId);
    const key = `${datasetId}::keyword`;
    if (!target || !target.keywordConfig.enabled || target.keywordIndex?.indexPath || embeddingBuilds[key]?.status === 'building') return;
    setEmbeddingBuilds((c) => ({ ...c, [key]: { status: 'building' } }));
    setDatasetMessage({ kind: 'info', text: t('keyword_build_started', { name: target.name }) });
    try {
      const br = await api('/api/v1/runs/keyword-index-build', { method: 'POST', body: JSON.stringify(createKeywordIndexBuildPayload(target)) });
      const ki = extractKeywordIndexRecord(br); const res = asRecord(br.result);
      setDatasets((c) => c.map((d) => d.datasetId !== datasetId ? d : {
        ...d, keywordIndex: ki, keywordConfig: { ...d.keywordConfig, analyzer: typeof res?.analyzer === 'string' ? res.analyzer : d.keywordConfig.analyzer },
      }));
      setEmbeddingBuilds((c) => { const n = { ...c }; delete n[key]; return n; });
      setDatasetMessage({ kind: 'info', text: t('keyword_build_succeeded', { name: target.name }) });
    } catch (error) {
      const msg = String(error);
      setEmbeddingBuilds((c) => ({ ...c, [key]: { status: 'failed', error: msg } }));
      setDatasetMessage({ kind: 'error', text: t('keyword_build_failed', { error: msg }) });
    }
  }

  async function saveConfig() {
    try {
      const saved = await window.velariaShell.saveConfig({
        bitableAppId: configForm.bitableAppId.trim() || undefined,
        bitableAppSecret: configForm.bitableAppSecret.trim() || undefined,
      });
      setConfigForm({ bitableAppId: saved.bitableAppId || '', bitableAppSecret: saved.bitableAppSecret || '' });
      setConfigMessage({ kind: 'info', text: t('settings_saved') });
    } catch (error) {
      setConfigMessage({ kind: 'error', text: t('settings_save_failed', { error: String(error) }) });
    }
  }

  async function exportCurrentDataset() {
    if (!currentDataset) return;
    await window.velariaShell.exportFile({ sourcePath: currentDataset.sourcePath });
  }

  function saveRunDetailAsDataset(detail: RunDetailPayload, nextView: ViewKey = 'data') {
    const record = createDatasetRecord({
      name: `result-${detail.run.run_id.slice(0, 12)}`,
      sourceType: (detail.artifact?.format || '') === 'arrow' ? 'arrow' : detail.artifact?.format || 'parquet',
      sourcePath: decodeFileUri(detail.artifact?.uri || ''),
      preview: detail.preview,
      kind: 'result',
      description: `Saved from run ${detail.run.run_id}`,
      embeddingConfig: defaultEmbeddingConfig,
    });
    setDatasets((c) => [record, ...c]);
    setSelectedDatasetId(record.datasetId);
    setDatasetMessage({ kind: 'info', text: t('dataset_saved_from_run') });
    setView(nextView);
  }

  /* ---------- monitor handlers ---------- */
  async function createExternalSource(event: React.FormEvent) {
    event.preventDefault(); setMonitorMessage(null);
    try {
      await api('/api/v1/external-events/sources', { method: 'POST', body: JSON.stringify({
        source_id: sourceForm.sourceId.trim() || undefined,
        name: sourceForm.name.trim() || sourceForm.sourceId.trim() || 'external-source',
        schema_binding: { time_field: sourceForm.timeField.trim(), type_field: sourceForm.typeField.trim(), key_field: sourceForm.keyField.trim(), field_mappings: { price: sourceForm.priceField.trim() } },
      }) });
      setMonitorMessage({ kind: 'info', text: t('source_created') }); setSourceForm(defaultSourceForm); await refreshMonitors();
    } catch (error) { setMonitorMessage({ kind: 'error', text: t('source_create_failed', { error: String(error) }) }); }
  }

  async function createMonitorFromIntent(event: React.FormEvent) {
    event.preventDefault(); setMonitorMessage(null);
    try {
      await api('/api/v1/monitors/from-intent', { method: 'POST', body: JSON.stringify({
        name: monitorForm.name.trim() || undefined, intent_text: monitorForm.intentText.trim(),
        source: { kind: 'external_event', source_id: monitorForm.sourceId }, execution_mode: monitorForm.executionMode,
        template_params: { group_by: monitorForm.groupBy.split(',').map((s) => s.trim()).filter(Boolean), count_threshold: Number(monitorForm.countThreshold || '2') },
      }) });
      setMonitorMessage({ kind: 'info', text: t('monitor_created') }); setMonitorForm((c) => ({ ...defaultMonitorForm, sourceId: c.sourceId })); await refreshMonitors();
    } catch (error) { setMonitorMessage({ kind: 'error', text: t('monitor_create_failed', { error: String(error) }) }); }
  }

  async function monitorAction(monitorId: string, action: 'validate' | 'enable' | 'disable' | 'run' | 'delete') {
    setMonitorMessage(null);
    try {
      if (action === 'delete') await api(`/api/v1/monitors/${encodeURIComponent(monitorId)}`, { method: 'DELETE' });
      else await api(`/api/v1/monitors/${encodeURIComponent(monitorId)}/${action}`, { method: 'POST', body: JSON.stringify({}) });
      await refreshMonitors();
    } catch (error) { setMonitorMessage({ kind: 'error', text: String(error) }); }
  }

  async function focusEventAction(eventId: string, action: 'consume' | 'archive') {
    setMonitorMessage(null);
    try {
      await api(`/api/v1/focus-events/${encodeURIComponent(eventId)}/${action}`, { method: 'POST', body: JSON.stringify({}) });
      await refreshMonitors();
    } catch (error) { setMonitorMessage({ kind: 'error', text: String(error) }); }
  }

  /* ---------- render ---------- */
  return (
    <div className="shell">
      <aside className="sidebar">
        <h1 className="brand">Velaria</h1>
        <p className="brand-sub">{t('brand_sub')}</p>
        <div className="lang-switch">
          <button className={locale === 'en' ? 'active' : ''} onClick={() => setLocale('en')}>English</button>
          <button className={locale === 'zh' ? 'active' : ''} onClick={() => setLocale('zh')}>中文</button>
        </div>
        <nav className="nav">
          {(['data', 'analyze', 'runs',
            // Monitor view hidden — agentic features paused for v0.3 focus
            // 'monitors',
            'settings'] as ViewKey[]).map((key) => (
            <button key={key} className={view === key ? 'active' : ''} onClick={() => setView(key)}>
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
        {view === 'data' && (
          <DataView t={t} api={api} waitForRunCompletion={waitForRunCompletion}
            datasets={datasets} setDatasets={setDatasets} selectedDatasetId={selectedDatasetId} setSelectedDatasetId={setSelectedDatasetId}
            currentDataset={currentDataset} configForm={configForm} embeddingBuilds={embeddingBuilds}
            datasetMessage={datasetMessage} setDatasetMessage={setDatasetMessage}
            removeDataset={removeDataset} exportCurrentDataset={exportCurrentDataset}
            buildEmbeddingDataset={(id, s) => void buildEmbeddingDataset(id, s)} buildKeywordIndexDataset={(id, s) => void buildKeywordIndexDataset(id, s)}
            setView={setView} getEmbeddingStatusLabel={getEmbeddingStatusLabel} getEmbeddingUiStatus={getEmbeddingUiStatus} getKeywordUiStatus={getKeywordUiStatus}
            refreshRuns={(id) => void refreshRuns(id)} />
        )}
        {view === 'analyze' && (
          <AnalyzeView t={t} api={api}
            datasets={datasets} selectedDatasetId={selectedDatasetId} setSelectedDatasetId={setSelectedDatasetId}
            currentDataset={currentDataset} embeddingBuilds={embeddingBuilds}
            removeDataset={removeDataset} buildEmbeddingDataset={(id) => void buildEmbeddingDataset(id)} buildKeywordIndexDataset={(id) => void buildKeywordIndexDataset(id)}
            getEmbeddingStatusLabel={getEmbeddingStatusLabel} getEmbeddingUiStatus={getEmbeddingUiStatus} getKeywordUiStatus={getKeywordUiStatus}
            setSelectedRunId={setSelectedRunId} refreshRuns={(id) => void refreshRuns(id)} />
        )}
        {view === 'runs' && (
          <RunsView t={t} api={api}
            runs={runs} runsPage={runsPage} setRunsPage={setRunsPage}
            selectedRunId={selectedRunId} setSelectedRunId={setSelectedRunId}
            datasets={datasets} setDatasets={setDatasets}
            refreshRuns={(id) => void refreshRuns(id)} saveRunDetailAsDataset={saveRunDetailAsDataset} />
        )}
        {/* Monitor view hidden — agentic features paused for v0.3 focus */}
        {view === 'monitors' && (
          <MonitorsView t={t} sources={sources} monitors={monitors} focusEvents={focusEvents}
            sourceForm={sourceForm} setSourceForm={setSourceForm} monitorForm={monitorForm} setMonitorForm={setMonitorForm}
            monitorMessage={monitorMessage} createExternalSource={createExternalSource} createMonitorFromIntent={createMonitorFromIntent}
            monitorAction={monitorAction} focusEventAction={focusEventAction} refreshMonitors={() => void refreshMonitors()}
            setSelectedRunId={setSelectedRunId} setView={setView} />
        )}
        {view === 'settings' && (
          <SettingsPanel t={t} configForm={configForm} setConfigForm={setConfigForm}
            configMessage={configMessage} setConfigMessage={setConfigMessage} saveConfig={() => void saveConfig()} />
        )}
      </main>
    </div>
  );
}

import { useEffect, useMemo, useState } from 'react';
import { DATASETS_KEY, I18N, LOCALE_KEY, type Locale } from './i18n';

const DEFAULT_CHINESE_EMBEDDING_MODEL = 'BAAI/bge-small-zh-v1.5';
const EMBEDDING_MODEL_OPTIONS = [
  'BAAI/bge-small-zh-v1.5',
  'sentence-transformers/all-MiniLM-L6-v2',
] as const;
const EMBEDDING_TEMPLATE_OPTIONS = ['text-v1'] as const;

type PreviewData = {
  schema?: string[];
  rows?: Record<string, unknown>[];
  row_count?: number;
  truncated?: boolean;
};

type ImportOptions = {
  delimiter: string;
  columns: string;
  mappings: string;
  regexPattern: string;
  lineMode: 'split' | 'regex';
  jsonFormat: string;
};

type EmbeddingConfig = {
  enabled: boolean;
  textColumns: string[];
  provider: string;
  model: string;
  templateVersion: string;
  vectorColumn: string;
};

type EmbeddingDatasetRecord = {
  datasetPath: string;
  artifactUri: string;
  artifactId: string;
  runId: string;
  builtAt: string;
  schema: string[];
  rowCount?: number;
};

type DatasetRecord = {
  datasetId: string;
  name: string;
  sourceType: string;
  sourcePath: string;
  preview: PreviewData;
  schema: string[];
  kind: 'imported' | 'result';
  createdAt: string;
  description: string;
  sourceLabel: string;
  importOptions: ImportOptions;
  embeddingConfig: EmbeddingConfig;
  embeddingDataset: EmbeddingDatasetRecord | null;
};

type EmbeddingBuildState = {
  status: 'building' | 'failed';
  error?: string;
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
  preview: PreviewData;
};

type ImportPreviewPayload = {
  dataset: {
    name: string;
    source_type: string;
    source_path: string;
  };
  preview: PreviewData;
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
  delimiter: string;
  columns: string;
  mappings: string;
  regexPattern: string;
  lineMode: 'split' | 'regex';
  jsonFormat: string;
};

type FilterBuilderState = {
  column: string;
  operator: '=' | '!=' | '>' | '>=' | '<' | '<=';
  value: string;
};

type ImportFormState = {
  inputPath: string;
  inputType: string;
  delimiter: string;
  columns: string;
  regexPattern: string;
  datasetName: string;
  jsonFormat: string;
  embeddingEnabled: boolean;
  embeddingTextColumns: string;
  embeddingProvider: string;
  embeddingModel: string;
  embeddingTemplateVersion: string;
  embeddingVectorColumn: string;
};

type HybridSearchState = {
  queryText: string;
  textColumns: string;
  provider: string;
  model: string;
  templateVersion: string;
  topK: string;
  vectorColumn: string;
};

type LastResultState = {
  kind: 'sql' | 'hybrid';
  title: string;
  html: string;
};

const defaultImportOptions: ImportOptions = {
  delimiter: ',',
  columns: '',
  mappings: '',
  regexPattern: '',
  lineMode: 'split',
  jsonFormat: 'json_lines',
};

const defaultEmbeddingConfig: EmbeddingConfig = {
  enabled: false,
  textColumns: [],
  provider: 'minilm',
  model: DEFAULT_CHINESE_EMBEDDING_MODEL,
  templateVersion: 'text-v1',
  vectorColumn: 'embedding',
};

const defaultImportForm: ImportFormState = {
  inputPath: '',
  inputType: 'auto',
  delimiter: ',',
  columns: '',
  regexPattern: '',
  datasetName: '',
  jsonFormat: 'json_lines',
  embeddingEnabled: false,
  embeddingTextColumns: '',
  embeddingProvider: 'minilm',
  embeddingModel: DEFAULT_CHINESE_EMBEDDING_MODEL,
  embeddingTemplateVersion: 'text-v1',
  embeddingVectorColumn: 'embedding',
};

const defaultAnalyzeState: AnalyzeState = {
  inputPath: '',
  inputType: 'auto',
  tableName: 'input_table',
  preset: 'preview',
  query: 'SELECT * FROM input_table LIMIT 20',
  ...defaultImportOptions,
};

const defaultHybridSearchState: HybridSearchState = {
  queryText: '',
  textColumns: '',
  provider: 'minilm',
  model: DEFAULT_CHINESE_EMBEDDING_MODEL,
  templateVersion: 'text-v1',
  topK: '10',
  vectorColumn: 'embedding',
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

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function csvToList(value: string): string[] {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function listToCsv(values: string[]): string {
  return values.join(', ');
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function normalizePreview(value: unknown): PreviewData {
  const record = asRecord(value);
  const schema = Array.isArray(record?.schema)
    ? record.schema.filter((item): item is string => typeof item === 'string')
    : undefined;
  const rows = Array.isArray(record?.rows)
    ? record.rows.filter((item): item is Record<string, unknown> => !!item && typeof item === 'object' && !Array.isArray(item))
    : undefined;
  return {
    schema,
    rows,
    row_count:
      typeof record?.row_count === 'number'
        ? record.row_count
        : Array.isArray(rows)
          ? rows.length
          : undefined,
    truncated: typeof record?.truncated === 'boolean' ? record.truncated : undefined,
  };
}

function normalizeImportOptions(value: unknown): ImportOptions {
  const record = asRecord(value);
  return {
    delimiter: typeof record?.delimiter === 'string' ? record.delimiter : ',',
    columns: typeof record?.columns === 'string' ? record.columns : '',
    mappings: typeof record?.mappings === 'string' ? record.mappings : '',
    regexPattern: typeof record?.regexPattern === 'string' ? record.regexPattern : '',
    lineMode: record?.lineMode === 'regex' ? 'regex' : 'split',
    jsonFormat: typeof record?.jsonFormat === 'string' ? record.jsonFormat : 'json_lines',
  };
}

function normalizeEmbeddingConfig(value: unknown): EmbeddingConfig {
  const record = asRecord(value);
  const textColumns = Array.isArray(record?.textColumns)
    ? record.textColumns.filter((item): item is string => typeof item === 'string')
    : [];
  const provider = typeof record?.provider === 'string' ? record.provider : 'minilm';
  const model =
    typeof record?.model === 'string' && record.model
      ? record.model
      : provider === 'minilm'
        ? DEFAULT_CHINESE_EMBEDDING_MODEL
        : '';
  return {
    enabled: Boolean(record?.enabled),
    textColumns,
    provider,
    model,
    templateVersion: typeof record?.templateVersion === 'string' ? record.templateVersion : '',
    vectorColumn: typeof record?.vectorColumn === 'string' && record.vectorColumn
      ? record.vectorColumn
      : 'embedding',
  };
}

function normalizeEmbeddingDatasetRecord(value: unknown): EmbeddingDatasetRecord | null {
  const record = asRecord(value);
  if (!record) return null;
  if (typeof record.datasetPath !== 'string' || !record.datasetPath) return null;
  return {
    datasetPath: record.datasetPath,
    artifactUri: typeof record.artifactUri === 'string' ? record.artifactUri : '',
    artifactId: typeof record.artifactId === 'string' ? record.artifactId : '',
    runId: typeof record.runId === 'string' ? record.runId : '',
    builtAt: typeof record.builtAt === 'string' ? record.builtAt : new Date().toISOString(),
    schema: Array.isArray(record.schema)
      ? record.schema.filter((item): item is string => typeof item === 'string')
      : [],
    rowCount: typeof record.rowCount === 'number' ? record.rowCount : undefined,
  };
}

function createDatasetRecord(payload: {
  name: string;
  sourceType: string;
  sourcePath: string;
  preview: PreviewData;
  kind: DatasetRecord['kind'];
  description?: string;
  importOptions?: Partial<ImportOptions>;
  embeddingConfig?: Partial<EmbeddingConfig>;
  embeddingDataset?: EmbeddingDatasetRecord | null;
}): DatasetRecord {
  const importOptions = { ...defaultImportOptions, ...(payload.importOptions || {}) };
  const embeddingConfig = { ...defaultEmbeddingConfig, ...(payload.embeddingConfig || {}) };
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
    importOptions,
    embeddingConfig,
    embeddingDataset: payload.embeddingDataset || null,
  };
}

function normalizeDatasetRecord(value: unknown): DatasetRecord | null {
  const record = asRecord(value);
  if (!record) return null;
  if (typeof record.datasetId !== 'string') return null;
  if (typeof record.name !== 'string') return null;
  if (typeof record.sourceType !== 'string') return null;
  if (typeof record.sourcePath !== 'string') return null;
  const preview = normalizePreview(record.preview);
  return {
    datasetId: record.datasetId,
    name: record.name,
    sourceType: record.sourceType,
    sourcePath: record.sourcePath,
    preview,
    schema: Array.isArray(record.schema)
      ? record.schema.filter((item): item is string => typeof item === 'string')
      : preview.schema || [],
    kind: record.kind === 'result' ? 'result' : 'imported',
    createdAt: typeof record.createdAt === 'string' ? record.createdAt : new Date().toISOString(),
    description: typeof record.description === 'string' ? record.description : '',
    sourceLabel: typeof record.sourceLabel === 'string' ? record.sourceLabel : '',
    importOptions: normalizeImportOptions(record.importOptions),
    embeddingConfig: normalizeEmbeddingConfig(record.embeddingConfig),
    embeddingDataset: normalizeEmbeddingDatasetRecord(record.embeddingDataset),
  };
}

function loadDatasets(): DatasetRecord[] {
  try {
    const raw = window.localStorage.getItem(DATASETS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => normalizeDatasetRecord(item))
      .filter((item): item is DatasetRecord => item !== null);
  } catch {
    return [];
  }
}

function saveDatasets(datasets: DatasetRecord[]) {
  window.localStorage.setItem(DATASETS_KEY, JSON.stringify(datasets));
}

function highlightSql(sql: string): string {
  let html = escapeHtml(sql);
  html = html.replace(/(--.*)$/gm, '<span class="sql-token-comment">$1</span>');
  html = html.replace(/('(?:''|[^'])*')/g, '<span class="sql-token-string">$1</span>');
  html = html.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="sql-token-number">$1</span>');
  html = html.replace(
    /\b(SELECT|FROM|WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|INSERT\s+INTO|CREATE\s+TABLE|CREATE\s+SOURCE\s+TABLE|CREATE\s+SINK\s+TABLE|VALUES|AND|OR|AS|JOIN|LEFT|RIGHT|INNER|OUTER|ON|ASC|DESC|COUNT|SUM|AVG|MIN|MAX|HYBRID\s+SEARCH)\b/gi,
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

function quoteIdentifier(identifier: string) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    return identifier;
  }
  return `"${identifier.replaceAll('"', '""')}"`;
}

function quoteLiteral(value: string) {
  const trimmed = value.trim();
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return trimmed;
  }
  return `'${trimmed.replaceAll("'", "''")}'`;
}

function renderPreviewTable(preview: PreviewData, emptyText: string) {
  const rows = preview.rows || [];
  const schema = preview.schema || (rows[0] ? Object.keys(rows[0]) : []);
  if (!rows.length) {
    return `<div class="empty">${escapeHtml(emptyText)}</div>`;
  }
  const head = schema.map((column) => `<th>${escapeHtml(column)}</th>`).join('');
  const body = rows
    .map((row) => {
      const cells = schema
        .map((column) => `<td>${escapeHtml((row as Record<string, unknown>)[column] ?? '')}</td>`)
        .join('');
      return `<tr>${cells}</tr>`;
    })
    .join('');
  return `
    <div class="preview-wrap">
      <table>
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function toNumericScore(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function metricPrefersHigherScores(metric: string): boolean {
  return metric.trim().toLowerCase() === 'dot';
}

function renderHybridPreviewTable(
  preview: PreviewData,
  emptyText: string,
  metric: string,
  scoreColumn = 'vector_score'
) {
  const rows = [...(preview.rows || [])];
  const baseSchema = preview.schema || (rows[0] ? Object.keys(rows[0]) : []);
  if (!rows.length) {
    return `<div class="empty">${escapeHtml(emptyText)}</div>`;
  }

  const hasScore = baseSchema.includes(scoreColumn);
  const higherIsBetter = metricPrefersHigherScores(metric);
  const scoreDisplayLabel = higherIsBetter ? 'vector_score' : 'vector_distance';
  if (hasScore) {
    rows.sort((left, right) => {
      const leftScore = toNumericScore((left as Record<string, unknown>)[scoreColumn]);
      const rightScore = toNumericScore((right as Record<string, unknown>)[scoreColumn]);
      if (leftScore == null && rightScore == null) return 0;
      if (leftScore == null) return 1;
      if (rightScore == null) return -1;
      return higherIsBetter ? rightScore - leftScore : leftScore - rightScore;
    });
  }

  const schema = hasScore
    ? ['rank', scoreDisplayLabel, ...baseSchema.filter((column) => column !== scoreColumn)]
    : ['rank', ...baseSchema];

  const head = schema.map((column) => `<th>${escapeHtml(column)}</th>`).join('');
  const body = rows
    .map((row, index) => {
      const record = row as Record<string, unknown>;
      const cells = schema
        .map((column) => {
          if (column === 'rank') {
            return `<td>${index + 1}</td>`;
          }
          const value = column === scoreDisplayLabel ? record[scoreColumn] : record[column];
          if (column === scoreDisplayLabel) {
            const score = toNumericScore(value);
            return `<td>${score == null ? '' : score.toFixed(6)}</td>`;
          }
          return `<td>${escapeHtml(value ?? '')}</td>`;
        })
        .join('');
      return `<tr>${cells}</tr>`;
    })
    .join('');

  return `
    <div class="preview-wrap">
      <table class="hybrid-table">
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function extractHybridPreview(payload: unknown): PreviewData {
  const record = asRecord(payload);
  if (!record) return {};
  if (record.preview) return normalizePreview(record.preview);
  if (record.result) {
    const result = asRecord(record.result);
    if (result?.preview) return normalizePreview(result.preview);
    return normalizePreview(result);
  }
  return normalizePreview(record);
}

function extractHybridExplain(payload: unknown): string {
  const record = asRecord(payload);
  if (!record) return '';
  const explain = record.explain ?? asRecord(record.result)?.explain ?? record.strategy ?? record.debug;
  if (!explain) return '';
  if (typeof explain === 'string') return explain;
  try {
    return JSON.stringify(explain, null, 2);
  } catch {
    return String(explain);
  }
}

function extractEmbeddingDatasetRecord(payload: unknown): EmbeddingDatasetRecord {
  const record = asRecord(payload);
  const result = asRecord(record?.result);
  const run = asRecord(record?.run);
  const artifact = asRecord(record?.artifact);
  const datasetPath = typeof result?.dataset_path === 'string' ? result.dataset_path : '';
  if (!datasetPath) {
    throw new Error('embedding build response missing dataset_path');
  }
  return {
    datasetPath,
    artifactUri: typeof artifact?.uri === 'string' ? artifact.uri : '',
    artifactId: typeof artifact?.artifact_id === 'string' ? artifact.artifact_id : '',
    runId:
      typeof record?.run_id === 'string'
        ? record.run_id
        : typeof run?.run_id === 'string'
          ? run.run_id
          : '',
    builtAt: new Date().toISOString(),
    schema: Array.isArray(result?.schema)
      ? result.schema.filter((item): item is string => typeof item === 'string')
      : [],
    rowCount: typeof result?.row_count === 'number' ? result.row_count : undefined,
  };
}

function embeddingFormToConfig(form: ImportFormState): EmbeddingConfig {
  return {
    enabled: form.embeddingEnabled,
    textColumns: csvToList(form.embeddingTextColumns),
    provider: form.embeddingProvider.trim(),
    model: form.embeddingModel.trim(),
    templateVersion: form.embeddingTemplateVersion.trim(),
    vectorColumn: form.embeddingVectorColumn.trim() || 'embedding',
  };
}

function createEmbeddingBuildPayload(dataset: DatasetRecord): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    input_path: dataset.sourcePath,
    input_type: dataset.sourceType,
    delimiter: dataset.importOptions.delimiter,
    json_format: dataset.importOptions.jsonFormat,
    line_mode: dataset.importOptions.lineMode,
    text_columns: dataset.embeddingConfig.textColumns,
    provider: dataset.embeddingConfig.provider || undefined,
    model: dataset.embeddingConfig.model || undefined,
    template_version: dataset.embeddingConfig.templateVersion || undefined,
    vector_column: dataset.embeddingConfig.vectorColumn || 'embedding',
    run_name: `embedding-${dataset.name}`,
    description: `Embedding build for dataset ${dataset.name}`,
  };
  if (dataset.importOptions.columns) payload.columns = dataset.importOptions.columns;
  if (dataset.importOptions.mappings) payload.mappings = dataset.importOptions.mappings;
  if (dataset.importOptions.regexPattern) payload.regex_pattern = dataset.importOptions.regexPattern;
  return payload;
}

function isDatasetBackedByRun(dataset: DatasetRecord, runDir: string): boolean {
  return dataset.kind === 'result' && dataset.sourcePath.startsWith(`${runDir}/`);
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
  const [importForm, setImportForm] = useState<ImportFormState>(defaultImportForm);
  const [importMessage, setImportMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [datasetMessage, setDatasetMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [runsPage, setRunsPage] = useState(1);
  const [runMessage, setRunMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedRunDetail, setSelectedRunDetail] = useState<RunDetailPayload | null>(null);
  const [analysisState, setAnalysisState] = useState<AnalyzeState>(defaultAnalyzeState);
  const [filterBuilder, setFilterBuilder] = useState<FilterBuilderState>({
    column: '',
    operator: '=',
    value: '',
  });
  const [analysisResultHtml, setAnalysisResultHtml] = useState('<div class="empty">No run yet.</div>');
  const [hybridSearch, setHybridSearch] = useState<HybridSearchState>(defaultHybridSearchState);
  const [hybridResultHtml, setHybridResultHtml] = useState('<div class="empty">No hybrid search yet.</div>');
  const [hybridMessage, setHybridMessage] = useState<{ kind: 'info' | 'error'; text: string } | null>(null);
  const [lastResult, setLastResult] = useState<LastResultState>({
    kind: 'sql',
    title: 'SQL',
    html: '<div class="empty">No run yet.</div>',
  });
  const [sqlUnderstandingExpanded, setSqlUnderstandingExpanded] = useState(false);
  const [savingDataset, setSavingDataset] = useState(false);
  const [embeddingBuilds, setEmbeddingBuilds] = useState<Record<string, EmbeddingBuildState>>({});

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
    if (selectedDatasetId && !datasets.some((dataset) => dataset.datasetId === selectedDatasetId)) {
      setSelectedDatasetId(datasets[0]?.datasetId ?? null);
      return;
    }
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
  const schemaColumns = currentDataset?.schema || [];

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
  }

  async function bootstrap() {
    try {
      const info = await window.velariaShell.getServiceInfo();
      setServiceInfo(info);
      const health = await fetch(`${info.baseUrl}/health`).then((r) => r.json());
      setServiceStatus(t('status_ready_on', { port: health.port }));
      setServiceMeta(t('status_packaged', { packaged: info.packaged, version: health.version }));
      const runsPayload = await fetch(`${info.baseUrl}/api/v1/runs?limit=100`).then((r) => r.json());
      setRuns(runsPayload.runs || []);
      if (runsPayload.runs?.[0]) {
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
    if (!currentDataset) return;
    setAnalysisState((prev) => ({
      ...prev,
      inputPath: currentDataset.sourcePath,
      inputType: currentDataset.sourceType,
      delimiter: currentDataset.importOptions.delimiter,
      columns: currentDataset.importOptions.columns,
      mappings: currentDataset.importOptions.mappings,
      regexPattern: currentDataset.importOptions.regexPattern,
      lineMode: currentDataset.importOptions.lineMode,
      jsonFormat: currentDataset.importOptions.jsonFormat,
    }));
    setHybridSearch((prev) => ({
      ...prev,
      textColumns: listToCsv(currentDataset.embeddingConfig.textColumns),
      provider: currentDataset.embeddingConfig.provider,
      model: currentDataset.embeddingConfig.model,
      templateVersion: currentDataset.embeddingConfig.templateVersion,
      vectorColumn: currentDataset.embeddingConfig.vectorColumn || 'embedding',
    }));
    setFilterBuilder((prev) => ({
      ...prev,
      column: currentDataset.schema.includes(prev.column) ? prev.column : '',
    }));
  }, [currentDataset]);

  async function refreshRuns(nextSelectedRunId: string | null = selectedRunId) {
    const payload = await api('/api/v1/runs?limit=100');
    const nextRuns = payload.runs || [];
    setRuns(nextRuns);
    setRunsPage(1);
    const fallbackRunId =
      nextSelectedRunId && nextRuns.some((run: RunSummary) => run.run_id === nextSelectedRunId)
        ? nextSelectedRunId
        : nextRuns[0]?.run_id ?? null;
    setSelectedRunId(fallbackRunId);
    if (!fallbackRunId) {
      setSelectedRunDetail(null);
    }
  }

  async function previewImport(event: React.FormEvent) {
    event.preventDefault();
    setImportMessage({ kind: 'info', text: t('loading_preview') });
    setDatasetMessage(null);
    try {
      const payload: Record<string, unknown> = {
        input_path: importForm.inputPath.trim(),
        input_type: importForm.inputType,
        delimiter: importForm.delimiter,
        dataset_name: importForm.datasetName.trim(),
      };
      const columns = importForm.columns.trim();
      const regexPattern = importForm.regexPattern.trim();
      if (importForm.inputType === 'json' && columns) payload.columns = columns;
      if (importForm.inputType === 'line') {
        payload.mappings = columns;
        payload.regex_pattern = regexPattern;
        payload.line_mode = regexPattern ? 'regex' : 'split';
      }
      if (importForm.embeddingEnabled) {
        payload.embedding_config = embeddingFormToConfig(importForm);
      }
      const result = (await api('/api/v1/import/preview', {
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

  function getEmbeddingUiStatus(dataset: DatasetRecord | null): 'disabled' | 'configured' | 'building' | 'failed' | 'ready' {
    if (!dataset) return 'disabled';
    const transient = embeddingBuilds[dataset.datasetId];
    if (dataset.embeddingDataset?.datasetPath) return 'ready';
    if (transient?.status === 'building') return 'building';
    if (transient?.status === 'failed') return 'failed';
    if (dataset.embeddingConfig.enabled) return 'configured';
    return 'disabled';
  }

  function getEmbeddingStatusLabel(dataset: DatasetRecord | null) {
    const status = getEmbeddingUiStatus(dataset);
    if (status === 'ready') return t('embedding_ready');
    if (status === 'building') return t('embedding_building_short');
    if (status === 'failed') return t('embedding_build_failed_short');
    if (status === 'configured') return t('embedding_configured_only');
    return t('embedding_disabled_short');
  }

  async function buildEmbeddingDataset(datasetId: string, datasetSnapshot?: DatasetRecord) {
    const target = datasetSnapshot || datasets.find((dataset) => dataset.datasetId === datasetId);
    if (
      !target ||
      !target.embeddingConfig.enabled ||
      target.embeddingDataset?.datasetPath ||
      embeddingBuilds[datasetId]?.status === 'building'
    ) {
      return;
    }

    setEmbeddingBuilds((current) => ({
      ...current,
      [datasetId]: { status: 'building' },
    }));
    setDatasetMessage({
      kind: 'info',
      text: t('embedding_build_started', { name: target.name }),
    });

    try {
      const buildResult = await api('/api/v1/runs/embedding-build', {
        method: 'POST',
        body: JSON.stringify(createEmbeddingBuildPayload(target)),
      });
      const embeddingDataset = extractEmbeddingDatasetRecord(buildResult);
      const result = asRecord(buildResult.result);

      setDatasets((current) =>
        current.map((dataset) => {
          if (dataset.datasetId !== datasetId) return dataset;
          return {
            ...dataset,
            embeddingDataset,
            embeddingConfig: {
              ...dataset.embeddingConfig,
              provider:
                typeof result?.provider === 'string' ? result.provider : dataset.embeddingConfig.provider,
              model: typeof result?.model === 'string' ? result.model : dataset.embeddingConfig.model,
              templateVersion:
                typeof result?.template_version === 'string'
                  ? result.template_version
                  : dataset.embeddingConfig.templateVersion,
              vectorColumn:
                typeof result?.vector_column === 'string'
                  ? result.vector_column
                  : dataset.embeddingConfig.vectorColumn,
            },
          };
        })
      );
      setEmbeddingBuilds((current) => {
        const next = { ...current };
        delete next[datasetId];
        return next;
      });
      setDatasetMessage({
        kind: 'info',
        text: t('embedding_build_succeeded', { name: target.name }),
      });
      if (selectedDatasetId === datasetId) {
        setHybridMessage({ kind: 'info', text: t('hybrid_ready_after_embedding_build') });
      }
    } catch (error) {
      const message = String(error);
      setEmbeddingBuilds((current) => ({
        ...current,
        [datasetId]: { status: 'failed', error: message },
      }));
      setDatasetMessage({
        kind: 'error',
        text: t('embedding_build_failed', { error: message }),
      });
      if (selectedDatasetId === datasetId) {
        setHybridMessage({ kind: 'error', text: t('embedding_build_failed', { error: message }) });
      }
    }
  }

  async function saveImportDataset() {
    if (!pendingImport) return;
    const embeddingConfig = embeddingFormToConfig(importForm);
    const importOptions = {
      delimiter: importForm.delimiter,
      columns: importForm.inputType === 'json' ? importForm.columns.trim() : '',
      mappings: importForm.inputType === 'line' ? importForm.columns.trim() : '',
      regexPattern: importForm.regexPattern.trim(),
      lineMode: importForm.regexPattern.trim() ? 'regex' : 'split',
      jsonFormat: importForm.jsonFormat,
    };

    setSavingDataset(true);
    setDatasetMessage(null);
    setImportMessage({
      kind: 'info',
      text: embeddingConfig.enabled ? t('embedding_building') : t('dataset_saving'),
    });
    try {
      const record = createDatasetRecord({
        name: pendingImport.dataset.name,
        sourceType: pendingImport.dataset.source_type,
        sourcePath: pendingImport.dataset.source_path,
        preview: pendingImport.preview,
        kind: 'imported',
        importOptions,
        embeddingConfig,
        embeddingDataset: null,
      });
      setDatasets((current) => [record, ...current]);
      setSelectedDatasetId(record.datasetId);
      setPendingImport(null);
      setImportMessage(null);
      setDatasetMessage({
        kind: 'info',
        text: embeddingConfig.enabled
          ? t('dataset_saved_with_embedding_message')
          : t('dataset_saved_message'),
      });
      setView('analyze');
      if (embeddingConfig.enabled) {
        void buildEmbeddingDataset(record.datasetId, record);
      }
    } catch (error) {
      setImportMessage({
        kind: 'error',
        text: embeddingConfig.enabled
          ? t('embedding_build_failed', { error: String(error) })
          : t('dataset_save_failed', { error: String(error) }),
      });
    } finally {
      setSavingDataset(false);
    }
  }

  function removeDataset(datasetId: string) {
    const target = datasets.find((dataset) => dataset.datasetId === datasetId);
    if (!target) return;
    const confirmed = window.confirm(t('confirm_remove_dataset', { name: target.name }));
    if (!confirmed) return;
    setDatasets((current) => current.filter((dataset) => dataset.datasetId !== datasetId));
    setEmbeddingBuilds((current) => {
      const next = { ...current };
      delete next[datasetId];
      return next;
    });
    setDatasetMessage({
      kind: 'info',
      text: t('dataset_removed_message', { name: target.name }),
    });
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

  function applyFilterBuilder() {
    if (!filterBuilder.column || !filterBuilder.value.trim()) return;
    const table = analysisState.tableName || 'input_table';
    const whereClause = `SELECT * FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(
      filterBuilder.column
    )} ${filterBuilder.operator} ${quoteLiteral(filterBuilder.value)} LIMIT 50`;
    setAnalysisState((current) => ({
      ...current,
      query: whereClause,
      preset: 'filter',
    }));
  }

  async function runAnalysis() {
    setAnalysisResultHtml(`<div class="empty">${escapeHtml(t('running_analysis'))}</div>`);
    try {
      const payload = {
        input_path: analysisState.inputPath,
        input_type: analysisState.inputType,
        delimiter: analysisState.delimiter,
        line_mode: analysisState.lineMode,
        regex_pattern: analysisState.regexPattern || undefined,
        mappings: analysisState.mappings || undefined,
        columns: analysisState.columns || undefined,
        json_format: analysisState.jsonFormat,
        table: analysisState.tableName,
        query: analysisState.query,
        run_name: `analysis-${new Date().toISOString()}`,
        description: 'Desktop workbench analysis run',
      };
      const runPayload = await api('/api/v1/runs/file-sql', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      setSelectedRunId(runPayload.run_id);
      const runDetail = (await api(`/api/v1/runs/${encodeURIComponent(runPayload.run_id)}/result?limit=20`)) as RunDetailPayload;
      setSelectedRunDetail(runDetail);
      setAnalysisResultHtml(`
        <div class="meta">
          <span>${escapeHtml(runPayload.run.status)}</span>
          <span>${escapeHtml(runPayload.run.run_id)}</span>
          <span>${escapeHtml(t('rows_count', { count: runDetail.preview.row_count || '—' }))}</span>
        </div>
        ${renderPreviewTable(runDetail.preview, t('no_preview_rows'))}
      `);
      setLastResult({
        kind: 'sql',
        title: t('run_detail'),
        html: `
          <div class="meta">
            <span>${escapeHtml(runPayload.run.status)}</span>
            <span>${escapeHtml(runPayload.run.run_id)}</span>
            <span>${escapeHtml(t('rows_count', { count: runDetail.preview.row_count || '—' }))}</span>
          </div>
          ${renderPreviewTable(runDetail.preview, t('no_preview_rows'))}
        `,
      });
      await refreshRuns(runPayload.run_id);
    } catch (error) {
      setSelectedRunDetail(null);
      const html = `<div class="empty">${escapeHtml(t('run_failed', { error: String(error) }))}</div>`;
      setAnalysisResultHtml(html);
      setLastResult({
        kind: 'sql',
        title: t('run_detail'),
        html,
      });
    }
  }

  async function runHybridSearch() {
    if (!currentDataset) {
      const html = `<div class="empty">${escapeHtml(t('no_dataset_for_analyze'))}</div>`;
      setHybridResultHtml(html);
      setLastResult({
        kind: 'hybrid',
        title: t('hybrid_results'),
        html,
      });
      return;
    }
    const datasetPath = currentDataset.embeddingDataset?.datasetPath || '';
    if (!hybridSearchReady || !datasetPath) {
      const message = t('hybrid_requires_embedding_dataset');
      const html = `<div class="empty">${escapeHtml(message)}</div>`;
      setHybridResultHtml(html);
      setLastResult({
        kind: 'hybrid',
        title: t('hybrid_results'),
        html,
      });
      setHybridMessage({ kind: 'error', text: message });
      return;
    }
    const queryText = hybridSearch.queryText.trim();
    if (!queryText) {
      const message = t('hybrid_query_required');
      const html = `<div class="empty">${escapeHtml(message)}</div>`;
      setHybridResultHtml(html);
      setLastResult({
        kind: 'hybrid',
        title: t('hybrid_results'),
        html,
      });
      setHybridMessage({ kind: 'error', text: message });
      return;
    }
    setHybridMessage({ kind: 'info', text: t('hybrid_loading') });
    setHybridResultHtml(`<div class="empty">${escapeHtml(t('hybrid_loading'))}</div>`);
    try {
      const whereSql = sqlStructure.where?.trim() || '';
      const payload = {
        dataset_path: datasetPath,
        query_text: queryText,
        provider: hybridSearch.provider.trim() || undefined,
        model: hybridSearch.model.trim() || undefined,
        template_version: hybridSearch.templateVersion.trim() || undefined,
        top_k: Number(hybridSearch.topK) || 10,
        where_sql: whereSql || undefined,
        vector_column: hybridSearch.vectorColumn.trim() || 'embedding',
      };
      const result = await api('/api/v1/runs/hybrid-search', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      const preview = extractHybridPreview(result);
      const explain = extractHybridExplain(result);
      const metric = String((asRecord(result.result)?.metric as string | undefined) || 'cosine');
      const effectiveWhere = String((asRecord(result.result)?.where_sql as string | undefined) || whereSql);
      const html = `
        <div class="meta">
          <span>${escapeHtml(t('rows_count', { count: preview.row_count ?? preview.rows?.length ?? '—' }))}</span>
          <span>${escapeHtml(metric)}</span>
          <span>${escapeHtml(effectiveWhere ? t('hybrid_filter_applied') : t('hybrid_filter_none'))}</span>
          <span>${escapeHtml(hybridSearch.vectorColumn.trim() || 'embedding')}</span>
          <span>${escapeHtml(hybridSearch.provider.trim() || '—')}</span>
        </div>
        ${effectiveWhere ? `<div class="helper">${escapeHtml(t('hybrid_filter_sql', { where: effectiveWhere }))}</div>` : ''}
        ${explain ? `<pre class="explain-block">${escapeHtml(explain)}</pre>` : ''}
        ${renderHybridPreviewTable(preview, t('hybrid_no_result'), metric)}
      `;
      setHybridResultHtml(html);
      setLastResult({
        kind: 'hybrid',
        title: t('hybrid_results'),
        html,
      });
      setHybridMessage(null);
    } catch (error) {
      const html = `<div class="empty">${escapeHtml(t('hybrid_failed', { error: String(error) }))}</div>`;
      setHybridResultHtml(html);
      setLastResult({
        kind: 'hybrid',
        title: t('hybrid_results'),
        html,
      });
      setHybridMessage({ kind: 'error', text: t('hybrid_failed', { error: String(error) }) });
    }
  }

  async function loadRunDetail(runId: string) {
    try {
      const payload = (await api(`/api/v1/runs/${encodeURIComponent(runId)}/result?limit=20`)) as RunDetailPayload;
      setSelectedRunDetail(payload);
      setRunMessage(null);
    } catch (error) {
      setSelectedRunDetail(null);
      setRunMessage({ kind: 'error', text: t('run_detail_failed', { error: String(error) }) });
    }
  }

  useEffect(() => {
    if (selectedRunId && view === 'runs') {
      void loadRunDetail(selectedRunId);
    }
  }, [selectedRunId, view]);

  async function deleteRun(runId: string) {
    const target = runs.find((run) => run.run_id === runId);
    const confirmed = window.confirm(
      t('confirm_delete_run', { name: target?.run_name || runId })
    );
    if (!confirmed) return;
    try {
      const runPayload = await api(`/api/v1/runs/${encodeURIComponent(runId)}`);
      const runDir = String(runPayload.run?.run_dir || '');
      const removedDatasetCount = runDir
        ? datasets.filter((dataset) => isDatasetBackedByRun(dataset, runDir)).length
        : 0;
      await api(`/api/v1/runs/${encodeURIComponent(runId)}`, { method: 'DELETE' });
      if (runDir) {
        setDatasets((current) => current.filter((dataset) => !isDatasetBackedByRun(dataset, runDir)));
      }
      await refreshRuns(selectedRunId === runId ? null : selectedRunId);
      if (selectedRunId === runId) {
        setSelectedRunDetail(null);
      }
      setRunMessage({
        kind: 'info',
        text: t('run_deleted_message', { runId, count: removedDatasetCount }),
      });
    } catch (error) {
      setRunMessage({ kind: 'error', text: t('run_delete_failed', { error: String(error) }) });
    }
  }

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

  function saveRunDetailAsDataset(nextView: ViewKey = 'data') {
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
      embeddingConfig: defaultEmbeddingConfig,
    });
    setDatasets((current) => [record, ...current]);
    setSelectedDatasetId(record.datasetId);
    setDatasetMessage({ kind: 'info', text: t('dataset_saved_from_run') });
    setView(nextView);
  }

  const datasetCards = visibleDatasets.map((dataset) => {
    const embeddingStatus = getEmbeddingStatusLabel(dataset);
    return (
      <div
        key={dataset.datasetId}
        className={`analyze-dataset-item ${dataset.datasetId === selectedDatasetId ? 'active' : ''}`}
        onClick={() => setSelectedDatasetId(dataset.datasetId)}
      >
        <div className="item-head">
          <h4>{dataset.name}</h4>
          <button
            type="button"
            className="ghost danger-button"
            onClick={(event) => {
              event.stopPropagation();
              removeDataset(dataset.datasetId);
            }}
          >
            {t('remove_dataset')}
          </button>
        </div>
        <div className="meta">
          <span>{dataset.sourceType}</span>
          <span>{t('rows_count', { count: dataset.preview?.row_count ?? '—' })}</span>
          <span>{embeddingStatus}</span>
        </div>
      </div>
    );
  });

  const sqlCards = [
    ['sql_part_select', sqlStructure.select],
    ['sql_part_from', sqlStructure.from],
    ['sql_part_where', sqlStructure.where],
    ['sql_part_group_by', sqlStructure.groupBy],
    ['sql_part_order_by', sqlStructure.orderBy],
    ['sql_part_limit', sqlStructure.limit],
  ] as const;

  const currentEmbeddingStatus = getEmbeddingUiStatus(currentDataset);
  const currentEmbeddingBuild = currentDataset ? embeddingBuilds[currentDataset.datasetId] : null;
  const datasetKindLabel = currentDataset
    ? t(currentDataset.kind === 'result' ? 'kind_result' : 'kind_imported')
    : '—';
  const currentEmbeddingSummary = currentDataset?.embeddingDataset?.datasetPath
    ? `${currentDataset.embeddingConfig.textColumns.join(', ') || '—'} · ${currentDataset.embeddingConfig.provider || '—'} · ${currentDataset.embeddingConfig.model || '—'}`
    : currentEmbeddingStatus === 'building'
      ? `${t('embedding_building_short')} · ${currentDataset?.embeddingConfig.textColumns.join(', ') || '—'}`
      : currentEmbeddingStatus === 'failed'
        ? `${t('embedding_build_failed_short')} · ${currentDataset?.embeddingConfig.textColumns.join(', ') || '—'}`
        : currentDataset?.embeddingConfig.enabled
      ? `${t('embedding_configured_only')} · ${currentDataset.embeddingConfig.textColumns.join(', ') || '—'}`
      : t('embedding_disabled');
  const currentEmbeddingDatasetValue = currentDataset?.embeddingDataset?.datasetPath || '';
  const currentEmbeddingDatasetPath =
    currentEmbeddingStatus === 'ready'
      ? currentEmbeddingDatasetValue
      : currentEmbeddingStatus === 'building'
        ? t('embedding_dataset_building')
        : currentEmbeddingStatus === 'failed'
          ? t('embedding_dataset_failed')
          : t('embedding_dataset_missing');
  const hybridSearchReady = currentEmbeddingStatus === 'ready';
  const datasetListEmptyMessage =
    view === 'analyze' ? t('no_dataset_for_analyze') : t('no_datasets_available');
  const datasetCardEmptyMessage =
    currentDataset == null ? t('dataset_detail_empty') : null;
  const showBuildEmbeddingAction =
    currentDataset != null &&
    (currentEmbeddingStatus === 'configured' || currentEmbeddingStatus === 'failed');
  const isBuildingEmbedding = currentEmbeddingStatus === 'building';

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
                        <div className="item-head">
                          <h4>{dataset.name}</h4>
                          <span className="badge">{getEmbeddingStatusLabel(dataset)}</span>
                        </div>
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
                      <input
                        value={importForm.inputPath}
                        onChange={(event) =>
                          setImportForm((current) => ({ ...current, inputPath: event.target.value }))
                        }
                        placeholder={t('input_placeholder')}
                        required
                      />
                    </label>
                    <div className="actions">
                      <button
                        type="button"
                        className="ghost"
                        onClick={async () => {
                          const path = await window.velariaShell.pickFile();
                          if (path) {
                            setImportForm((current) => ({ ...current, inputPath: path }));
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
                        <select
                          value={importForm.inputType}
                          onChange={(event) =>
                            setImportForm((current) => ({ ...current, inputType: event.target.value }))
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
                      <label>
                        <span>{t('delimiter_label')}</span>
                        <input
                          value={importForm.delimiter}
                          onChange={(event) =>
                            setImportForm((current) => ({ ...current, delimiter: event.target.value }))
                          }
                        />
                      </label>
                    </div>
                    <div className="field-grid">
                      <label>
                        <span>{t('columns_or_mappings')}</span>
                        <input
                          value={importForm.columns}
                          onChange={(event) =>
                            setImportForm((current) => ({ ...current, columns: event.target.value }))
                          }
                          placeholder={t('columns_placeholder')}
                        />
                      </label>
                      <label>
                        <span>{t('regex_pattern')}</span>
                        <input
                          value={importForm.regexPattern}
                          onChange={(event) =>
                            setImportForm((current) => ({ ...current, regexPattern: event.target.value }))
                          }
                          placeholder={t('regex_placeholder')}
                        />
                      </label>
                    </div>
                    <label>
                      <span>{t('dataset_name')}</span>
                      <input
                        value={importForm.datasetName}
                        onChange={(event) =>
                          setImportForm((current) => ({ ...current, datasetName: event.target.value }))
                        }
                        placeholder={t('dataset_name_placeholder')}
                      />
                    </label>

                    <div className="subsection-card">
                      <div className="subsection-head">
                        <div>
                          <h3>{t('import_embedding_title')}</h3>
                          <div className="helper">{t('import_embedding_hint')}</div>
                        </div>
                        <label className="toggle-row">
                          <input
                            type="checkbox"
                            checked={importForm.embeddingEnabled}
                            onChange={(event) =>
                              setImportForm((current) => ({
                                ...current,
                                embeddingEnabled: event.target.checked,
                              }))
                            }
                          />
                          <span>{t('embedding_enable')}</span>
                        </label>
                      </div>
                      <div className="field-grid">
                        <label>
                          <span>{t('embedding_text_columns')}</span>
                          <input
                            value={importForm.embeddingTextColumns}
                            onChange={(event) =>
                              setImportForm((current) => ({
                                ...current,
                                embeddingTextColumns: event.target.value,
                              }))
                            }
                            placeholder={t('embedding_columns_placeholder')}
                            disabled={!importForm.embeddingEnabled}
                          />
                        </label>
                        <label>
                          <span>{t('embedding_provider')}</span>
                          <input
                            value={importForm.embeddingProvider}
                            onChange={(event) =>
                              setImportForm((current) => ({
                                ...current,
                                embeddingProvider: event.target.value,
                              }))
                            }
                            placeholder={t('embedding_provider_placeholder')}
                            disabled={!importForm.embeddingEnabled}
                          />
                        </label>
                      </div>
                      <div className="field-grid">
                        <label>
                          <span>{t('embedding_model')}</span>
                          <select
                            value={importForm.embeddingModel}
                            onChange={(event) =>
                              setImportForm((current) => ({
                                ...current,
                                embeddingModel: event.target.value,
                              }))
                            }
                            disabled={!importForm.embeddingEnabled}
                          >
                            {EMBEDDING_MODEL_OPTIONS.map((model) => (
                              <option key={model} value={model}>
                                {model}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          <span>{t('embedding_template_version')}</span>
                          <select
                            value={importForm.embeddingTemplateVersion}
                            onChange={(event) =>
                              setImportForm((current) => ({
                                ...current,
                                embeddingTemplateVersion: event.target.value,
                              }))
                            }
                            disabled={!importForm.embeddingEnabled}
                          >
                            {EMBEDDING_TEMPLATE_OPTIONS.map((template) => (
                              <option key={template} value={template}>
                                {template}
                              </option>
                            ))}
                          </select>
                        </label>
                      </div>
                      <label>
                        <span>{t('embedding_vector_column')}</span>
                        <input
                          value={importForm.embeddingVectorColumn}
                          onChange={(event) =>
                            setImportForm((current) => ({
                              ...current,
                              embeddingVectorColumn: event.target.value,
                            }))
                          }
                          placeholder={t('embedding_vector_placeholder')}
                          disabled={!importForm.embeddingEnabled}
                        />
                      </label>
                    </div>
                  </form>
                  <div className="notice">{t('import_hint')}</div>
                  <div className="notice subtle">{t('dataset_delete_hint')}</div>
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
                          <span>
                            {importForm.embeddingEnabled
                              ? t('embedding_configured_only')
                              : t('embedding_disabled_short')}
                          </span>
                        </div>
                        <div
                          dangerouslySetInnerHTML={{
                            __html: renderPreviewTable(pendingImport.preview, t('no_preview_rows')),
                          }}
                        />
                      </>
                    ) : (
                      <div className="empty">{t('no_preview_rows')}</div>
                    )}
                  </div>
                  <div className="actions">
                    <button onClick={saveImportDataset} disabled={!pendingImport || savingDataset}>
                      {t('save_as_dataset')}
                    </button>
                  </div>
                </div>
              </section>

              <section className="panel">
                <div className="panel-head">
                  <h2>{t('datasets')}</h2>
                </div>
                <div className="panel-body stack">
                  {datasetMessage && (
                    <div className={`notice ${datasetMessage.kind === 'error' ? 'error' : ''}`}>
                      {datasetMessage.text}
                    </div>
                  )}
                  <div className="list">
                    {datasets.map((dataset) => (
                      <div
                        key={dataset.datasetId}
                        className={`list-item ${dataset.datasetId === selectedDatasetId ? 'active' : ''}`}
                        onClick={() => setSelectedDatasetId(dataset.datasetId)}
                      >
                        <div className="item-head">
                          <h4>{dataset.name}</h4>
                          <div className="inline-actions">
                            <span className="badge">{getEmbeddingStatusLabel(dataset)}</span>
                            <button
                              type="button"
                              className="ghost danger-button"
                              onClick={(event) => {
                                event.stopPropagation();
                                removeDataset(dataset.datasetId);
                              }}
                            >
                              {t('remove_dataset')}
                            </button>
                          </div>
                        </div>
                        <div className="meta">
                          <span>{dataset.sourceType}</span>
                          <span>{t('rows_count', { count: dataset.preview?.row_count ?? '—' })}</span>
                          <span>{t(dataset.kind === 'result' ? 'kind_result' : 'kind_imported')}</span>
                        </div>
                      </div>
                    ))}
                    {!datasets.length && <div className="empty">{t('no_datasets_available')}</div>}
                  </div>

                  {currentDataset && (
                    <div className="result-box">
                      <div className="item-head">
                        <h4>{currentDataset.name}</h4>
                        <div className="inline-actions">
                          <button className="ghost" type="button" onClick={exportCurrentDataset}>
                            {t('export_file')}
                          </button>
                          <button
                            className="ghost"
                            type="button"
                            onClick={() => {
                              setView('analyze');
                            }}
                          >
                            {t('analyze_action')}
                          </button>
                          <button
                            className="ghost danger-button"
                            type="button"
                            onClick={() => removeDataset(currentDataset.datasetId)}
                          >
                            {t('remove_dataset')}
                          </button>
                        </div>
                      </div>
                      <div className="compact-grid">
                        <div className="compact-card">
                          <strong>{t('field_source')}</strong>
                          <div className="mono">{currentDataset.sourcePath}</div>
                        </div>
                        <div className="compact-card">
                          <strong>{t('field_schema')}</strong>
                          <div>{currentDataset.schema.join(', ') || '—'}</div>
                        </div>
                        <div className="compact-card">
                          <strong>{t('field_embedding')}</strong>
                          <div>{currentEmbeddingSummary}</div>
                        </div>
                        <div className="compact-card">
                          <strong>{t('field_kind')}</strong>
                          <div>{datasetKindLabel}</div>
                        </div>
                      </div>
                      {showBuildEmbeddingAction && (
                        <div className="notice">
                          <div>{t('embedding_build_needed_hint')}</div>
                          <div className="actions notice-actions">
                            <button
                              type="button"
                              className="ghost"
                              onClick={() => {
                                void buildEmbeddingDataset(currentDataset.datasetId);
                              }}
                            >
                              {t(
                                currentEmbeddingStatus === 'failed'
                                  ? 'embedding_build_retry'
                                  : 'embedding_build_action'
                              )}
                            </button>
                          </div>
                        </div>
                      )}
                      {isBuildingEmbedding && (
                        <div className="notice">{t('embedding_building_hint')}</div>
                      )}
                      {currentEmbeddingStatus === 'failed' && currentEmbeddingBuild?.error && (
                        <div className="notice error">
                          {t('embedding_build_failed', { error: currentEmbeddingBuild.error })}
                        </div>
                      )}
                      <div
                        dangerouslySetInnerHTML={{
                          __html: renderPreviewTable(currentDataset.preview, t('no_preview_rows')),
                        }}
                      />
                    </div>
                  )}
                </div>
              </section>
            </div>
          </section>
        )}

        {view === 'analyze' && (
          <section className="section active">
            <div className="stack">
              <section className="panel">
                <div className="panel-head">
                  <h2>{t('analyze_workspace')}</h2>
                  <div className="meta">
                    <span>{t('meta_sql_mode')}</span>
                    <span>{t('meta_run_tracked')}</span>
                  </div>
                </div>
                <div className="panel-body stack">
                  {datasets.length ? (
                    <div className="summary-band analyze-summary-band">
                      <div className="subsection-card">
                        <div className="subsection-head">
                          <h3>{t('datasets')}</h3>
                        </div>
                        <div className="stack">
                          <input
                            className="analyze-search"
                            placeholder={t('analysis_dataset_search')}
                            value={datasetSearch}
                            onChange={(event) => setDatasetSearch(event.target.value)}
                          />
                          <div className="analyze-dataset-list" style={{ maxHeight: 220, overflow: 'auto' }}>
                            {datasetCards}
                            {!datasetCards.length && <div className="empty">{datasetListEmptyMessage}</div>}
                          </div>
                        </div>
                      </div>

                      <div className="subsection-card">
                        <div className="subsection-head">
                          <h3>{t('dataset_context')}</h3>
                          <div className="actions">
                            <button
                              type="button"
                              className="ghost"
                              onClick={() => setSqlUnderstandingExpanded((value) => !value)}
                              disabled={!currentDataset}
                            >
                              {t(sqlUnderstandingExpanded ? 'collapse' : 'expand')}
                            </button>
                          </div>
                        </div>
                        <div className="stack">
                          <label>
                            <span>{t('dataset_label')}</span>
                            <select
                              value={selectedDatasetId || ''}
                              onChange={(event) => setSelectedDatasetId(event.target.value)}
                              disabled={!visibleDatasets.length}
                            >
                              {visibleDatasets.length ? (
                                visibleDatasets.map((dataset) => (
                                  <option key={dataset.datasetId} value={dataset.datasetId}>
                                    {dataset.name}
                                  </option>
                                ))
                              ) : (
                                <option value="">{t('no_dataset_option')}</option>
                              )}
                            </select>
                          </label>
                          {currentDataset ? (
                            <>
                              <div className="compact-grid">
                                <div className="compact-card">
                                  <strong>{t('field_source')}</strong>
                                  <div className="mono">{currentDataset.sourcePath}</div>
                                </div>
                                <div className="compact-card">
                                  <strong>{t('field_embedding_dataset')}</strong>
                                  <div className="mono">{currentEmbeddingDatasetPath || '—'}</div>
                                </div>
                                <div className="compact-card">
                                  <strong>{t('field_schema')}</strong>
                                  <div>{currentDataset.schema.join(', ') || '—'}</div>
                                </div>
                                <div className="compact-card">
                                  <strong>{t('field_embedding')}</strong>
                                  <div>{currentEmbeddingSummary}</div>
                                </div>
                                <div className="compact-card">
                                  <strong>{t('field_kind')}</strong>
                                  <div>{datasetKindLabel}</div>
                                </div>
                              </div>
                              {showBuildEmbeddingAction && (
                                <div className="notice">
                                  <div>{t('embedding_build_needed_hint')}</div>
                                  <div className="actions notice-actions">
                                    <button
                                      type="button"
                                      className="ghost"
                                      onClick={() => {
                                        void buildEmbeddingDataset(currentDataset.datasetId);
                                      }}
                                    >
                                      {t(
                                        currentEmbeddingStatus === 'failed'
                                          ? 'embedding_build_retry'
                                          : 'embedding_build_action'
                                      )}
                                    </button>
                                  </div>
                                </div>
                              )}
                              {isBuildingEmbedding && (
                                <div className="notice">{t('embedding_building_hint')}</div>
                              )}
                              {currentEmbeddingStatus === 'failed' && currentEmbeddingBuild?.error && (
                                <div className="notice error">
                                  {t('embedding_build_failed', { error: currentEmbeddingBuild.error })}
                                </div>
                              )}
                            </>
                          ) : (
                            <div className="compact-empty-card">
                              <div className="empty">{datasetCardEmptyMessage}</div>
                            </div>
                          )}
                          {sqlUnderstandingExpanded && currentDataset && (
                            <div className="compact-grid">
                              {sqlCards.map(([labelKey, value]) => (
                                <div key={labelKey} className="compact-card">
                                  <strong>{t(labelKey)}</strong>
                                  <div>{value || t('sql_part_not_set')}</div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="compact-empty-card">
                      <div className="empty">{t('no_dataset_for_analyze')}</div>
                    </div>
                  )}

                  <div className="workbench-stack">
                  <div className="workbench-form">
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
                    <div className="query-toolbar">
                      <label className="field">
                        <span>{t('filter_column')}</span>
                        <select
                          value={filterBuilder.column}
                          onChange={(event) =>
                            setFilterBuilder((current) => ({ ...current, column: event.target.value }))
                          }
                        >
                          <option value="">{t('filter_column_placeholder')}</option>
                          {schemaColumns.map((column) => (
                            <option key={column} value={column}>
                              {column}
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="field">
                        <span>{t('filter_operator')}</span>
                        <select
                          value={filterBuilder.operator}
                          onChange={(event) =>
                            setFilterBuilder((current) => ({
                              ...current,
                              operator: event.target.value as FilterBuilderState['operator'],
                            }))
                          }
                        >
                          <option value="=">=</option>
                          <option value="!=">!=</option>
                          <option value=">">&gt;</option>
                          <option value=">=">&gt;=</option>
                          <option value="<">&lt;</option>
                          <option value="<=">&lt;=</option>
                        </select>
                      </label>
                      <label className="field">
                        <span>{t('filter_value')}</span>
                        <input
                          value={filterBuilder.value}
                          onChange={(event) =>
                            setFilterBuilder((current) => ({ ...current, value: event.target.value }))
                          }
                        />
                      </label>
                      <div className="field">
                        <span>{t('filter_quick_action')}</span>
                        <button type="button" className="ghost" onClick={applyFilterBuilder}>
                          {t('filter_generate')}
                        </button>
                      </div>
                    </div>
                    <div className="hybrid-inline-block">
                    <div className="helper">{t('hybrid_search_inline_hint')}</div>
                    {hybridMessage && (
                      <div className={`notice ${hybridMessage.kind === 'error' ? 'error' : ''}`}>
                        {hybridMessage.text}
                      </div>
                    )}
                    {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'disabled' && (
                      <div className="notice error">{t('hybrid_requires_embedding_enabled')}</div>
                    )}
                    {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'configured' && (
                      <div className="notice">
                        <div>{t('hybrid_requires_embedding_dataset')}</div>
                        <div className="actions notice-actions">
                          <button
                            type="button"
                            className="ghost"
                            onClick={() => {
                              void buildEmbeddingDataset(currentDataset.datasetId);
                            }}
                          >
                            {t('embedding_build_action')}
                          </button>
                        </div>
                      </div>
                    )}
                    {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'building' && (
                      <div className="notice">{t('embedding_building_hint')}</div>
                    )}
                    {!hybridSearchReady && currentDataset && currentEmbeddingStatus === 'failed' && (
                      <div className="notice error">
                        <div>{t('hybrid_requires_embedding_dataset')}</div>
                        <div className="actions notice-actions">
                          <button
                            type="button"
                            className="ghost"
                            onClick={() => {
                              void buildEmbeddingDataset(currentDataset.datasetId);
                            }}
                          >
                            {t('embedding_build_retry')}
                          </button>
                        </div>
                      </div>
                    )}
                    <div className="hybrid-inline-form">
                      <label>
                        <span>{t('hybrid_query')}</span>
                        <input
                          value={hybridSearch.queryText}
                          onChange={(event) =>
                            setHybridSearch((current) => ({ ...current, queryText: event.target.value }))
                          }
                          placeholder={t('hybrid_query_placeholder')}
                          disabled={!currentDataset}
                        />
                      </label>
                      <div className="helper">{t('hybrid_locked_config_hint', {
                        columns: currentDataset?.embeddingConfig.textColumns.join(', ') || '—',
                        provider: currentDataset?.embeddingConfig.provider || '—',
                        model: currentDataset?.embeddingConfig.model || '—',
                      })}</div>
                      <div className="field-grid">
                        <label>
                          <span>{t('hybrid_top_k')}</span>
                          <input
                            value={hybridSearch.topK}
                            onChange={(event) =>
                              setHybridSearch((current) => ({ ...current, topK: event.target.value }))
                            }
                            disabled={!currentDataset}
                          />
                        </label>
                      </div>
                    </div>
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
                    <div className="actions primary-actions">
                      <button type="button" onClick={() => void runAnalysis()}>{t('run_analysis')}</button>
                      <button type="button" onClick={() => void runHybridSearch()} disabled={!currentDataset || !hybridSearchReady}>
                        {t('run_hybrid_search')}
                      </button>
                    </div>
                  </div>
                  </div>

                  <div className="result-stack">
                    <div className="helper">{lastResult.title}</div>
                    <div
                      className="result-box"
                      dangerouslySetInnerHTML={{ __html: lastResult.html }}
                    />
                  </div>
                </div>
              </section>
            </div>
          </section>
        )}

        {view === 'runs' && (
          <section className="section active">
            <section className="panel">
              <div className="panel-head">
                <h2>{t('run_history')}</h2>
                <div className="actions">
                  <button className="ghost" onClick={() => void refreshRuns()}>
                    {t('refresh')}
                  </button>
                </div>
              </div>
              <div className="panel-body stack">
                {runMessage && (
                  <div className={`notice ${runMessage.kind === 'error' ? 'error' : ''}`}>
                    {runMessage.text}
                  </div>
                )}
                <div className="list">
                  {pagedRuns.map((run) => {
                    const expanded = run.run_id === selectedRunId;
                    return (
                      <div key={run.run_id} className={`list-item ${expanded ? 'active' : ''}`}>
                        <div
                          onClick={() => setSelectedRunId(expanded ? null : run.run_id)}
                          style={{ cursor: 'pointer' }}
                        >
                          <div className="item-head">
                            <h4>{run.run_name || run.run_id}</h4>
                            <button
                              type="button"
                              className="ghost danger-button"
                              onClick={(event) => {
                                event.stopPropagation();
                                void deleteRun(run.run_id);
                              }}
                            >
                              {t('delete_run')}
                            </button>
                          </div>
                          <div className="meta">
                            <span>{run.status}</span>
                            <span>{run.action}</span>
                            <span>{`${run.artifact_count ?? 0} ${t('label_artifacts')}`}</span>
                          </div>
                        </div>
                        {expanded && selectedRunDetail && selectedRunDetail.run.run_id === run.run_id && (
                          <div className="result-box" style={{ marginTop: 14 }}>
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
                              <button className="ghost" onClick={() => saveRunDetailAsDataset('data')}>
                                {t('save_result_action')}
                              </button>
                              <button
                                className="ghost"
                                onClick={() => {
                                  saveRunDetailAsDataset('analyze');
                                }}
                              >
                                {t('analyze_action')}
                              </button>
                              <button
                                className="ghost danger-button"
                                onClick={() => {
                                  void deleteRun(run.run_id);
                                }}
                              >
                                {t('delete_run')}
                              </button>
                            </div>
                            <div
                              dangerouslySetInnerHTML={{
                                __html: renderPreviewTable(selectedRunDetail.preview, t('no_preview_rows')),
                              }}
                            />
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
          </section>
        )}
      </main>
    </div>
  );
}

import { DATASETS_KEY } from './i18n';

export const DEFAULT_CHINESE_EMBEDDING_MODEL = 'BAAI/bge-small-zh-v1.5';
export const EMBEDDING_MODEL_OPTIONS = [
  'BAAI/bge-small-zh-v1.5',
  'sentence-transformers/all-MiniLM-L6-v2',
] as const;
export const EMBEDDING_TEMPLATE_OPTIONS = ['text-v1'] as const;

export type PreviewData = {
  schema?: string[];
  rows?: Record<string, unknown>[];
  row_count?: number;
  truncated?: boolean;
};

export type ImportOptions = {
  delimiter: string;
  columns: string;
  mappings: string;
  regexPattern: string;
  lineMode: 'split' | 'regex';
  jsonFormat: string;
};

export type EmbeddingConfig = {
  enabled: boolean;
  textColumns: string[];
  provider: string;
  model: string;
  templateVersion: string;
  vectorColumn: string;
};

export type KeywordIndexConfig = {
  enabled: boolean;
  textColumns: string[];
  analyzer: string;
};

export type EmbeddingDatasetRecord = {
  datasetPath: string;
  artifactUri: string;
  artifactId: string;
  runId: string;
  builtAt: string;
  schema: string[];
  rowCount?: number;
};

export type KeywordIndexRecord = {
  indexPath: string;
  artifactUri: string;
  artifactId: string;
  runId: string;
  builtAt: string;
  schema: string[];
  docCount?: number;
  termCount?: number;
};

export type DatasetRecord = {
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
  keywordConfig: KeywordIndexConfig;
  keywordIndex: KeywordIndexRecord | null;
};

export type EmbeddingBuildState = {
  status: 'building' | 'failed';
  error?: string;
};

export type RunSummary = {
  run_id: string;
  status: string;
  action: string;
  artifact_count?: number;
  run_name?: string | null;
};

export type RunDetailPayload = {
  run: RunSummary & Record<string, unknown>;
  artifact?: {
    artifact_id: string;
    uri: string;
    format: string;
    schema_json?: string[];
  } | null;
  preview: PreviewData;
};

export type ImportPreviewPayload = {
  dataset: {
    name: string;
    source_type: string;
    source_path: string;
    source_label?: string;
  };
  preview: PreviewData;
};

export type ServiceInfo = {
  baseUrl: string;
  packaged: boolean;
};

export type AppConfig = {
  bitableAppId: string;
  bitableAppSecret: string;
  aiProvider: 'openai' | 'claude' | 'custom';
  aiApiKey: string;
  aiBaseUrl: string;
  aiModel: string;
};

// 'monitors' is kept for code compatibility but hidden from navigation
// Monitor view hidden — agentic features paused for v0.3 focus
export type ViewKey = 'data' | 'analyze' | 'runs' | 'settings' | 'monitors';

export type AnalyzeState = {
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

export type FilterBuilderState = {
  column: string;
  operator: '=' | '!=' | '>' | '>=' | '<' | '<=';
  value: string;
};

export type ImportFormState = {
  inputPath: string;
  inputType: string;
  delimiter: string;
  columns: string;
  regexPattern: string;
  datasetName: string;
  bitableAppId: string;
  bitableAppSecret: string;
  jsonFormat: string;
  embeddingEnabled: boolean;
  embeddingTextColumns: string;
  embeddingProvider: string;
  embeddingModel: string;
  embeddingTemplateVersion: string;
  embeddingVectorColumn: string;
  keywordEnabled: boolean;
  keywordTextColumns: string;
  keywordAnalyzer: string;
};

export type HybridSearchState = {
  queryText: string;
  textColumns: string;
  provider: string;
  model: string;
  templateVersion: string;
  topK: string;
  vectorColumn: string;
};

export type KeywordSearchState = {
  queryText: string;
  topK: string;
};

export type LastResultState = {
  kind: 'sql' | 'hybrid' | 'keyword';
  title: string;
  html: string;
};

export type ExternalSourceRecord = {
  source_id: string;
  kind: string;
  name: string;
  schema_binding?: {
    time_field?: string;
    type_field?: string;
    key_field?: string;
    field_mappings?: Record<string, string>;
  };
};

export type MonitorRecord = {
  monitor_id: string;
  name: string;
  enabled: boolean;
  intent_text?: string;
  execution_mode: string;
  source: Record<string, unknown>;
  validation?: { status?: string; errors?: string[] };
  state?: { status?: string; last_error?: string | null; stream_query_id?: string | null };
};

export type FocusEventRecord = {
  event_id: string;
  monitor_id: string;
  triggered_at: string;
  severity: string;
  title: string;
  summary: string;
  status: string;
  key_fields: Record<string, unknown>;
  run_id?: string | null;
  artifact_ids?: string[];
};

export type SourceFormState = {
  sourceId: string;
  name: string;
  timeField: string;
  typeField: string;
  keyField: string;
  priceField: string;
};

export type MonitorFormState = {
  name: string;
  intentText: string;
  sourceId: string;
  executionMode: 'batch' | 'stream';
  countThreshold: string;
  groupBy: string;
};

/** Translation function type */
export type TFunction = (key: string, vars?: Record<string, string | number>) => string;

/* ---------- defaults ---------- */

export const defaultImportOptions: ImportOptions = {
  delimiter: ',',
  columns: '',
  mappings: '',
  regexPattern: '',
  lineMode: 'split',
  jsonFormat: 'json_lines',
};

export const defaultEmbeddingConfig: EmbeddingConfig = {
  enabled: false,
  textColumns: [],
  provider: 'minilm',
  model: DEFAULT_CHINESE_EMBEDDING_MODEL,
  templateVersion: 'text-v1',
  vectorColumn: 'embedding',
};

export const defaultKeywordConfig: KeywordIndexConfig = {
  enabled: false,
  textColumns: [],
  analyzer: 'jieba',
};

export const defaultImportForm: ImportFormState = {
  inputPath: '',
  inputType: 'auto',
  delimiter: ',',
  columns: '',
  regexPattern: '',
  datasetName: '',
  bitableAppId: '',
  bitableAppSecret: '',
  jsonFormat: 'json_lines',
  embeddingEnabled: false,
  embeddingTextColumns: '',
  embeddingProvider: 'minilm',
  embeddingModel: DEFAULT_CHINESE_EMBEDDING_MODEL,
  embeddingTemplateVersion: 'text-v1',
  embeddingVectorColumn: 'embedding',
  keywordEnabled: false,
  keywordTextColumns: '',
  keywordAnalyzer: 'jieba',
};

export const defaultAnalyzeState: AnalyzeState = {
  inputPath: '',
  inputType: 'auto',
  tableName: 'input_table',
  preset: 'preview',
  query: 'SELECT * FROM input_table LIMIT 20',
  ...defaultImportOptions,
};

export const defaultHybridSearchState: HybridSearchState = {
  queryText: '',
  textColumns: '',
  provider: 'minilm',
  model: DEFAULT_CHINESE_EMBEDDING_MODEL,
  templateVersion: 'text-v1',
  topK: '10',
  vectorColumn: 'embedding',
};

export const defaultKeywordSearchState: KeywordSearchState = {
  queryText: '',
  topK: '10',
};

export const defaultSourceForm: SourceFormState = {
  sourceId: '',
  name: '',
  timeField: 'ts',
  typeField: 'kind',
  keyField: 'symbol',
  priceField: 'price',
};

export const defaultMonitorForm: MonitorFormState = {
  name: '',
  intentText: 'count events in a window',
  sourceId: '',
  executionMode: 'stream',
  countThreshold: '2',
  groupBy: 'source_key,event_type',
};

export const viewMeta = {
  data: { titleKey: 'view_data_title', subtitleKey: 'view_data_subtitle' },
  analyze: { titleKey: 'view_analyze_title', subtitleKey: 'view_analyze_subtitle' },
  runs: { titleKey: 'view_runs_title', subtitleKey: 'view_runs_subtitle' },
  monitors: { titleKey: 'view_monitors_title', subtitleKey: 'view_monitors_subtitle' },
  settings: { titleKey: 'view_settings_title', subtitleKey: 'view_settings_subtitle' },
} as const;

/* ---------- pure helpers ---------- */

export function decodeFileUri(uri: string): string {
  try {
    if (!uri.startsWith('file://')) return uri;
    return decodeURIComponent(new URL(uri).pathname);
  } catch {
    return uri;
  }
}

export function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

export function csvToList(value: string): string[] {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

export function listToCsv(values: string[]): string {
  return values.join(', ');
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

export function normalizePreview(value: unknown): PreviewData {
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

export function normalizeImportOptions(value: unknown): ImportOptions {
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

export function normalizeEmbeddingConfig(value: unknown): EmbeddingConfig {
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

export function normalizeKeywordConfig(value: unknown): KeywordIndexConfig {
  const record = asRecord(value);
  const textColumns = Array.isArray(record?.textColumns)
    ? record.textColumns.filter((item): item is string => typeof item === 'string')
    : [];
  return {
    enabled: Boolean(record?.enabled),
    textColumns,
    analyzer: typeof record?.analyzer === 'string' && record.analyzer ? record.analyzer : 'jieba',
  };
}

export function normalizeEmbeddingDatasetRecord(value: unknown): EmbeddingDatasetRecord | null {
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

export function normalizeKeywordIndexRecord(value: unknown): KeywordIndexRecord | null {
  const record = asRecord(value);
  if (!record) return null;
  if (typeof record.indexPath !== 'string' || !record.indexPath) return null;
  return {
    indexPath: record.indexPath,
    artifactUri: typeof record.artifactUri === 'string' ? record.artifactUri : '',
    artifactId: typeof record.artifactId === 'string' ? record.artifactId : '',
    runId: typeof record.runId === 'string' ? record.runId : '',
    builtAt: typeof record.builtAt === 'string' ? record.builtAt : new Date().toISOString(),
    schema: Array.isArray(record.schema)
      ? record.schema.filter((item): item is string => typeof item === 'string')
      : [],
    docCount: typeof record.docCount === 'number' ? record.docCount : undefined,
    termCount: typeof record.termCount === 'number' ? record.termCount : undefined,
  };
}

export function normalizeDatasetRecord(value: unknown): DatasetRecord | null {
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
    keywordConfig: normalizeKeywordConfig(record.keywordConfig),
    keywordIndex: normalizeKeywordIndexRecord(record.keywordIndex),
  };
}

export function createDatasetRecord(payload: {
  name: string;
  sourceType: string;
  sourcePath: string;
  preview: PreviewData;
  kind: DatasetRecord['kind'];
  description?: string;
  sourceLabel?: string;
  importOptions?: Partial<ImportOptions>;
  embeddingConfig?: Partial<EmbeddingConfig>;
  embeddingDataset?: EmbeddingDatasetRecord | null;
  keywordConfig?: Partial<KeywordIndexConfig>;
  keywordIndex?: KeywordIndexRecord | null;
}): DatasetRecord {
  const importOptions = { ...defaultImportOptions, ...(payload.importOptions || {}) };
  const embeddingConfig = { ...defaultEmbeddingConfig, ...(payload.embeddingConfig || {}) };
  const keywordConfig = { ...defaultKeywordConfig, ...(payload.keywordConfig || {}) };
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
    sourceLabel: payload.sourceLabel || '',
    importOptions,
    embeddingConfig,
    embeddingDataset: payload.embeddingDataset || null,
    keywordConfig,
    keywordIndex: payload.keywordIndex || null,
  };
}

export function highlightSql(sql: string): string {
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

export function extractClause(sql: string, startPattern: RegExp, endPatterns: RegExp[] = []): string | null {
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

export function parseSqlStructure(sql: string) {
  return {
    select: extractClause(sql, /\bSELECT\b/i, [/\bFROM\b/i]),
    from: extractClause(sql, /\bFROM\b/i, [/\bWHERE\b/i, /\bGROUP\s+BY\b/i, /\bORDER\s+BY\b/i, /\bLIMIT\b/i]),
    where: extractClause(sql, /\bWHERE\b/i, [/\bGROUP\s+BY\b/i, /\bORDER\s+BY\b/i, /\bLIMIT\b/i]),
    groupBy: extractClause(sql, /\bGROUP\s+BY\b/i, [/\bORDER\s+BY\b/i, /\bLIMIT\b/i]),
    orderBy: extractClause(sql, /\bORDER\s+BY\b/i, [/\bLIMIT\b/i]),
    limit: extractClause(sql, /\bLIMIT\b/i),
  };
}

export function quoteIdentifier(identifier: string) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    return identifier;
  }
  return `"${identifier.replaceAll('"', '""')}"`;
}

export function quoteLiteral(value: string) {
  const trimmed = value.trim();
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return trimmed;
  }
  return `'${trimmed.replaceAll("'", "''")}'`;
}

export function renderPreviewTable(preview: PreviewData, emptyText: string) {
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

export function renderHybridPreviewTable(
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

export function extractHybridPreview(payload: unknown): PreviewData {
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

export function extractHybridExplain(payload: unknown): string {
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

export function extractEmbeddingDatasetRecord(payload: unknown): EmbeddingDatasetRecord {
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

export function extractEmbeddingDatasetFromRunPayload(payload: unknown): EmbeddingDatasetRecord | null {
  const record = asRecord(payload);
  const run = asRecord(record?.run);
  const details = asRecord(run?.details);
  return normalizeEmbeddingDatasetRecord(details?.embedding_dataset ?? null);
}

export function extractKeywordIndexRecord(payload: unknown): KeywordIndexRecord {
  const record = asRecord(payload);
  const result = asRecord(record?.result);
  const run = asRecord(record?.run);
  const artifact = asRecord(record?.artifact);
  const indexPath = typeof result?.index_path === 'string' ? result.index_path : '';
  if (!indexPath) {
    throw new Error('keyword index build response missing index_path');
  }
  return {
    indexPath,
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
    docCount: typeof result?.doc_count === 'number' ? result.doc_count : undefined,
    termCount: typeof result?.term_count === 'number' ? result.term_count : undefined,
  };
}

export function extractKeywordIndexFromRunPayload(payload: unknown): KeywordIndexRecord | null {
  const record = asRecord(payload);
  const run = asRecord(record?.run);
  const details = asRecord(run?.details);
  return normalizeKeywordIndexRecord(details?.keyword_index ?? null);
}

export function embeddingFormToConfig(form: ImportFormState): EmbeddingConfig {
  return {
    enabled: form.embeddingEnabled,
    textColumns: csvToList(form.embeddingTextColumns),
    provider: form.embeddingProvider.trim(),
    model: form.embeddingModel.trim(),
    templateVersion: form.embeddingTemplateVersion.trim(),
    vectorColumn: form.embeddingVectorColumn.trim() || 'embedding',
  };
}

export function keywordFormToConfig(form: ImportFormState): KeywordIndexConfig {
  return {
    enabled: form.keywordEnabled,
    textColumns: csvToList(form.keywordTextColumns),
    analyzer: form.keywordAnalyzer.trim() || 'jieba',
  };
}

export function createEmbeddingBuildPayload(dataset: DatasetRecord): Record<string, unknown> {
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

export function createKeywordIndexBuildPayload(dataset: DatasetRecord): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    text_columns: dataset.keywordConfig.textColumns,
    analyzer: dataset.keywordConfig.analyzer || 'jieba',
    run_name: `keyword-index-${dataset.name}`,
    description: `Keyword index build for dataset ${dataset.name}`,
  };
  if (['parquet', 'arrow', 'bitable'].includes(dataset.sourceType)) {
    payload.dataset_path = dataset.sourcePath;
  } else {
    payload.input_path = dataset.sourcePath;
    payload.input_type = dataset.sourceType;
    payload.delimiter = dataset.importOptions.delimiter;
    payload.json_format = dataset.importOptions.jsonFormat;
    payload.line_mode = dataset.importOptions.lineMode;
    if (dataset.importOptions.columns) payload.columns = dataset.importOptions.columns;
    if (dataset.importOptions.mappings) payload.mappings = dataset.importOptions.mappings;
    if (dataset.importOptions.regexPattern) payload.regex_pattern = dataset.importOptions.regexPattern;
  }
  return payload;
}

export function isDatasetBackedByRun(dataset: DatasetRecord, runDir: string): boolean {
  return dataset.kind === 'result' && dataset.sourcePath.startsWith(`${runDir}/`);
}

export function formatDatasetTimestamp(date = new Date()): string {
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}-${pad(
    date.getHours()
  )}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

export function buildSavedDatasetName(dataset: ImportPreviewPayload['dataset']): string {
  const base = dataset.name.trim() || 'dataset';
  if (dataset.source_type !== 'bitable') return base;
  return `${base}-${formatDatasetTimestamp()}`;
}

export function maskSecret(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return '';
  if (trimmed.length <= 6) return `${trimmed.slice(0, 1)}***${trimmed.slice(-1)}`;
  return `${trimmed.slice(0, 3)}***${trimmed.slice(-3)}`;
}

export function importPreviewFromRunPayload(payload: unknown): ImportPreviewPayload {
  const record = asRecord(payload);
  const run = asRecord(record?.run);
  const details = asRecord(run?.details);
  const artifact = asRecord(record?.artifact);
  return {
    dataset: {
      name:
        typeof details?.dataset_name === 'string' && details.dataset_name
          ? details.dataset_name
          : typeof run?.run_name === 'string' && run.run_name
            ? run.run_name
            : 'bitable',
      source_type:
        typeof details?.source_type === 'string' && details.source_type ? details.source_type : 'bitable',
      source_path:
        typeof details?.source_path === 'string' && details.source_path
          ? details.source_path
          : decodeFileUri(typeof artifact?.uri === 'string' ? artifact.uri : ''),
      source_label: typeof details?.source_label === 'string' ? details.source_label : '',
    },
    preview: normalizePreview(record?.preview),
  };
}

export function isTerminalRunStatus(status: string | undefined): boolean {
  return status === 'succeeded' || status === 'failed' || status === 'cancelled';
}

export function externalEventColumns(source: ExternalSourceRecord | null | undefined): string[] {
  if (!source) return ['event_time', 'event_type', 'source_key'];
  const mapped = Object.keys(source.schema_binding?.field_mappings || {}).filter(Boolean);
  return ['event_time', 'event_type', 'source_key', ...mapped];
}

export function loadDatasets(): DatasetRecord[] {
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

export function saveDatasets(datasets: DatasetRecord[]) {
  window.localStorage.setItem(DATASETS_KEY, JSON.stringify(datasets));
}

const DATASETS_KEY = 'velaria.prototype.datasets.v1';
const LOCALE_KEY = 'velaria.prototype.locale.v1';

const I18N = {
  en: {
    app_title: 'Velaria Prototype',
    brand_sub:
      'Local data workbench prototype. Import first, analyze next, save only when it matters.',
    nav_home: 'Home',
    nav_data: 'Data',
    nav_analyze: 'Analyze',
    nav_runs: 'Runs',
    service_title: 'Service',
    status_bootstrapping: 'Bootstrapping…',
    status_waiting: 'Waiting for local velaria-service.',
    status_ready_on: 'Ready on {port}',
    status_packaged: 'packaged={packaged} | version={version}',
    status_bootstrap_failed: 'Bootstrap failed',
    scope_title: 'Prototype Scope',
    scope_body:
      'This prototype keeps imported datasets locally in browser storage and uses the Python sidecar for preview, SQL execution, run tracking, and artifact preview.',
    view_home_title: 'Import, inspect, analyze, keep moving.',
    view_home_subtitle:
      'This prototype now follows a real workbench shape: datasets on one side, runs on the other, and analysis in the middle.',
    view_data_title: 'Build datasets from local files before you analyze them.',
    view_data_subtitle:
      'Import preview comes first, dataset creation comes second. The Data view is where files become reusable objects.',
    view_analyze_title: 'Turn one dataset into one tracked analysis run.',
    view_analyze_subtitle:
      'The Analyze view keeps the SQL surface explicit, but never lets the result disappear without a run record.',
    view_runs_title: 'Runs are the history of what actually happened.',
    view_runs_subtitle:
      'Preview the result artifact, inspect metadata, and decide whether a result should stay temporary or become a reusable dataset.',
    quick_actions: 'Quick Actions',
    quick_import: 'Import File',
    quick_analyze: 'Analyze Dataset',
    quick_runs: 'Open Runs',
    quick_helper:
      'Use the Data view to build datasets, the Analyze view to execute SQL, and the Runs view to inspect result artifacts.',
    getting_started: 'How To Start',
    guide_step_1_label: 'Step 1',
    guide_step_1_title: 'Import a local file',
    guide_step_1_body:
      'Go to Data, choose a file, then click Preview Import. Start with CSV if you want the simplest path.',
    guide_step_2_label: 'Step 2',
    guide_step_2_title: 'Save it as a dataset',
    guide_step_2_body:
      'After the preview looks right, click Save as Dataset. That creates a reusable object for later analysis.',
    guide_step_3_label: 'Step 3',
    guide_step_3_title: 'Run analysis',
    guide_step_3_body:
      'Open Analyze, pick the dataset, keep the preview query or use a preset, then click Run Analysis.',
    recent_datasets: 'Recent Datasets',
    metric_datasets: 'Datasets',
    metric_saved_results: 'Saved Results',
    metric_runs: 'Tracked Runs',
    metric_last_status: 'Last Run Status',
    import_wizard: 'Import Wizard',
    meta_preview_first: 'Preview first',
    meta_dataset_second: 'Dataset second',
    meta_sql_mode: 'SQL mode',
    meta_run_tracked: 'Run tracked',
    input_path: 'Input Path',
    input_type: 'Input Type',
    delimiter_label: 'Delimiter / Split Delimiter',
    columns_or_mappings: 'JSON Columns or Line Mappings',
    regex_pattern: 'Regex Pattern',
    dataset_name: 'Dataset Name',
    choose_file: 'Choose File',
    preview_import: 'Preview Import',
    save_as_dataset: 'Save as Dataset',
    export_file: 'Export File',
    datasets: 'Datasets',
    refresh: 'Refresh',
    data_detail: 'Data Detail',
    analyze_action: 'Analyze',
    remove_dataset: 'Remove Dataset',
    dataset_context: 'Dataset Context',
    dataset_label: 'Dataset',
    analyze_workspace: 'Analyze Workspace',
    table_name: 'Table Name',
    query_preset: 'Query Preset',
    preset_preview: 'Preview',
    preset_filter: 'Filter Score > 20',
    preset_aggregate: 'Group By + Count',
    sql_query: 'SQL Query',
    run_analysis: 'Run Analysis',
    export_result_file: 'Export Result File',
    save_result_as_dataset: 'Save Result as Dataset',
    page_prev: 'Previous',
    page_next: 'Next',
    page_status: 'Page {page} / {total}',
    import_step_1: '1. Choose a file and input type.',
    import_step_2: '2. Preview the data and confirm schema.',
    import_step_3: '3. Save the preview as a reusable dataset.',
    import_hint:
      'Start here. First choose a file, then click Preview Import. If the preview looks correct, click Save as Dataset.',
    analyze_hint:
      'Pick a dataset first. If you are unsure what to run, keep the default preview query or choose a preset.',
    run_history: 'Run History',
    run_detail: 'Run Detail',
    no_preview_rows: 'No preview rows returned.',
    no_datasets_yet: 'No datasets yet. Import a local file to get started.',
    no_datasets_available: 'No datasets available. Save an import preview first.',
    dataset_detail_empty:
      'Select a dataset to inspect schema, preview, and source metadata.',
    no_dataset_for_analyze: 'Import and save a dataset before you analyze it.',
    no_runs_yet: 'No tracked runs yet.',
    run_detail_empty: 'Select a run to inspect result preview, artifact, and metadata.',
    loading_runs: 'Loading runs…',
    loading_preview: 'Loading preview…',
    preview_failed: 'Preview failed: {error}',
    rows_count: '{count} rows',
    loading_run_detail: 'Loading run detail…',
    run_detail_failed: 'Failed to load run detail: {error}',
    running_analysis: 'Running analysis…',
    run_failed: 'Run failed: {error}',
    export_success: 'Exported to: {path}',
    export_cancelled: 'Export cancelled.',
    export_failed: 'Export failed: {error}',
    kind_imported: 'Imported',
    kind_result: 'Result',
    field_source: 'source',
    field_created_at: 'created_at',
    field_schema: 'schema',
    field_dataset_id: 'dataset_id',
    field_table: 'table',
    field_query: 'query',
    field_artifact: 'artifact',
    field_run_id: 'run_id',
    field_artifact_id: 'artifact_id',
    field_artifact_uri: 'artifact_uri',
    save_result_action: 'Save Result as Dataset',
    input_placeholder: '/absolute/path/to/input.csv',
    columns_placeholder: 'user_id,name,score or uid:1,action:2',
    regex_placeholder: 'Optional for line regex mode',
    dataset_name_placeholder: 'Optional, defaults to filename',
    no_dataset_option: 'No dataset available',
    label_artifacts: 'artifacts',
  },
  zh: {
    app_title: 'Velaria 原型',
    brand_sub: '本地数据工作台原型。先导入，再分析，只有在值得保留时才保存。',
    nav_home: '首页',
    nav_data: '数据',
    nav_analyze: '分析',
    nav_runs: '运行',
    service_title: '服务',
    status_bootstrapping: '正在启动…',
    status_waiting: '等待本地 velaria-service 就绪。',
    status_ready_on: '已就绪，端口 {port}',
    status_packaged: 'packaged={packaged} | 版本={version}',
    status_bootstrap_failed: '启动失败',
    scope_title: '原型范围',
    scope_body:
      '这个原型会把导入后的数据集保存在浏览器本地存储中，并通过 Python sidecar 完成预览、SQL 执行、运行跟踪和产物预览。',
    view_home_title: '先导入、先确认，再分析、再保存。',
    view_home_subtitle:
      '这个原型已经按真正的数据工作台组织：一边是数据集，一边是运行历史，中间是分析工作区。',
    view_data_title: '先把本地文件变成数据集，再进入分析。',
    view_data_subtitle:
      '先做导入预览，再创建数据集。Data 视图是文件变成可复用对象的地方。',
    view_analyze_title: '把一个数据集转成一次可追踪的分析运行。',
    view_analyze_subtitle:
      'Analyze 视图保持 SQL 面明确，同时保证结果不会脱离 run 记录而消失。',
    view_runs_title: 'Runs 记录的是实际发生过的分析历史。',
    view_runs_subtitle:
      '查看结果产物、检查元数据，并决定结果是继续保持临时状态，还是沉淀成可复用数据集。',
    quick_actions: '快捷动作',
    quick_import: '导入文件',
    quick_analyze: '分析数据集',
    quick_runs: '查看运行',
    quick_helper:
      '在 Data 视图中构建数据集，在 Analyze 视图中执行 SQL，在 Runs 视图中查看结果产物。',
    getting_started: '三步开始使用',
    guide_step_1_label: '第 1 步',
    guide_step_1_title: '导入本地文件',
    guide_step_1_body:
      '先进入“数据”页，选择一个本地文件，然后点击“预览导入”。如果想先走最简单的路径，优先用 CSV。',
    guide_step_2_label: '第 2 步',
    guide_step_2_title: '保存为数据集',
    guide_step_2_body:
      '确认预览结果和列结构没问题后，点击“保存为数据集”。这样它才会变成后续可复用对象。',
    guide_step_3_label: '第 3 步',
    guide_step_3_title: '执行分析',
    guide_step_3_body:
      '进入“分析”页，选择刚保存的数据集。先保留默认预览查询，或者直接使用模板，再点击“执行分析”。',
    recent_datasets: '最近数据集',
    metric_datasets: '数据集',
    metric_saved_results: '已保存结果',
    metric_runs: '已跟踪运行',
    metric_last_status: '最近运行状态',
    import_wizard: '导入向导',
    meta_preview_first: '先预览',
    meta_dataset_second: '后建数据集',
    meta_sql_mode: 'SQL 模式',
    meta_run_tracked: '写入运行记录',
    input_path: '输入路径',
    input_type: '输入类型',
    delimiter_label: '分隔符 / 切分分隔符',
    columns_or_mappings: 'JSON 列或 Line 映射',
    regex_pattern: '正则表达式',
    dataset_name: '数据集名称',
    choose_file: '选择文件',
    preview_import: '预览导入',
    save_as_dataset: '保存为数据集',
    export_file: '导出文件',
    datasets: '数据集',
    refresh: '刷新',
    data_detail: '数据详情',
    analyze_action: '去分析',
    remove_dataset: '移除数据集',
    dataset_context: '数据集上下文',
    dataset_label: '数据集',
    analyze_workspace: '分析工作区',
    table_name: '表名',
    query_preset: '查询模板',
    preset_preview: '预览',
    preset_filter: '筛选分数 > 20',
    preset_aggregate: '分组计数',
    sql_query: 'SQL 查询',
    run_analysis: '执行分析',
    export_result_file: '导出结果文件',
    save_result_as_dataset: '将结果保存为数据集',
    page_prev: '上一页',
    page_next: '下一页',
    page_status: '第 {page} / {total} 页',
    import_step_1: '1. 先选择文件和输入类型。',
    import_step_2: '2. 先看预览，再确认 schema。',
    import_step_3: '3. 把预览保存成可复用数据集。',
    import_hint:
      '从这里开始。先选择文件，再点“预览导入”。如果预览正确，再点“保存为数据集”。',
    analyze_hint:
      '先选一个数据集。如果你不知道要跑什么，先保留默认预览查询，或者直接选一个模板。',
    run_history: '运行历史',
    run_detail: '运行详情',
    no_preview_rows: '没有可展示的预览行。',
    no_datasets_yet: '还没有数据集。先导入一个本地文件开始。',
    no_datasets_available: '当前没有数据集。先把导入预览保存成数据集。',
    dataset_detail_empty: '请选择一个数据集，查看 schema、预览和来源元数据。',
    no_dataset_for_analyze: '先导入并保存至少一个数据集，再进入分析。',
    no_runs_yet: '还没有任何运行记录。',
    run_detail_empty: '请选择一个运行，查看结果预览、产物和元数据。',
    loading_runs: '正在加载运行…',
    loading_preview: '正在加载预览…',
    preview_failed: '预览失败：{error}',
    rows_count: '{count} 行',
    loading_run_detail: '正在加载运行详情…',
    run_detail_failed: '加载运行详情失败：{error}',
    running_analysis: '正在执行分析…',
    run_failed: '运行失败：{error}',
    export_success: '已导出到：{path}',
    export_cancelled: '已取消导出。',
    export_failed: '导出失败：{error}',
    kind_imported: '导入数据',
    kind_result: '分析结果',
    field_source: '来源',
    field_created_at: '创建时间',
    field_schema: 'Schema',
    field_dataset_id: '数据集 ID',
    field_table: '表',
    field_query: '查询',
    field_artifact: '产物',
    field_run_id: '运行 ID',
    field_artifact_id: '产物 ID',
    field_artifact_uri: '产物地址',
    save_result_action: '保存结果为数据集',
    input_placeholder: '/绝对路径/到/输入文件.csv',
    columns_placeholder: 'user_id,name,score 或 uid:1,action:2',
    regex_placeholder: 'line regex 模式可选',
    dataset_name_placeholder: '可选，默认使用文件名',
    no_dataset_option: '当前没有可用数据集',
    label_artifacts: '个产物',
  },
};

const state = {
  serviceInfo: null,
  view: 'home',
  locale: window.localStorage.getItem(LOCALE_KEY) || 'zh',
  datasets: [],
  selectedDatasetId: null,
  pendingImport: null,
  runs: [],
  runsPage: 1,
  runsPageSize: 8,
  selectedRunId: null,
  selectedRunDetail: null,
  lastAnalysisPayload: null,
};

const viewMeta = {
  home: { titleKey: 'view_home_title', subtitleKey: 'view_home_subtitle' },
  data: { titleKey: 'view_data_title', subtitleKey: 'view_data_subtitle' },
  analyze: { titleKey: 'view_analyze_title', subtitleKey: 'view_analyze_subtitle' },
  runs: { titleKey: 'view_runs_title', subtitleKey: 'view_runs_subtitle' },
};

function el(id) {
  return document.getElementById(id);
}

function t(key, vars = {}) {
  const dict = I18N[state.locale] || I18N.en;
  const raw = dict[key] || I18N.en[key] || key;
  return Object.entries(vars).reduce(
    (acc, [name, value]) => acc.replaceAll(`{${name}}`, String(value)),
    raw
  );
}

function applyStaticI18n() {
  document.documentElement.lang = state.locale === 'zh' ? 'zh-CN' : 'en';
  document.title = t('app_title');
  document.querySelectorAll('[data-i18n]').forEach((node) => {
    node.textContent = t(node.getAttribute('data-i18n'));
  });
  el('import-path').placeholder = t('input_placeholder');
  el('analysis-path').placeholder = t('input_placeholder');
  el('import-columns').placeholder = t('columns_placeholder');
  el('import-regex-pattern').placeholder = t('regex_placeholder');
  el('import-dataset-name').placeholder = t('dataset_name_placeholder');
  el('service-status').textContent = t('status_bootstrapping');
  el('service-meta').textContent = t('status_waiting');
  el('lang-en').classList.toggle('active', state.locale === 'en');
  el('lang-zh').classList.toggle('active', state.locale === 'zh');
  [...el('analysis-preset').options].forEach((option) => {
    option.textContent = t(`preset_${option.value}`);
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function decodeFileUri(uri) {
  try {
    if (!uri || !uri.startsWith('file://')) {
      return uri || '';
    }
    return decodeURIComponent(new URL(uri).pathname);
  } catch {
    return uri || '';
  }
}

function loadDatasets() {
  try {
    const raw = window.localStorage.getItem(DATASETS_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveDatasets() {
  window.localStorage.setItem(DATASETS_KEY, JSON.stringify(state.datasets));
}

function createDatasetRecord(payload) {
  return {
    datasetId: `dataset_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
    name: payload.name,
    sourceType: payload.sourceType,
    sourcePath: payload.sourcePath,
    preview: payload.preview,
    schema: payload.preview?.schema || [],
    kind: payload.kind || 'imported',
    createdAt: new Date().toISOString(),
    description: payload.description || '',
    sourceLabel: payload.sourceLabel || '',
  };
}

function upsertDataset(dataset) {
  const index = state.datasets.findIndex((item) => item.datasetId === dataset.datasetId);
  if (index >= 0) {
    state.datasets[index] = dataset;
  } else {
    state.datasets.unshift(dataset);
  }
  saveDatasets();
}

function removeDataset(datasetId) {
  state.datasets = state.datasets.filter((dataset) => dataset.datasetId !== datasetId);
  if (state.selectedDatasetId === datasetId) {
    state.selectedDatasetId = state.datasets[0]?.datasetId || null;
  }
  saveDatasets();
}

function currentDataset() {
  return state.datasets.find((dataset) => dataset.datasetId === state.selectedDatasetId) || null;
}

function kindLabel(kind) {
  return t(`kind_${kind}`);
}

function renderPreviewTable(preview) {
  const rows = preview?.rows || [];
  const schema = preview?.schema || (rows[0] ? Object.keys(rows[0]) : []);
  if (!rows.length) {
    return `<div class="empty">${escapeHtml(t('no_preview_rows'))}</div>`;
  }
  const head = schema.map((column) => `<th>${escapeHtml(column)}</th>`).join('');
  const body = rows
    .map((row) => {
      const cells = schema
        .map((column) => `<td>${escapeHtml(row[column] ?? '')}</td>`)
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

function renderKeyValueRows(rows) {
  return rows
    .map(
      ([key, value]) =>
        `<div class="mono"><strong>${escapeHtml(t(key))}:</strong> ${escapeHtml(
          typeof value === 'string' ? value : JSON.stringify(value)
        )}</div>`
    )
    .join('');
}

async function api(path, options = {}) {
  const response = await fetch(`${state.serviceInfo.baseUrl}${path}`, {
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

function setView(view) {
  state.view = view;
  Object.keys(viewMeta).forEach((key) => {
    const button = document.querySelector(`[data-view="${key}"]`);
    const section = el(`view-${key}`);
    if (button) {
      button.classList.toggle('active', key === view);
    }
    if (section) {
      section.classList.toggle('active', key === view);
    }
  });
  el('view-title').textContent = t(viewMeta[view].titleKey);
  el('view-subtitle').textContent = t(viewMeta[view].subtitleKey);
}

function renderHome() {
  el('metric-datasets').textContent = String(state.datasets.length);
  el('metric-result-datasets').textContent = String(
    state.datasets.filter((dataset) => dataset.kind === 'result').length
  );
  el('metric-runs').textContent = String(state.runs.length);
  el('metric-last-status').textContent = state.runs[0]?.status || '—';

  const container = el('home-datasets');
  if (!state.datasets.length) {
    container.innerHTML = `<div class="empty">${escapeHtml(t('no_datasets_yet'))}</div>`;
    return;
  }
  container.innerHTML = state.datasets
    .slice(0, 4)
    .map(
      (dataset) => `
        <div class="list-item" data-home-dataset="${escapeHtml(dataset.datasetId)}">
          <h4>${escapeHtml(dataset.name)}</h4>
          <div class="meta">
            <span>${escapeHtml(dataset.sourceType)}</span>
            <span>${escapeHtml(t('rows_count', { count: dataset.preview?.row_count ?? '—' }))}</span>
            <span>${escapeHtml(kindLabel(dataset.kind))}</span>
          </div>
        </div>
      `
    )
    .join('');
  container.querySelectorAll('[data-home-dataset]').forEach((node) => {
    node.addEventListener('click', () => {
      state.selectedDatasetId = node.getAttribute('data-home-dataset');
      setView('data');
      renderDatasets();
      renderAnalyze();
    });
  });
}

function renderDatasets() {
  const list = el('dataset-list');
  if (!state.datasets.length) {
    list.innerHTML = `<div class="empty">${escapeHtml(t('no_datasets_available'))}</div>`;
  } else {
    list.innerHTML = state.datasets
      .map(
        (dataset) => `
          <div class="list-item ${dataset.datasetId === state.selectedDatasetId ? 'active' : ''}" data-dataset-id="${escapeHtml(dataset.datasetId)}">
            <h4>${escapeHtml(dataset.name)}</h4>
            <div class="meta">
              <span>${escapeHtml(dataset.sourceType)}</span>
              <span>${escapeHtml(t('rows_count', { count: dataset.preview?.row_count ?? '—' }))}</span>
              <span>${escapeHtml(kindLabel(dataset.kind))}</span>
            </div>
          </div>
        `
      )
      .join('');
    list.querySelectorAll('[data-dataset-id]').forEach((node) => {
      node.addEventListener('click', () => {
        state.selectedDatasetId = node.getAttribute('data-dataset-id');
        renderDatasets();
        renderAnalyze();
      });
    });
  }

  const dataset = currentDataset();
  const detail = el('dataset-detail');
  el('dataset-analyze').disabled = !dataset;
  el('dataset-export').disabled = !dataset;
  el('dataset-remove').disabled = !dataset;
  if (!dataset) {
    detail.innerHTML = `<div class="empty">${escapeHtml(t('dataset_detail_empty'))}</div>`;
    return;
  }
  detail.innerHTML = `
    <div class="meta">
      <span>${escapeHtml(dataset.name)}</span>
      <span>${escapeHtml(dataset.sourceType)}</span>
      <span>${escapeHtml(kindLabel(dataset.kind))}</span>
      <span>${escapeHtml(t('rows_count', { count: dataset.preview?.row_count ?? '—' }))}</span>
    </div>
    ${renderKeyValueRows([
      ['field_source', dataset.sourcePath],
      ['field_created_at', dataset.createdAt],
      ['field_schema', dataset.schema.join(', ') || '—'],
    ])}
    ${renderPreviewTable(dataset.preview)}
  `;
}

function renderAnalyze() {
  const select = el('analysis-dataset-select');
  if (!state.datasets.length) {
    select.innerHTML = `<option value="">${escapeHtml(t('no_dataset_option'))}</option>`;
    select.disabled = true;
    el('analysis-dataset-meta').innerHTML = `<div class="empty">${escapeHtml(t('no_dataset_for_analyze'))}</div>`;
    return;
  }

  if (!state.selectedDatasetId || !currentDataset()) {
    state.selectedDatasetId = state.datasets[0].datasetId;
  }

  select.disabled = false;
  select.innerHTML = state.datasets
    .map(
      (dataset) =>
        `<option value="${escapeHtml(dataset.datasetId)}" ${
          dataset.datasetId === state.selectedDatasetId ? 'selected' : ''
        }>${escapeHtml(dataset.name)}</option>`
    )
    .join('');

  const dataset = currentDataset();
  if (!dataset) {
    return;
  }
  el('analysis-path').value = dataset.sourcePath;
  el('analysis-type').value = dataset.sourceType;
  el('analysis-dataset-meta').innerHTML = `
    <div class="meta">
      <span>${escapeHtml(dataset.sourceType)}</span>
      <span>${escapeHtml(t('rows_count', { count: dataset.preview?.row_count ?? '—' }))}</span>
      <span>${escapeHtml(kindLabel(dataset.kind))}</span>
    </div>
    ${renderKeyValueRows([
      ['field_source', dataset.sourcePath],
      ['field_schema', dataset.schema.join(', ') || '—'],
      ['field_dataset_id', dataset.datasetId],
    ])}
  `;
  el('save-run-dataset').disabled = !state.selectedRunDetail;
  el('export-analysis-result').disabled = !state.selectedRunDetail;
}

function renderRuns() {
  const list = el('run-list');
  const pageLabel = el('run-page-label');
  const prevButton = el('runs-prev-page');
  const nextButton = el('runs-next-page');
  if (!state.runs.length) {
    list.innerHTML = `<div class="empty">${escapeHtml(t('no_runs_yet'))}</div>`;
    pageLabel.textContent = t('page_status', { page: 0, total: 0 });
    prevButton.disabled = true;
    nextButton.disabled = true;
  } else {
    const totalPages = Math.max(1, Math.ceil(state.runs.length / state.runsPageSize));
    if (state.runsPage > totalPages) {
      state.runsPage = totalPages;
    }
    const start = (state.runsPage - 1) * state.runsPageSize;
    const pageRuns = state.runs.slice(start, start + state.runsPageSize);
    pageLabel.textContent = t('page_status', { page: state.runsPage, total: totalPages });
    prevButton.disabled = state.runsPage <= 1;
    nextButton.disabled = state.runsPage >= totalPages;
    list.innerHTML = pageRuns
      .map(
        (run) => `
          <div class="list-item ${run.run_id === state.selectedRunId ? 'active' : ''}" data-run-id="${escapeHtml(run.run_id)}">
            <h4>${escapeHtml(run.run_name || run.run_id)}</h4>
            <div class="meta">
              <span>${escapeHtml(run.status)}</span>
              <span>${escapeHtml(run.action)}</span>
              <span>${escapeHtml(`${run.artifact_count ?? 0} ${t('label_artifacts')}`)}</span>
            </div>
          </div>
        `
      )
      .join('');
    list.querySelectorAll('[data-run-id]').forEach((node) => {
      node.addEventListener('click', async () => {
        state.selectedRunId = node.getAttribute('data-run-id');
        renderRuns();
        await renderRunDetail();
      });
    });
  }
}

async function renderRunDetail() {
  const detail = el('run-detail');
  if (!state.selectedRunId) {
    detail.innerHTML = `<div class="empty">${escapeHtml(t('run_detail_empty'))}</div>`;
    return;
  }
  detail.innerHTML = `<div class="empty">${escapeHtml(t('loading_run_detail'))}</div>`;
  try {
    const payload = await api(`/api/runs/${encodeURIComponent(state.selectedRunId)}/result?limit=20`);
    state.selectedRunDetail = payload;
    detail.innerHTML = `
      <div class="meta">
        <span>${escapeHtml(payload.run.status)}</span>
        <span>${escapeHtml(payload.run.action)}</span>
        <span>${escapeHtml(t('rows_count', { count: payload.preview.row_count ?? '—' }))}</span>
      </div>
      ${renderKeyValueRows([
        ['field_run_id', payload.run.run_id],
        ['field_artifact_id', payload.artifact.artifact_id],
        ['field_artifact_uri', payload.artifact.uri],
        ['field_schema', (payload.artifact.schema_json || []).join(', ') || '—'],
      ])}
      <div class="actions">
        <button type="button" class="ghost" id="export-run-detail-file">${escapeHtml(t('export_file'))}</button>
        <button type="button" class="ghost" id="save-run-detail-dataset">${escapeHtml(t('save_result_action'))}</button>
      </div>
      ${renderPreviewTable({
        schema: payload.preview.schema || payload.artifact.schema_json || [],
        rows: payload.preview.rows || [],
      })}
    `;
    const exportButton = el('export-run-detail-file');
    const saveButton = el('save-run-detail-dataset');
    if (exportButton) {
      exportButton.addEventListener('click', exportSelectedRunArtifact);
    }
    if (saveButton) {
      saveButton.addEventListener('click', saveResultAsDataset);
    }
  } catch (error) {
    detail.innerHTML = `<div class="empty">${escapeHtml(t('run_detail_failed', { error: error.message }))}</div>`;
  }
}

async function loadServiceInfo() {
  state.serviceInfo = await window.velariaShell.getServiceInfo();
  const health = await api('/health');
  const status = el('service-status');
  const meta = el('service-meta');
  status.textContent = t('status_ready_on', { port: health.port });
  status.classList.remove('warn');
  meta.textContent = t('status_packaged', {
    packaged: state.serviceInfo.packaged,
    version: health.version,
  });
}

async function refreshRuns() {
  const payload = await api('/api/runs?limit=100');
  state.runs = payload.runs || [];
  if (!state.runs.length) {
    state.runsPage = 1;
  }
  if (!state.selectedRunId && state.runs[0]) {
    state.selectedRunId = state.runs[0].run_id;
  }
  renderHome();
  renderRuns();
  if (state.view === 'runs') {
    await renderRunDetail();
  }
}

async function pickFile(targetId) {
  const selected = await window.velariaShell.pickFile();
  if (selected) {
    el(targetId).value = selected;
  }
}

function importPayloadFromForm() {
  const inputType = el('import-type').value;
  const payload = {
    input_path: el('import-path').value.trim(),
    input_type: inputType,
    delimiter: el('import-delimiter').value,
    dataset_name: el('import-dataset-name').value.trim(),
  };
  const columns = el('import-columns').value.trim();
  const regexPattern = el('import-regex-pattern').value.trim();
  if (inputType === 'json') {
    payload.columns = columns;
  } else if (inputType === 'line') {
    payload.mappings = columns;
    payload.regex_pattern = regexPattern || undefined;
    payload.line_mode = regexPattern ? 'regex' : 'split';
  }
  return payload;
}

async function submitImport(event) {
  event.preventDefault();
  const previewBox = el('import-preview');
  previewBox.innerHTML = `<div class="empty">${escapeHtml(t('loading_preview'))}</div>`;
  try {
    const result = await api('/api/import/preview', {
      method: 'POST',
      body: JSON.stringify(importPayloadFromForm()),
    });
    state.pendingImport = result;
    el('save-import-dataset').disabled = false;
    renderImportSteps();
    previewBox.innerHTML = `
      <div class="meta">
        <span>${escapeHtml(result.dataset.name)}</span>
        <span>${escapeHtml(result.dataset.source_type)}</span>
        <span>${escapeHtml(t('rows_count', { count: result.preview.row_count }))}</span>
      </div>
      ${renderPreviewTable(result.preview)}
    `;
  } catch (error) {
    state.pendingImport = null;
    el('save-import-dataset').disabled = true;
    renderImportSteps();
    previewBox.innerHTML = `<div class="empty">${escapeHtml(t('preview_failed', { error: error.message }))}</div>`;
  }
}

function savePendingImportAsDataset() {
  if (!state.pendingImport) {
    return;
  }
  const record = createDatasetRecord({
    name: state.pendingImport.dataset.name,
    sourceType: state.pendingImport.dataset.source_type,
    sourcePath: state.pendingImport.dataset.source_path,
    preview: state.pendingImport.preview,
    kind: 'imported',
  });
  upsertDataset(record);
  state.selectedDatasetId = record.datasetId;
  renderHome();
  renderDatasets();
  renderAnalyze();
  state.pendingImport = null;
  renderImportSteps();
  setView('analyze');
}

function applyPresetQuery() {
  const preset = el('analysis-preset').value;
  const table = el('analysis-table').value.trim() || 'input_table';
  if (preset === 'preview') {
    el('analysis-query').value = `SELECT * FROM ${table} LIMIT 20`;
    return;
  }
  if (preset === 'filter') {
    el('analysis-query').value = `SELECT * FROM ${table} WHERE score > 20 LIMIT 50`;
    return;
  }
  if (preset === 'aggregate') {
    el('analysis-query').value = `SELECT score, COUNT(*) AS cnt FROM ${table} GROUP BY score ORDER BY cnt DESC LIMIT 20`;
  }
}

async function submitAnalysis(event) {
  event.preventDefault();
  const resultBox = el('analysis-result');
  resultBox.innerHTML = `<div class="empty">${escapeHtml(t('running_analysis'))}</div>`;
  const payload = {
    input_path: el('analysis-path').value.trim(),
    input_type: el('analysis-type').value,
    table: el('analysis-table').value.trim() || 'input_table',
    query: el('analysis-query').value,
    run_name: `analysis-${new Date().toISOString()}`,
    description: 'Desktop workbench analysis run',
  };
  try {
    const runPayload = await api('/api/runs/file-sql', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    state.lastAnalysisPayload = runPayload;
    state.selectedRunId = runPayload.run_id;
    const resultPayload = await api(`/api/runs/${encodeURIComponent(runPayload.run_id)}/result?limit=20`);
    state.selectedRunDetail = resultPayload;
    el('save-run-dataset').disabled = false;
    el('export-analysis-result').disabled = false;
    resultBox.innerHTML = `
      <div class="meta">
        <span>${escapeHtml(runPayload.run.status)}</span>
        <span>${escapeHtml(runPayload.run.run_id)}</span>
        <span>${escapeHtml(t('rows_count', { count: resultPayload.preview.row_count }))}</span>
      </div>
      ${renderKeyValueRows([
        ['field_table', runPayload.result.table],
        ['field_query', runPayload.result.query],
        ['field_artifact', resultPayload.artifact.artifact_id],
      ])}
      <div class="actions">
        <button type="button" class="ghost" id="export-analysis-result-inline">${escapeHtml(t('export_result_file'))}</button>
      </div>
      ${renderPreviewTable({
        schema: resultPayload.preview.schema || resultPayload.artifact.schema_json || [],
        rows: resultPayload.preview.rows || [],
      })}
    `;
    const exportInline = el('export-analysis-result-inline');
    if (exportInline) {
      exportInline.addEventListener('click', exportSelectedRunArtifact);
    }
    await refreshRuns();
  } catch (error) {
    state.lastAnalysisPayload = null;
    state.selectedRunDetail = null;
    el('save-run-dataset').disabled = true;
    el('export-analysis-result').disabled = true;
    resultBox.innerHTML = `<div class="empty">${escapeHtml(t('run_failed', { error: error.message }))}</div>`;
  }
}

async function exportFileAtPath(sourcePath) {
  try {
    const result = await window.velariaShell.exportFile({ sourcePath });
    if (result?.cancelled) {
      return { ok: false, cancelled: true };
    }
    return { ok: true, destination: result.destination };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

function prependNotice(target, html) {
  if (!target) {
    return;
  }
  target.innerHTML = `${html}${target.innerHTML}`;
}

async function exportCurrentDataset() {
  const dataset = currentDataset();
  if (!dataset) {
    return;
  }
  const result = await exportFileAtPath(dataset.sourcePath);
  const detail = el('dataset-detail');
  if (result.ok) {
    prependNotice(detail, `<div class="notice">${escapeHtml(t('export_success', { path: result.destination }))}</div>`);
    return;
  }
  if (result.cancelled) {
    prependNotice(detail, `<div class="notice">${escapeHtml(t('export_cancelled'))}</div>`);
    return;
  }
  prependNotice(detail, `<div class="notice error">${escapeHtml(t('export_failed', { error: result.error }))}</div>`);
}

async function exportSelectedRunArtifact() {
  const detail = state.selectedRunDetail;
  if (!detail?.artifact?.uri) {
    return;
  }
  const sourcePath = decodeFileUri(detail.artifact.uri);
  const result = await exportFileAtPath(sourcePath);
  const message = result.ok
    ? `<div class="notice">${escapeHtml(t('export_success', { path: result.destination }))}</div>`
    : result.cancelled
      ? `<div class="notice">${escapeHtml(t('export_cancelled'))}</div>`
      : `<div class="notice error">${escapeHtml(t('export_failed', { error: result.error }))}</div>`;
  prependNotice(el('run-detail'), message);
  prependNotice(el('analysis-result'), message);
}

function saveResultAsDataset() {
  const detail = state.selectedRunDetail;
  if (!detail) {
    return;
  }
  const record = createDatasetRecord({
    name: `result-${detail.run.run_id.slice(0, 12)}`,
    sourceType: detail.artifact.format === 'arrow' ? 'arrow' : detail.artifact.format,
    sourcePath: decodeFileUri(detail.artifact.uri),
    preview: {
      schema: detail.preview.schema || detail.artifact.schema_json || [],
      rows: detail.preview.rows || [],
      row_count: detail.preview.row_count,
      truncated: detail.preview.truncated,
    },
    kind: 'result',
    description: `Saved from run ${detail.run.run_id}`,
  });
  upsertDataset(record);
  state.selectedDatasetId = record.datasetId;
  renderHome();
  renderDatasets();
  renderAnalyze();
  setView('data');
}

function setLocale(locale) {
  state.locale = locale;
  window.localStorage.setItem(LOCALE_KEY, locale);
  applyStaticI18n();
  setView(state.view);
  renderHome();
  renderDatasets();
  renderAnalyze();
  renderRuns();
  renderImportSteps();
  if (state.selectedRunId) {
    renderRunDetail();
  }
}

function renderImportSteps() {
  const hasPath = Boolean(el('import-path')?.value?.trim());
  const hasPreview = Boolean(state.pendingImport);
  const step1 = el('import-step-1');
  const step2 = el('import-step-2');
  const step3 = el('import-step-3');
  if (!step1 || !step2 || !step3) {
    return;
  }
  step1.className = `step-pill ${hasPath ? 'done' : 'active'}`.trim();
  step2.className = `step-pill ${hasPreview ? 'done' : hasPath ? 'active' : ''}`.trim();
  step3.className = `step-pill ${hasPreview ? 'active' : ''}`.trim();
}

function bindEvents() {
  document.querySelectorAll('.nav button[data-view]').forEach((button) => {
    button.addEventListener('click', () => setView(button.getAttribute('data-view')));
  });
  el('lang-en').addEventListener('click', () => setLocale('en'));
  el('lang-zh').addEventListener('click', () => setLocale('zh'));
  el('quick-import').addEventListener('click', () => setView('data'));
  el('quick-analyze').addEventListener('click', () => setView('analyze'));
  el('quick-runs').addEventListener('click', () => setView('runs'));
  el('pick-import-file').addEventListener('click', () => pickFile('import-path'));
  el('pick-analysis-file').addEventListener('click', () => pickFile('analysis-path'));
  el('import-form').addEventListener('submit', submitImport);
  el('import-path').addEventListener('input', renderImportSteps);
  el('import-type').addEventListener('change', renderImportSteps);
  el('analysis-form').addEventListener('submit', submitAnalysis);
  el('save-import-dataset').addEventListener('click', savePendingImportAsDataset);
  el('dataset-export').addEventListener('click', exportCurrentDataset);
  el('save-run-dataset').addEventListener('click', saveResultAsDataset);
  el('export-analysis-result').addEventListener('click', exportSelectedRunArtifact);
  el('refresh-datasets').addEventListener('click', renderDatasets);
  el('refresh-runs').addEventListener('click', refreshRuns);
  el('runs-prev-page').addEventListener('click', () => {
    if (state.runsPage > 1) {
      state.runsPage -= 1;
      renderRuns();
    }
  });
  el('runs-next-page').addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(state.runs.length / state.runsPageSize));
    if (state.runsPage < totalPages) {
      state.runsPage += 1;
      renderRuns();
    }
  });
  el('dataset-analyze').addEventListener('click', () => setView('analyze'));
  el('dataset-remove').addEventListener('click', () => {
    const dataset = currentDataset();
    if (!dataset) {
      return;
    }
    removeDataset(dataset.datasetId);
    renderHome();
    renderDatasets();
    renderAnalyze();
  });
  el('analysis-dataset-select').addEventListener('change', (event) => {
    state.selectedDatasetId = event.target.value || null;
    renderDatasets();
    renderAnalyze();
  });
  el('analysis-preset').addEventListener('change', applyPresetQuery);
}

async function bootstrap() {
  state.datasets = loadDatasets();
  if (!state.selectedDatasetId && state.datasets[0]) {
    state.selectedDatasetId = state.datasets[0].datasetId;
  }
  bindEvents();
  applyStaticI18n();
  setView(state.view);
  renderHome();
  renderDatasets();
  renderAnalyze();
  renderImportSteps();
  await loadServiceInfo();
  await refreshRuns();
}

bootstrap().catch((error) => {
  el('service-status').textContent = t('status_bootstrap_failed');
  el('service-status').classList.add('warn');
  el('service-meta').textContent = error.message;
});

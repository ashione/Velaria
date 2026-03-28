// @ts-nocheck
declare const React: any;
declare const ReactDOM: any;

interface JobSnapshot {
  job_id: string;
  state: string;
  status_code: string;
  client_node: string;
  worker_node: string;
  payload: string;
  sql: string;
  result_payload: string;
  chain: {
    chain_id: string;
    state: string;
    status_code: string;
    task_ids: string[];
  };
  task: {
    task_id: string;
    state: string;
    status_code: string;
    worker_id: string;
  };
}

interface JobListPayload {
  count: number;
  jobs: JobSnapshot[];
}

interface SubmitResult {
  ok: boolean;
  job_id: string;
  state?: string;
  status_code?: string;
  payload?: string;
  message?: string;
}

const { useEffect, useCallback, useState, createElement: h } = React;

const sqlPresets = [
  {
    name: '默认聚合',
    sql: 'SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token',
  },
  {
    name: '按 SQL 过滤',
    sql: 'SELECT token, SUM(score) AS total_score FROM rpc_input WHERE score > 0 GROUP BY token',
  },
  {
    name: '创建表',
    sql: 'CREATE TABLE users (id,name,score)',
  },
  {
    name: '插入一条',
    sql: 'INSERT INTO users VALUES (1, "alice", 10)',
  },
  {
    name: '按列插入',
    sql: 'INSERT INTO users (id, name, score) VALUES (2, "bob", 20)',
  },
];

interface ComplexSqlStep {
  label: string;
  payload: string;
  sql: string;
}

const complexSqlDemos: Array<{ name: string; steps: ComplexSqlStep[] }> = [
  {
    name: '复杂分析 Demo（本地 DDL/DML + 最终查询）',
    steps: [
      {
        label: 'Step 1: 创建用户明细表',
        payload: '',
        sql: "CREATE TABLE app_users (user_id INT, token STRING, score INT, region STRING)",
      },
      {
        label: 'Step 2: 写入用户明细',
        payload: '',
        sql: "INSERT INTO app_users VALUES (1, 'alice', 25, 'apac'), (2, 'bob', 18, 'emea'), (3, 'claire', 34, 'na'), (4, 'david', 11, 'apac'), (5, 'ella', 7, 'na')",
      },
      {
        label: 'Step 3: 创建交易事实表',
        payload: '',
        sql: "CREATE TABLE app_actions (user_id INT, action STRING, score INT)",
      },
      {
        label: 'Step 4: 写入交易事实',
        payload: '',
        sql: "INSERT INTO app_actions VALUES (1, 'view', 5), (1, 'purchase', 20), (2, 'view', 12), (2, 'click', 6), (3, 'purchase', 30), (4, 'view', 4), (5, 'click', 11)",
      },
      {
        label: 'Step 5: 基于 join 的分组计算',
        payload: '',
        sql: "CREATE TABLE app_region_summary (region STRING, total_score INT, user_count INT)",
      },
      {
        label: 'Step 6: 写入分组结果',
        payload: '',
        sql: "INSERT INTO app_region_summary SELECT u.region AS region, SUM(a.score) AS total_score, COUNT(*) AS user_count FROM app_users AS u INNER JOIN app_actions AS a ON u.user_id = a.user_id WHERE a.score > 6 GROUP BY u.region HAVING SUM(a.score) > 15",
      },
      {
        label: 'Step 7: 查看汇总结果',
        payload: '',
        sql: "SELECT region, total_score, user_count FROM app_region_summary WHERE total_score > 10 LIMIT 5",
      },
    ],
  },
];

const api = {
  async listJobs(): Promise<JobListPayload> {
    const response = await fetch('/api/jobs');
    if (!response.ok) {
      throw new Error('列表查询失败');
    }
    return (await response.json()) as JobListPayload;
  },
  async getJob(jobId: string): Promise<JobSnapshot> {
    const response = await fetch(`/api/jobs/${jobId}`);
    if (!response.ok) {
      throw new Error('详情查询失败');
    }
    return (await response.json()) as JobSnapshot;
  },
  async submit(payload: string, sql: string): Promise<SubmitResult> {
    const params = new URLSearchParams();
    params.set('payload', payload);
    params.set('sql', sql);
    const response = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
    });
    const body = (await response.json()) as SubmitResult & { message?: string };
    if (!response.ok || !body.ok) {
      throw new Error(body.message || `提交失败: ${response.status}`);
    }
    return body;
  },
};

function stateClassName(state: string): string {
  return state === 'FINISHED' || state === 'SUCCEEDED' || state === 'JOB_FINISHED'
      ? 'ok'
      : 'warn';
}

function shorten(value: string | undefined, maxLength: number): string {
  if (!value) return '-';
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength)}...`;
}

function JobList({
  jobs,
  onSelect,
}: {
  jobs: JobSnapshot[];
  onSelect: (jobId: string) => void;
}) {
  return h(
    'div',
    { className: 'grid' },
    h(
      'table',
      null,
      h(
        'thead',
        null,
        h(
          'tr',
          null,
          h('th', null, 'JobId'),
          h('th', null, 'State'),
          h('th', null, 'SQL'),
          h('th', null, 'Worker'),
          h('th', null, '链路/任务'),
          h('th', null, '结果摘要')
        )
      ),
      h(
        'tbody',
        null,
        ...jobs.map((job) =>
          h(
            'tr',
            { key: job.job_id },
            h(
              'td',
              null,
              h(
                'a',
                {
                  href: '#',
                  onClick: (event: Event) => {
                    event.preventDefault();
                    onSelect(job.job_id);
                  },
                },
                job.job_id
              )
            ),
            h(
              'td',
              null,
              h('div', { className: stateClassName(job.state) }, job.state),
              h('div', { className: 'tiny' }, job.status_code)
            ),
            h('td', null, shorten(job.sql || job.payload, 52)),
            h('td', null, job.worker_node || '-'),
            h(
              'td',
              null,
              h('div', null, job.chain.chain_id || '-'),
              h('div', { className: 'tiny' }, `${job.task.task_id || '-'} / ${job.chain.task_ids.length} tasks`)
            ),
            h('td', null, shorten(job.result_payload || '-', 80))
          )
        )
      )
    )
  );
}

function App() {
  const [jobs, setJobs] = useState<JobSnapshot[]>([]);
  const [payload, setPayload] = useState('demo payload');
  const [sql, setSql] = useState('SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token');
  const [statusText, setStatusText] = useState('ready');
  const [hintText, setHintText] = useState('');
  const [detailText, setDetailText] = useState('');
  const [selectedJobId, setSelectedJobId] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const refresh = useCallback(async () => {
    const data = await api.listJobs();
    setJobs(Array.isArray(data.jobs) ? data.jobs : []);
    setHintText(`更新于 ${new Date().toLocaleTimeString()}，共 ${data.count} 条`);
  }, []);

  useEffect(() => {
    refresh().catch((error: unknown) => {
      setHintText(error instanceof Error ? error.message : '刷新失败');
    });

    const timer = window.setInterval(() => {
      refresh().catch(() => {});
    }, 1000);
    return () => {
      window.clearInterval(timer);
    };
  }, [refresh]);

  const submit = async () => {
    setSubmitting(true);
    setStatusText('提交中...');
    try {
      if (!payload && !sql) {
        setStatusText('payload 或 sql 至少填写一个');
        return;
      }
      const result = await api.submit(payload, sql);
      const stateText = result.state || 'SUBMITTED';
      const statusCode = result.status_code || 'JOB_SUBMITTED';
      setStatusText(`任务已提交：${result.job_id}（${stateText}/${statusCode}）`);
      await refresh();
      const detail = await api.getJob(result.job_id);
      setSelectedJobId(result.job_id);
      setDetailText(JSON.stringify(detail, null, 2));
    } catch (error: unknown) {
      setStatusText(error instanceof Error ? error.message : String(error));
    } finally {
      setSubmitting(false);
    }
  };

  const showDetail = async (jobId: string) => {
    try {
      const detail = await api.getJob(jobId);
      setSelectedJobId(jobId);
      setDetailText(JSON.stringify(detail, null, 2));
    } catch (error: unknown) {
      setDetailText(error instanceof Error ? error.message : String(error));
    }
  };

  const applyPreset = (nextSql: string) => {
    setSql(nextSql);
    setStatusText(`已加载 SQL 模板`);
  };

  const applyComplexDemo = async (demoName: string, steps: ComplexSqlStep[]) => {
    setSubmitting(true);
    setStatusText(`开始执行 ${demoName}`);
    setDetailText('');
    const logs: string[] = [];
    let lastJobId = '';
    let finalDetail = '';
    try {
      for (let i = 0; i < steps.length; i += 1) {
        const step = steps[i];
        setStatusText(`执行 ${demoName}：${step.label}`);
        const result = await api.submit(step.payload, step.sql);
        lastJobId = result.job_id;
        const stateText = result.state || 'SUBMITTED';
        const statusCode = result.status_code || 'JOB_SUBMITTED';
        logs.push(`${step.label}: ${result.job_id} -> ${stateText}/${statusCode}`);
        await refresh();
        if (i === steps.length - 1) {
          const detail = await api.getJob(result.job_id);
          setSelectedJobId(result.job_id);
          finalDetail = JSON.stringify(detail, null, 2);
        }
      }
      setStatusText(`复杂 Demo 已提交：${lastJobId}`);
      if (finalDetail) {
        setDetailText(finalDetail);
      } else {
        setDetailText(logs.join('\n'));
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      logs.push(`失败: ${message}`);
      setStatusText(message);
    } finally {
      if (!finalDetail && logs.length > 0) {
        setDetailText(logs.join('\n'));
      }
      setSubmitting(false);
    }
  };

  return h(
    'div',
    { className: 'panel-grid' },
    h(
      'div',
      { className: 'panel' },
      h('h3', null, '提交任务'),
      h('div', { className: 'row', style: { marginBottom: '10px' } },
        h('label', { className: 'tiny', style: { display: 'block', width: '100%' } }, 'Payload（用于 rpc_input / 调试）'),
        h('input', {
          className: 'small',
          value: payload,
          onInput: (event: InputEvent) => {
            const target = event.target as HTMLInputElement | null;
            if (target) setPayload(target.value);
          },
          placeholder: 'demo payload'
        })
      ),
      h('div', { style: { marginBottom: '10px' } },
        h('label', { htmlFor: 'sqlInput' }, 'SQL（可选）'),
        h('textarea', {
          id: 'sqlInput',
          value: sql,
          onInput: (event: InputEvent) => {
            const target = event.target as HTMLTextAreaElement | null;
            if (target) setSql(target.value);
          },
          placeholder: 'SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token'
        }),
        h('div', { className: 'tiny', style: { marginTop: '4px' } }, '支持 CREATE TABLE / INSERT / SELECT 常用语法')
      ),
      h(
        'div',
        { className: 'row', style: { marginBottom: '10px' } },
        ...sqlPresets.map((item) =>
          h(
            'button',
            {
              type: 'button',
              onClick: () => applyPreset(item.sql),
              style: { minWidth: 'auto', paddingLeft: '12px', paddingRight: '12px' },
            },
            item.name
          )
        )
      ),
      h(
        'div',
        { className: 'row', style: { marginBottom: '10px' } },
        ...complexSqlDemos.map((demo) =>
          h(
            'button',
            {
              type: 'button',
              disabled: submitting,
              onClick: () => applyComplexDemo(demo.name, demo.steps),
              style: { minWidth: 'auto', paddingLeft: '12px', paddingRight: '12px' },
            },
            `复杂Demo：${demo.name}`
          )
        )
      ),
      h(
        'div',
        { className: 'row' },
        h('button', { type: 'button', onClick: submit, disabled: submitting }, submitting ? '提交中…' : '提交任务'),
        h('span', { className: 'muted', id: 'submitStatus' }, statusText)
      )
    ),
    h(
      'div',
      { className: 'panel' },
      h('h3', null, '运行作业'),
      h(
        'div',
        { className: 'row', style: { marginBottom: '10px' } },
        h('button', { type: 'button', onClick: refresh }, '刷新'),
        h('span', { className: 'muted', id: 'refreshHint' }, hintText)
      ),
      h(
        'div',
        { className: 'muted', style: { marginBottom: '6px' } },
        `${jobs.length} 条作业（最近排序）`
      ),
      h(JobList, { jobs, onSelect: showDetail })
    ),
    h(
      'div',
      { className: 'panel' },
      h('h3', null, '作业详情'),
      h('div', { className: 'muted', style: { marginBottom: '6px' } },
        selectedJobId ? `JobId: ${selectedJobId}` : '点击任务行查看详情，或提交后自动展开'
      ),
      h('pre', { className: 'code' }, detailText || '暂无详情')
    )
  );
}

const root = document.getElementById('dashboard-root');
if (root) {
  if (typeof ReactDOM.createRoot === 'function') {
    ReactDOM.createRoot(root).render(h(App));
  } else {
    ReactDOM.render(h(App), root);
  }
}

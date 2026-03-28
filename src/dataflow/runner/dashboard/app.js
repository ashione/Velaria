const { useEffect, useCallback, useState, createElement: h } = React;
const api = {
    async listJobs() {
        const response = await fetch('/api/jobs');
        if (!response.ok) {
            throw new Error('列表查询失败');
        }
        return (await response.json());
    },
    async getJob(jobId) {
        const response = await fetch(`/api/jobs/${jobId}`);
        if (!response.ok) {
            throw new Error('详情查询失败');
        }
        return (await response.json());
    },
    async submit(payload, sql) {
        const params = new URLSearchParams();
        params.set('payload', payload);
        params.set('sql', sql);
        const response = await fetch('/api/jobs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params.toString(),
        });
        const body = await response.json();
        if (!response.ok || !body.ok) {
            throw new Error(body.message || `提交失败: ${response.status}`);
        }
        return body;
    },
};
function stateClassName(state) {
    return state === 'FINISHED' || state === 'SUCCEEDED' || state === 'JOB_FINISHED'
        ? 'ok'
        : 'warn';
}
function JobList({ jobs, onSelect, }) {
    return h('div', { className: 'grid' }, h('table', null, h('thead', null, h('tr', null, h('th', null, 'JobId'), h('th', null, 'State'), h('th', null, 'Worker'), h('th', null, 'Chain'), h('th', null, '任务摘要'), h('th', null, 'Task'))), h('tbody', null, ...jobs.map((job) => h('tr', { key: job.job_id }, h('td', null, h('a', {
        href: '#',
        onClick: (event) => {
            event.preventDefault();
            onSelect(job.job_id);
        },
    }, job.job_id)), h('td', null, h('div', { className: stateClassName(job.state) }, job.state), h('div', { className: 'tiny' }, job.status_code)), h('td', null, job.worker_node || '-'), h('td', null, job.chain.chain_id || '-', h('div', { className: 'tiny' }, `${(job.chain.task_ids || []).length} tasks`)), h('td', null, job.result_payload || job.payload || '-'), h('td', null, job.task.task_id || '-'))))));
}
function App() {
    const [jobs, setJobs] = useState([]);
    const [payload, setPayload] = useState('demo payload');
    const [sql, setSql] = useState('SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token');
    const [statusText, setStatusText] = useState('ready');
    const [hintText, setHintText] = useState('');
    const [detailText, setDetailText] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const refresh = useCallback(async () => {
        const data = await api.listJobs();
        setJobs(Array.isArray(data.jobs) ? data.jobs : []);
        setHintText(`更新于 ${new Date().toLocaleTimeString()}，共 ${data.count} 条`);
    }, []);
    useEffect(() => {
        refresh().catch((error) => {
            setHintText(error instanceof Error ? error.message : '刷新失败');
        });
        const timer = window.setInterval(() => {
            refresh().catch(() => { });
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
            setStatusText(`任务已提交：${result.job_id}`);
            await refresh();
            const detail = await api.getJob(result.job_id);
            setDetailText(JSON.stringify(detail, null, 2));
        }
        catch (error) {
            setStatusText(error instanceof Error ? error.message : String(error));
        }
        finally {
            setSubmitting(false);
        }
    };
    const showDetail = async (jobId) => {
        try {
            const detail = await api.getJob(jobId);
            setDetailText(JSON.stringify(detail, null, 2));
        }
        catch (error) {
            setDetailText(error instanceof Error ? error.message : String(error));
        }
    };
    return h('div', { className: 'panel-grid' }, h('div', { className: 'panel' }, h('h3', null, '提交任务'), h('div', { className: 'row', style: { marginBottom: '10px' } }, h('input', {
        className: 'small',
        value: payload,
        onInput: (event) => {
            const target = event.target;
            if (target)
                setPayload(target.value);
        },
        placeholder: 'payload（示例：demo payload）'
    })), h('div', { style: { marginBottom: '10px' } }, h('label', { htmlFor: 'sqlInput' }, 'SQL（可选）'), h('textarea', {
        id: 'sqlInput',
        value: sql,
        onInput: (event) => {
            const target = event.target;
            if (target)
                setSql(target.value);
        },
        placeholder: 'SELECT token, SUM(score) AS total_score FROM rpc_input GROUP BY token'
    })), h('div', { className: 'row' }, h('button', { type: 'button', onClick: submit, disabled: submitting }, submitting ? '提交中…' : '提交任务'), h('span', { className: 'muted', id: 'submitStatus' }, statusText))), h('div', { className: 'panel' }, h('h3', null, '运行作业'), h('div', { className: 'row', style: { marginBottom: '10px' } }, h('button', { type: 'button', onClick: refresh }, '刷新'), h('span', { className: 'muted', id: 'refreshHint' }, hintText)), h('div', { className: 'muted', style: { marginBottom: '6px' } }, `${jobs.length} 条作业`), h(JobList, { jobs, onSelect: showDetail })), h('div', { className: 'panel' }, h('h3', null, '运行详情'), h('pre', { className: 'code' }, detailText || '点击任务行可查看详情 JSON')));
}
const root = document.getElementById('dashboard-root');
if (root) {
    if (typeof ReactDOM.createRoot === 'function') {
        ReactDOM.createRoot(root).render(h(App));
    }
    else {
        ReactDOM.render(h(App), root);
    }
}

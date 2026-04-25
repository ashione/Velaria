import { useMemo } from 'react';
import type {
  ExternalSourceRecord,
  FocusEventRecord,
  MonitorFormState,
  MonitorRecord,
  SourceFormState,
  TFunction,
  ViewKey,
} from '../types';
import {
  externalEventColumns,
} from '../types';

export type MonitorsViewProps = {
  t: TFunction;
  sources: ExternalSourceRecord[];
  monitors: MonitorRecord[];
  focusEvents: FocusEventRecord[];
  sourceForm: SourceFormState;
  setSourceForm: React.Dispatch<React.SetStateAction<SourceFormState>>;
  monitorForm: MonitorFormState;
  setMonitorForm: React.Dispatch<React.SetStateAction<MonitorFormState>>;
  monitorMessage: { kind: 'info' | 'error'; text: string } | null;
  /* callbacks */
  createExternalSource: (event: React.FormEvent) => void;
  createMonitorFromIntent: (event: React.FormEvent) => void;
  monitorAction: (monitorId: string, action: 'validate' | 'enable' | 'disable' | 'run' | 'delete') => void;
  focusEventAction: (eventId: string, action: 'consume' | 'archive') => void;
  refreshMonitors: () => void;
  setSelectedRunId: (id: string | null) => void;
  setView: (view: ViewKey) => void;
};

export function MonitorsView(props: MonitorsViewProps) {
  const {
    t,
    sources,
    monitors,
    focusEvents,
    sourceForm,
    setSourceForm,
    monitorForm,
    setMonitorForm,
    monitorMessage,
    createExternalSource,
    createMonitorFromIntent,
    monitorAction,
    focusEventAction,
    refreshMonitors,
    setSelectedRunId,
    setView,
  } = props;

  const selectedMonitorSource = useMemo(
    () => sources.find((source) => source.source_id === monitorForm.sourceId) || null,
    [sources, monitorForm.sourceId]
  );

  const selectedMonitorColumns = useMemo(
    () => externalEventColumns(selectedMonitorSource),
    [selectedMonitorSource]
  );

  return (
    <section className="section active">
      <div className="grid">
        <section className="panel half">
          <div className="panel-head">
            <h2>{t('sources_title')}</h2>
            <div className="actions">
              <button className="ghost" onClick={() => void refreshMonitors()}>
                {t('refresh_monitors')}
              </button>
            </div>
          </div>
          <div className="panel-body stack">
            {monitorMessage && (
              <div className={`notice ${monitorMessage.kind === 'error' ? 'error' : ''}`}>
                {monitorMessage.text}
              </div>
            )}
            <form onSubmit={createExternalSource}>
              <div className="field-grid">
                <label>
                  <span>{t('source_id')}</span>
                  <input
                    value={sourceForm.sourceId}
                    onChange={(event) =>
                      setSourceForm((current) => ({ ...current, sourceId: event.target.value }))
                    }
                    required
                  />
                </label>
                <label>
                  <span>{t('source_name')}</span>
                  <input
                    value={sourceForm.name}
                    onChange={(event) =>
                      setSourceForm((current) => ({ ...current, name: event.target.value }))
                    }
                  />
                </label>
              </div>
              <div className="field-grid">
                <label>
                  <span>{t('time_field')}</span>
                  <input
                    value={sourceForm.timeField}
                    onChange={(event) =>
                      setSourceForm((current) => ({ ...current, timeField: event.target.value }))
                    }
                  />
                </label>
                <label>
                  <span>{t('type_field')}</span>
                  <input
                    value={sourceForm.typeField}
                    onChange={(event) =>
                      setSourceForm((current) => ({ ...current, typeField: event.target.value }))
                    }
                  />
                </label>
              </div>
              <div className="field-grid">
                <label>
                  <span>{t('key_field')}</span>
                  <input
                    value={sourceForm.keyField}
                    onChange={(event) =>
                      setSourceForm((current) => ({ ...current, keyField: event.target.value }))
                    }
                  />
                </label>
                <label>
                  <span>{t('extra_field')}</span>
                  <input
                    value={sourceForm.priceField}
                    onChange={(event) =>
                      setSourceForm((current) => ({ ...current, priceField: event.target.value }))
                    }
                  />
                </label>
              </div>
              <div className="actions">
                <button type="submit">{t('create_source')}</button>
              </div>
            </form>
            <div className="list">
              {sources.map((source) => (
                <div key={source.source_id} className="list-item">
                  <div className="item-head">
                    <h4>{source.name}</h4>
                    <span className="badge">{source.kind}</span>
                  </div>
                  <div className="meta">
                    <span>{source.source_id}</span>
                    <span>{source.schema_binding?.key_field || '\u2014'}</span>
                  </div>
                </div>
              ))}
              {!sources.length && <div className="empty">{t('no_sources_yet')}</div>}
            </div>
          </div>
        </section>

        <section className="panel half">
          <div className="panel-head">
            <h2>{t('monitors_title')}</h2>
          </div>
          <div className="panel-body stack">
            <form onSubmit={createMonitorFromIntent}>
              <label>
                <span>{t('monitor_name')}</span>
                <input
                  value={monitorForm.name}
                  onChange={(event) =>
                    setMonitorForm((current) => ({ ...current, name: event.target.value }))
                  }
                />
              </label>
              <label>
                <span>{t('intent_text')}</span>
                <input
                  value={monitorForm.intentText}
                  onChange={(event) =>
                    setMonitorForm((current) => ({ ...current, intentText: event.target.value }))
                  }
                  required
                />
              </label>
              <div className="field-grid">
                <label>
                  <span>{t('source_id')}</span>
                  <select
                    value={monitorForm.sourceId}
                    onChange={(event) =>
                      setMonitorForm((current) => ({
                        ...current,
                        sourceId: event.target.value,
                        groupBy: 'source_key,event_type',
                      }))
                    }
                    required
                  >
                    <option value="">{t('no_sources_yet')}</option>
                    {sources.map((source) => (
                      <option key={source.source_id} value={source.source_id}>
                        {source.name}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>{t('execution_mode')}</span>
                  <select
                    value={monitorForm.executionMode}
                    onChange={(event) =>
                      setMonitorForm((current) => ({
                        ...current,
                        executionMode: event.target.value as MonitorFormState['executionMode'],
                      }))
                    }
                  >
                    <option value="stream">stream</option>
                    <option value="batch">batch</option>
                  </select>
                </label>
              </div>
              <div className="field-grid">
                <label>
                  <span>{t('count_threshold')}</span>
                  <input
                    value={monitorForm.countThreshold}
                    onChange={(event) =>
                      setMonitorForm((current) => ({ ...current, countThreshold: event.target.value }))
                    }
                  />
                </label>
                <label>
                  <span>{t('group_by_fields')}</span>
                  <input
                    value={monitorForm.groupBy}
                    onChange={(event) =>
                      setMonitorForm((current) => ({ ...current, groupBy: event.target.value }))
                    }
                  />
                  <small className="brand-sub">
                    {t('monitor_available_columns', { columns: selectedMonitorColumns.join(', ') })}
                  </small>
                </label>
              </div>
              <div className="actions">
                <button type="submit" disabled={!sources.length}>{t('create_monitor')}</button>
              </div>
            </form>
            <div className="list">
              {monitors.map((monitor) => (
                <div key={monitor.monitor_id} className="list-item">
                  <div className="item-head">
                    <h4>{monitor.name}</h4>
                    <span className="badge">
                      {monitor.state?.status || monitor.validation?.status || t('monitor_status')}
                    </span>
                  </div>
                  <div className="meta">
                    <span>{monitor.execution_mode}</span>
                    <span>{monitor.enabled ? 'enabled' : 'disabled'}</span>
                  </div>
                  <div className="actions" style={{ marginTop: 10 }}>
                    <button className="ghost" onClick={() => void monitorAction(monitor.monitor_id, 'validate')}>
                      {t('monitor_validate')}
                    </button>
                    {monitor.enabled ? (
                      <button className="ghost" onClick={() => void monitorAction(monitor.monitor_id, 'disable')}>
                        {t('monitor_disable')}
                      </button>
                    ) : (
                      <button className="ghost" onClick={() => void monitorAction(monitor.monitor_id, 'enable')}>
                        {t('monitor_enable')}
                      </button>
                    )}
                    <button className="ghost" onClick={() => void monitorAction(monitor.monitor_id, 'run')}>
                      {t('monitor_run_now')}
                    </button>
                    <button className="ghost danger-button" onClick={() => void monitorAction(monitor.monitor_id, 'delete')}>
                      {t('monitor_delete')}
                    </button>
                  </div>
                  {monitor.state?.last_error && (
                    <div className="notice error" style={{ marginTop: 10 }}>
                      {monitor.state.last_error}
                    </div>
                  )}
                </div>
              ))}
              {!monitors.length && <div className="empty">{t('no_monitors_yet')}</div>}
            </div>
          </div>
        </section>
      </div>

      <section className="panel" style={{ marginTop: 24 }}>
        <div className="panel-head">
          <h2>{t('focus_events_title')}</h2>
          <div className="actions">
            <button className="ghost" onClick={() => void refreshMonitors()}>
              {t('refresh_focus_events')}
            </button>
          </div>
        </div>
        <div className="panel-body">
          <div className="list">
            {focusEvents.map((focusEvent) => (
              <div key={focusEvent.event_id} className="list-item">
                <div className="item-head">
                  <h4>{focusEvent.title}</h4>
                  <span className="badge">{focusEvent.severity}</span>
                </div>
                <div className="meta">
                  <span>{focusEvent.monitor_id}</span>
                  <span>{focusEvent.status}</span>
                  <span>{focusEvent.triggered_at}</span>
                </div>
                <div className="helper" style={{ marginTop: 10 }}>{focusEvent.summary}</div>
                <div className="mono" style={{ marginTop: 10 }}>
                  {JSON.stringify(focusEvent.key_fields || {}, null, 2)}
                </div>
                <div className="actions" style={{ marginTop: 10 }}>
                  {focusEvent.run_id && (
                    <button
                      className="ghost"
                      onClick={() => {
                        setSelectedRunId(focusEvent.run_id || null);
                        setView('runs');
                      }}
                    >
                      {t('open_run_detail')}
                    </button>
                  )}
                  <button className="ghost" onClick={() => void focusEventAction(focusEvent.event_id, 'consume')}>
                    {t('focus_event_consume')}
                  </button>
                  <button className="ghost" onClick={() => void focusEventAction(focusEvent.event_id, 'archive')}>
                    {t('focus_event_archive')}
                  </button>
                </div>
              </div>
            ))}
            {!focusEvents.length && <div className="empty">{t('no_focus_events_yet')}</div>}
          </div>
        </div>
      </section>
    </section>
  );
}

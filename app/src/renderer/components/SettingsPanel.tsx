import type { AppConfig, TFunction } from '../types';
import { maskSecret } from '../types';

export type SettingsPanelProps = {
  t: TFunction;
  configForm: AppConfig;
  setConfigForm: React.Dispatch<React.SetStateAction<AppConfig>>;
  configMessage: { kind: 'info' | 'error'; text: string } | null;
  setConfigMessage: (message: { kind: 'info' | 'error'; text: string } | null) => void;
  saveConfig: () => void;
};

export function SettingsPanel(props: SettingsPanelProps) {
  const {
    t,
    configForm,
    setConfigForm,
    configMessage,
    setConfigMessage,
    saveConfig,
  } = props;

  return (
    <section className="section active">
      <div className="grid">
        <section className="panel half">
          <div className="panel-head">
            <h2>{t('settings_title')}</h2>
          </div>
          <div className="panel-body stack">
            <div className="helper">{t('settings_hint')}</div>
            {configMessage && (
              <div className={`notice ${configMessage.kind === 'error' ? 'error' : ''}`}>
                {configMessage.text}
              </div>
            )}
            <label>
              <span>{t('bitable_app_id')}</span>
              <input
                value={configForm.bitableAppId}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, bitableAppId: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder={t('bitable_app_id_placeholder')}
              />
            </label>
            <label>
              <span>{t('bitable_app_secret')}</span>
              <input
                type="password"
                value={configForm.bitableAppSecret}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, bitableAppSecret: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder={t('bitable_app_secret_placeholder')}
              />
            </label>
            <div className="helper">
              {configForm.bitableAppSecret
                ? t('settings_secret_preview', { value: maskSecret(configForm.bitableAppSecret) })
                : t('settings_secret_empty')}
            </div>
            <div className="actions">
              <button type="button" onClick={() => void saveConfig()}>
                {t('settings_save')}
              </button>
            </div>
          </div>
        </section>
        <section className="panel half">
          <div className="panel-head">
            <h2>{t('ai_settings_title')}</h2>
          </div>
          <div className="panel-body stack">
            <div className="helper">{t('ai_settings_hint')}</div>
            <label>
              <span>{t('agent_runtime')}</span>
              <select
                value={configForm.agentRuntime}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, agentRuntime: event.target.value as AppConfig['agentRuntime'] }));
                  setConfigMessage(null);
                }}
              >
                <option value="codex">Codex Runtime</option>
                <option value="claude">Claude Code Runtime</option>
              </select>
            </label>
            <label>
              <span>{t('agent_auth_mode')}</span>
              <select
                value={configForm.agentAuthMode}
                onChange={(event) => {
                  const nextMode = event.target.value as AppConfig['agentAuthMode'];
                  setConfigForm((current) => ({
                    ...current,
                    agentAuthMode: nextMode,
                    agentApiKey: nextMode === 'oauth' ? '' : current.agentApiKey,
                  }));
                  setConfigMessage(null);
                }}
              >
                <option value="oauth">{t('agent_auth_oauth')}</option>
                <option value="api_key">{t('agent_auth_api_key')}</option>
              </select>
            </label>
            <label>
              <span>{t('agent_provider')}</span>
              <select
                value={configForm.agentProvider}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, agentProvider: event.target.value as AppConfig['agentProvider'] }));
                  setConfigMessage(null);
                }}
              >
                <option value="openai">OpenAI Compatible</option>
                <option value="anthropic">Anthropic</option>
                <option value="custom">Custom</option>
              </select>
            </label>
            {configForm.agentAuthMode === 'api_key' && (
              <label>
                <span>{t('agent_api_key')}</span>
                <input
                  type="password"
                  value={configForm.agentApiKey}
                  onChange={(event) => {
                    setConfigForm((current) => ({ ...current, agentApiKey: event.target.value }));
                    setConfigMessage(null);
                  }}
                  placeholder="sk-..."
                />
              </label>
            )}
            <label>
              <span>{t('agent_base_url')}</span>
              <input
                value={configForm.agentBaseUrl}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, agentBaseUrl: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder="https://api.openai.com/v1"
              />
            </label>
            <label>
              <span>{t('agent_model')}</span>
              <input
                value={configForm.agentModel}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, agentModel: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder="gpt-5.4-mini"
              />
            </label>
            <div className="helper">
              {configForm.agentAuthMode === 'oauth'
                ? t('agent_oauth_hint')
                : configForm.agentApiKey
                  ? t('settings_secret_preview', { value: maskSecret(configForm.agentApiKey) })
                  : t('agent_no_api_key')}
            </div>
            <div className="actions">
              <button type="button" onClick={() => void saveConfig()}>
                {t('settings_save')}
              </button>
            </div>
          </div>
        </section>
      </div>
    </section>
  );
}

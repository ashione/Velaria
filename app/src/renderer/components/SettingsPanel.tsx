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
              <span>{t('ai_provider')}</span>
              <select
                value={configForm.aiProvider}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, aiProvider: event.target.value as AppConfig['aiProvider'] }));
                  setConfigMessage(null);
                }}
              >
                <option value="openai">OpenAI Compatible</option>
                <option value="claude">Claude (Anthropic)</option>
              </select>
            </label>
            <label>
              <span>{t('ai_api_key')}</span>
              <input
                type="password"
                value={configForm.aiApiKey}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, aiApiKey: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder="sk-..."
              />
            </label>
            <label>
              <span>{t('ai_base_url')}</span>
              <input
                value={configForm.aiBaseUrl}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, aiBaseUrl: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder="https://api.openai.com/v1"
              />
            </label>
            <label>
              <span>{t('ai_model')}</span>
              <input
                value={configForm.aiModel}
                onChange={(event) => {
                  setConfigForm((current) => ({ ...current, aiModel: event.target.value }));
                  setConfigMessage(null);
                }}
                placeholder="gpt-4o-mini"
              />
            </label>
            <div className="helper">
              {configForm.aiApiKey
                ? t('settings_secret_preview', { value: maskSecret(configForm.aiApiKey) })
                : t('ai_no_api_key')}
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

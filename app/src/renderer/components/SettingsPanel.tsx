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
      </div>
    </section>
  );
}

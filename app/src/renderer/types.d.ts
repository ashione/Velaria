export {};

declare global {
  interface Window {
    velariaShell: {
      pickFile: () => Promise<string | null>;
      getServiceInfo: () => Promise<{ baseUrl: string; packaged: boolean }>;
      getConfig: () => Promise<{ bitableAppId?: string; bitableAppSecret?: string }>;
      saveConfig: (payload: {
        bitableAppId?: string;
        bitableAppSecret?: string;
      }) => Promise<{ bitableAppId?: string; bitableAppSecret?: string }>;
      exportFile: (payload: { sourcePath: string; suggestedName?: string }) => Promise<{
        cancelled: boolean;
        destination?: string;
      }>;
    };
  }
}

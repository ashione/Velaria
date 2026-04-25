export {};

declare global {
  interface Window {
    velariaShell: {
      pickFile: () => Promise<string | null>;
      getServiceInfo: () => Promise<{ baseUrl: string; packaged: boolean }>;
      getConfig: () => Promise<{
        bitableAppId?: string;
        bitableAppSecret?: string;
        aiProvider?: string;
        aiApiKey?: string;
        aiBaseUrl?: string;
        aiModel?: string;
      }>;
      saveConfig: (payload: {
        bitableAppId?: string;
        bitableAppSecret?: string;
        aiProvider?: string;
        aiApiKey?: string;
        aiBaseUrl?: string;
        aiModel?: string;
      }) => Promise<{
        bitableAppId?: string;
        bitableAppSecret?: string;
        aiProvider?: string;
        aiApiKey?: string;
        aiBaseUrl?: string;
        aiModel?: string;
      }>;
      exportFile: (payload: { sourcePath: string; suggestedName?: string }) => Promise<{
        cancelled: boolean;
        destination?: string;
      }>;
    };
  }
}

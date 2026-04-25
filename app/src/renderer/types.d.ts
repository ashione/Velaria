export {};

declare global {
  interface Window {
    velariaShell: {
      pickFile: () => Promise<string | null>;
      getServiceInfo: () => Promise<{ baseUrl: string; packaged: boolean }>;
      getConfig: () => Promise<{
        bitableAppId?: string;
        bitableAppSecret?: string;
        agentRuntime?: string;
        agentAuthMode?: string;
        agentProvider?: string;
        agentApiKey?: string;
        agentBaseUrl?: string;
        agentModel?: string;
      }>;
      saveConfig: (payload: {
        bitableAppId?: string;
        bitableAppSecret?: string;
        agentRuntime?: string;
        agentAuthMode?: string;
        agentProvider?: string;
        agentApiKey?: string;
        agentBaseUrl?: string;
        agentModel?: string;
      }) => Promise<{
        bitableAppId?: string;
        bitableAppSecret?: string;
        agentRuntime?: string;
        agentAuthMode?: string;
        agentProvider?: string;
        agentApiKey?: string;
        agentBaseUrl?: string;
        agentModel?: string;
      }>;
      exportFile: (payload: { sourcePath: string; suggestedName?: string }) => Promise<{
        cancelled: boolean;
        destination?: string;
      }>;
    };
  }
}

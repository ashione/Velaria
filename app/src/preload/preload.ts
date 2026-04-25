import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('velariaShell', {
  pickFile: () => ipcRenderer.invoke('shell:pick-file') as Promise<string | null>,
  getServiceInfo: () =>
    ipcRenderer.invoke('shell:get-service-info') as Promise<{ baseUrl: string; packaged: boolean }>,
  getConfig: () =>
    ipcRenderer.invoke('shell:get-config') as Promise<{
      bitableAppId?: string;
      bitableAppSecret?: string;
      agentRuntime?: string;
      agentAuthMode?: string;
      agentProvider?: string;
      agentApiKey?: string;
      agentBaseUrl?: string;
      agentModel?: string;
    }>,
  saveConfig: (payload: {
    bitableAppId?: string;
    bitableAppSecret?: string;
    agentRuntime?: string;
    agentAuthMode?: string;
    agentProvider?: string;
    agentApiKey?: string;
    agentBaseUrl?: string;
    agentModel?: string;
  }) =>
    ipcRenderer.invoke('shell:save-config', payload) as Promise<{
      bitableAppId?: string;
      bitableAppSecret?: string;
      agentRuntime?: string;
      agentAuthMode?: string;
      agentProvider?: string;
      agentApiKey?: string;
      agentBaseUrl?: string;
      agentModel?: string;
    }>,
  exportFile: (payload: { sourcePath: string; suggestedName?: string }) =>
    ipcRenderer.invoke('shell:export-file', payload) as Promise<{
      cancelled: boolean;
      destination?: string;
    }>,
});

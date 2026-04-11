import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('velariaShell', {
  pickFile: () => ipcRenderer.invoke('shell:pick-file') as Promise<string | null>,
  getServiceInfo: () =>
    ipcRenderer.invoke('shell:get-service-info') as Promise<{ baseUrl: string; packaged: boolean }>,
  exportFile: (payload: { sourcePath: string; suggestedName?: string }) =>
    ipcRenderer.invoke('shell:export-file', payload) as Promise<{
      cancelled: boolean;
      destination?: string;
    }>,
});

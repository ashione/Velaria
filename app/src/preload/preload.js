const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('velariaShell', {
  pickFile: () => ipcRenderer.invoke('shell:pick-file'),
  getServiceInfo: () => ipcRenderer.invoke('shell:get-service-info'),
  exportFile: (payload) => ipcRenderer.invoke('shell:export-file', payload),
});

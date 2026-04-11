const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const path = require('node:path');
const fs = require('node:fs/promises');
const { spawn } = require('node:child_process');

const DEFAULT_PORT = parseInt(process.env.VELARIA_SERVICE_PORT || '37491', 10);

let mainWindow = null;
let sidecarProcess = null;

function repoRoot() {
  return path.resolve(__dirname, '..', '..', '..');
}

function serviceBaseUrl() {
  return `http://127.0.0.1:${DEFAULT_PORT}`;
}

function sidecarExecutablePath() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'bin', 'velaria-service', 'velaria-service');
  }
  return null;
}

function startSidecar() {
  if (sidecarProcess) {
    return sidecarProcess;
  }

  if (app.isPackaged) {
    const execPath = sidecarExecutablePath();
    sidecarProcess = spawn(execPath, ['--port', String(DEFAULT_PORT)], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } else {
    const root = repoRoot();
    sidecarProcess = spawn(
      'uv',
      [
        'run',
        '--project',
        path.join(root, 'python_api'),
        'python',
        path.join(root, 'python_api', 'velaria_service.py'),
        '--port',
        String(DEFAULT_PORT),
      ],
      {
        cwd: root,
        env: {
          ...process.env,
          PYTHONUNBUFFERED: '1',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    );
  }

  sidecarProcess.stdout.on('data', (chunk) => {
    process.stdout.write(`[velaria-service] ${chunk}`);
  });
  sidecarProcess.stderr.on('data', (chunk) => {
    process.stderr.write(`[velaria-service] ${chunk}`);
  });
  sidecarProcess.on('exit', (code, signal) => {
    console.log(`[velaria-service] exited code=${code} signal=${signal}`);
    sidecarProcess = null;
  });
  return sidecarProcess;
}

async function waitForServiceReady() {
  const deadline = Date.now() + 30000;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${serviceBaseUrl()}/health`);
      if (response.ok) {
        return await response.json();
      }
      lastError = new Error(`health status ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw lastError || new Error('velaria-service did not become ready');
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1360,
    height: 900,
    minWidth: 1100,
    minHeight: 760,
    backgroundColor: '#f3f0e8',
    webPreferences: {
      preload: path.join(__dirname, '..', 'preload', 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });
  mainWindow.loadFile(path.join(__dirname, '..', 'renderer', 'index.html'));
}

async function bootstrap() {
  startSidecar();
  await waitForServiceReady();
  createWindow();
}

app.whenReady().then(async () => {
  try {
    await bootstrap();
  } catch (error) {
    dialog.showErrorBox('Velaria bootstrap failed', String(error));
    app.quit();
  }
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('before-quit', () => {
  if (sidecarProcess && !sidecarProcess.killed) {
    sidecarProcess.kill('SIGTERM');
  }
});

ipcMain.handle('shell:pick-file', async () => {
  const result = await dialog.showOpenDialog({
    properties: ['openFile'],
    filters: [
      { name: 'Data files', extensions: ['csv', 'json', 'jsonl', 'xlsx', 'log', 'txt'] },
      { name: 'All files', extensions: ['*'] },
    ],
  });
  if (result.canceled || result.filePaths.length === 0) {
    return null;
  }
  return result.filePaths[0];
});

ipcMain.handle('shell:get-service-info', async () => {
  return {
    baseUrl: serviceBaseUrl(),
    packaged: app.isPackaged,
  };
});

ipcMain.handle('shell:export-file', async (_event, payload) => {
  const sourcePath = payload?.sourcePath;
  if (!sourcePath) {
    throw new Error('sourcePath is required');
  }
  const result = await dialog.showSaveDialog({
    defaultPath: payload?.suggestedName || path.basename(sourcePath),
  });
  if (result.canceled || !result.filePath) {
    return { cancelled: true };
  }
  await fs.copyFile(sourcePath, result.filePath);
  return {
    cancelled: false,
    destination: result.filePath,
  };
});

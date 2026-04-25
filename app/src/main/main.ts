import { app, BrowserWindow, dialog, ipcMain } from 'electron';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { spawn, type ChildProcessByStdio } from 'node:child_process';
import type { Readable } from 'node:stream';

const DEFAULT_PORT = Number.parseInt(process.env.VELARIA_SERVICE_PORT || '37491', 10);

let mainWindow: BrowserWindow | null = null;
type SidecarProcess = ChildProcessByStdio<null, Readable, Readable>;
type AppConfig = {
  bitableAppId?: string;
  bitableAppSecret?: string;
  agentRuntime?: string;
  agentAuthMode?: string;
  agentProvider?: string;
  agentApiKey?: string;
  agentBaseUrl?: string;
  agentModel?: string;
};

let sidecarProcess: SidecarProcess | null = null;
let embeddingPrepProcess: SidecarProcess | null = null;

function repoRoot() {
  return path.resolve(__dirname, '..', '..', '..', '..');
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

function jiebaDictDir() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'bin', 'velaria-service', '_internal', 'velaria', 'jieba_dict');
  }
  const devPath = path.join(repoRoot(), 'python', 'velaria', 'jieba_dict');
  return existsSync(devPath) ? devPath : '';
}

function configDir() {
  return path.join(os.homedir(), '.velaria');
}

function configPath() {
  return path.join(configDir(), 'config.json');
}

async function readAppConfig(): Promise<AppConfig> {
  try {
    const raw = await fs.readFile(configPath(), 'utf-8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return {};
    }
    return {
      bitableAppId:
        typeof parsed.bitableAppId === 'string' && parsed.bitableAppId.trim()
          ? parsed.bitableAppId.trim()
          : undefined,
      bitableAppSecret:
        typeof parsed.bitableAppSecret === 'string' && parsed.bitableAppSecret.trim()
          ? parsed.bitableAppSecret.trim()
          : undefined,
      agentRuntime:
        typeof parsed.agentRuntime === 'string' && parsed.agentRuntime.trim()
          ? parsed.agentRuntime.trim()
          : undefined,
      agentAuthMode:
        typeof parsed.agentAuthMode === 'string' && parsed.agentAuthMode.trim()
          ? parsed.agentAuthMode.trim()
          : undefined,
      agentProvider:
        typeof parsed.agentProvider === 'string' && parsed.agentProvider.trim()
          ? parsed.agentProvider.trim()
          : undefined,
      agentApiKey:
        typeof parsed.agentApiKey === 'string' && parsed.agentApiKey.trim()
          ? parsed.agentApiKey.trim()
          : undefined,
      agentBaseUrl:
        typeof parsed.agentBaseUrl === 'string' && parsed.agentBaseUrl.trim()
          ? parsed.agentBaseUrl.trim()
          : undefined,
      agentModel:
        typeof parsed.agentModel === 'string' && parsed.agentModel.trim()
          ? parsed.agentModel.trim()
          : undefined,
    };
  } catch {
    return {};
  }
}

async function writeAppConfig(payload: AppConfig): Promise<AppConfig> {
  let existing: Record<string, unknown> = {};
  try {
    const raw = await fs.readFile(configPath(), 'utf-8');
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      existing = parsed as Record<string, unknown>;
    }
  } catch {
    existing = {};
  }
  const sanitized: AppConfig = {
    bitableAppId:
      typeof payload.bitableAppId === 'string' && payload.bitableAppId.trim()
        ? payload.bitableAppId.trim()
        : undefined,
    bitableAppSecret:
      typeof payload.bitableAppSecret === 'string' && payload.bitableAppSecret.trim()
        ? payload.bitableAppSecret.trim()
        : undefined,
    agentRuntime:
      typeof payload.agentRuntime === 'string' && payload.agentRuntime.trim()
        ? payload.agentRuntime.trim()
        : undefined,
    agentAuthMode:
      typeof payload.agentAuthMode === 'string' && payload.agentAuthMode.trim()
        ? payload.agentAuthMode.trim()
        : undefined,
    agentProvider:
      typeof payload.agentProvider === 'string' && payload.agentProvider.trim()
        ? payload.agentProvider.trim()
        : undefined,
    agentApiKey:
      payload.agentAuthMode === 'api_key' && typeof payload.agentApiKey === 'string' && payload.agentApiKey.trim()
        ? payload.agentApiKey.trim()
        : undefined,
    agentBaseUrl:
      typeof payload.agentBaseUrl === 'string' && payload.agentBaseUrl.trim()
        ? payload.agentBaseUrl.trim()
        : undefined,
    agentModel:
      typeof payload.agentModel === 'string' && payload.agentModel.trim()
        ? payload.agentModel.trim()
        : undefined,
  };
  const next: Record<string, unknown> = { ...existing, ...sanitized };
  for (const [key, value] of Object.entries(next)) {
    if (value === undefined) {
      delete next[key];
    }
  }
  await fs.mkdir(configDir(), { recursive: true });
  await fs.writeFile(configPath(), `${JSON.stringify(next, null, 2)}\n`, 'utf-8');
  return readAppConfig();
}

function startSidecar() {
  if (sidecarProcess) {
    return sidecarProcess;
  }

  let processRef: SidecarProcess;
  if (app.isPackaged) {
    const execPath = sidecarExecutablePath();
    if (!execPath) {
      throw new Error('missing packaged sidecar path');
    }
    processRef = spawn(execPath, ['--port', String(DEFAULT_PORT)], {
      env: {
        ...process.env,
        ...(jiebaDictDir() ? { VELARIA_JIEBA_DICT_DIR: jiebaDictDir() } : {}),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } else {
    const root = repoRoot();
    processRef = spawn(
      'uv',
      [
        'run',
        '--project',
        path.join(root, 'python'),
        'python',
        '-m',
        'velaria_service',
        '--port',
        String(DEFAULT_PORT),
      ],
      {
        cwd: root,
        env: {
          ...process.env,
          PYTHONPATH: path.join(root, 'python'),
          PYTHONUNBUFFERED: '1',
          ...(jiebaDictDir() ? { VELARIA_JIEBA_DICT_DIR: jiebaDictDir() } : {}),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    );
  }

  processRef.stdout.on('data', (chunk) => {
    process.stdout.write(`[velaria-service] ${chunk}`);
  });
  processRef.stderr.on('data', (chunk) => {
    process.stderr.write(`[velaria-service] ${chunk}`);
  });
  processRef.on('exit', (code, signal) => {
    console.log(`[velaria-service] exited code=${code} signal=${signal}`);
    sidecarProcess = null;
  });
  sidecarProcess = processRef;
  return processRef;
}

function startBackgroundEmbeddingPrep() {
  if (app.isPackaged || embeddingPrepProcess) {
    return embeddingPrepProcess;
  }
  const root = repoRoot();
  const syncRef = spawn(
    'uv',
    ['sync', '--project', path.join(root, 'python'), '--extra', 'embedding'],
    {
      cwd: root,
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  );
  syncRef.stdout.on('data', (chunk) => {
    process.stdout.write(`[velaria-embedding] ${chunk}`);
  });
  syncRef.stderr.on('data', (chunk) => {
    process.stderr.write(`[velaria-embedding] ${chunk}`);
  });
  syncRef.on('exit', (code) => {
    if (code !== 0) {
      console.warn(`[velaria-embedding] sync exited code=${code}`);
      embeddingPrepProcess = null;
      return;
    }
    const warmupRef = spawn(
      'uv',
      [
        'run',
        '--project',
        path.join(root, 'python'),
        '--extra',
        'embedding',
        'python',
        '-c',
        [
          'from velaria import DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL, SentenceTransformerEmbeddingProvider;',
          'provider = SentenceTransformerEmbeddingProvider(model_name=DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL);',
          'provider.warmup(model=DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL)',
        ].join(' '),
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
    warmupRef.stdout.on('data', (chunk) => {
      process.stdout.write(`[velaria-embedding] ${chunk}`);
    });
    warmupRef.stderr.on('data', (chunk) => {
      process.stderr.write(`[velaria-embedding] ${chunk}`);
    });
    warmupRef.on('exit', (warmupCode) => {
      if (warmupCode !== 0) {
        console.warn(`[velaria-embedding] warmup exited code=${warmupCode}`);
      }
      embeddingPrepProcess = null;
    });
    embeddingPrepProcess = warmupRef;
  });
  embeddingPrepProcess = syncRef;
  return embeddingPrepProcess;
}

async function waitForServiceReady() {
  const deadline = Date.now() + 30_000;
  let lastError: Error | null = null;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${serviceBaseUrl()}/health`);
      if (response.ok) {
        return await response.json();
      }
      lastError = new Error(`health status ${response.status}`);
    } catch (error) {
      lastError = error as Error;
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
  mainWindow.loadFile(path.join(__dirname, '..', '..', 'renderer', 'index.html'));
  if (!app.isPackaged) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }
}

async function bootstrap() {
  startSidecar();
  createWindow();
  void waitForServiceReady().catch((error) => {
    console.error(`[velaria-service] startup wait failed: ${String(error)}`);
  });
  void startBackgroundEmbeddingPrep();
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
  app.quit();
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

ipcMain.handle('shell:get-service-info', async () => ({
  baseUrl: serviceBaseUrl(),
  packaged: app.isPackaged,
}));

ipcMain.handle('shell:get-config', async () => readAppConfig());

ipcMain.handle('shell:save-config', async (_event, payload: AppConfig) => writeAppConfig(payload));

ipcMain.handle('shell:export-file', async (_event, payload: { sourcePath?: string; suggestedName?: string }) => {
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

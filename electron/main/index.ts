import { app, BrowserWindow, ipcMain, nativeTheme, safeStorage, net, dialog } from 'electron'
import { join } from 'path'
import { randomBytes } from 'crypto'
import { appendFileSync, mkdirSync, rmSync, writeFileSync, existsSync } from 'fs'
import { ensureDb, dbGetAdminSettings, dbSaveAdminSettings, dbIngestLifecycleMarkers, dbGetProducts, dbGetSyncLog, dbClearLogs, dbLogFinish, dbLogStart, dbUpsertProducts, dbDeleteProductsMissingForStore, dbCountProducts, dbReplaceProductPlacementsForStore, dbGetGridColumns, dbSaveGridColumns, dbRecordApiRawResponse, dbGetAppSetting, dbSetAppSetting } from './storage/db'
import { deleteSecrets, hasSecrets, loadSecrets, saveSecrets, updateStoreName } from './storage/secrets'
import { ozonGetStoreName, ozonPlacementZoneInfo, ozonProductInfoList, ozonProductList, ozonTestAuth, ozonWarehouseList, setOzonApiCaptureHook } from './ozon'
import { type SalesPeriod } from './sales-sync'
import { getDefaultRollingSalesPeriod, getLocalDatasetRows, hasExactLocalSalesSnapshot, ingestOzonFboPushPayload, logFboShipmentTrace, refreshCoreLocalDatasetSnapshots, refreshSalesRawSnapshotFromApi } from './local-datasets'
import { getLifecycleMarkerRootDir, getPersistentRootDir, readPersistentStorageBootstrapState } from './storage/paths'
import { startLocalHttpServer, type LocalHttpServerHandle } from './local-http-server'
let mainWindow: BrowserWindow | null = null
let localHttpServer: LocalHttpServerHandle | null = null
let startupShowTimer: NodeJS.Timeout | null = null
let backgroundSyncTimer: NodeJS.Timeout | null = null
let isQuitting = false
let syncProductsInFlight: Promise<any> | null = null
let salesRefreshInFlight: Promise<{ rowsCount: number }> | null = null
let salesRefreshInFlightScopeKey = ''
const salesSnapshotWarmupInFlight = new Map<string, Promise<void>>()
let latestSalesWarmupScopeKey = ''
let latestRequestedSalesPeriod: SalesPeriod | null = null
const LOCAL_SERVER_PORT_KEY = 'local_server.port'
const LOCAL_SERVER_TOKEN_KEY = 'local_server.token'
const LOCAL_SERVER_WEBHOOK_TOKEN_KEY = 'local_server.webhook_token'
const LOCAL_SERVER_WEBHOOK_DIAG_KEY = 'local_server.webhook_diag'
const DEFAULT_LOCAL_SERVER_PORT = 45711
const INSTALLER_CLOSE_REQUEST_FLAG = '--installer-close-request'
const BOOTSTRAP_SKIP_INITIAL_SYNC_KEY = 'bootstrap.skip_initial_sync'
const SALES_PREFERRED_PERIOD_KEY = 'sales.preferred_period'
const hasInstallerCloseRequestFlag = process.argv.includes(INSTALLER_CLOSE_REQUEST_FLAG)
let installerShutdownInFlight: Promise<void> | null = null
let gracefulShutdownInFlight: Promise<void> | null = null
let startupDbWasMissing = false

type BootstrapStageKey = 'prepare' | 'products' | 'placements' | 'sales' | 'finalize'
type BootstrapProgressTimelineEntry = {
  key: BootstrapStageKey
  label: string
  status: 'pending' | 'active' | 'done' | 'error'
  startedAt: string | null
  finishedAt: string | null
  detail: string | null
}
type BootstrapProgressState = {
  active: boolean
  startedAt: string | null
  updatedAt: string | null
  finishedAt: string | null
  stageKey: BootstrapStageKey | null
  stageLabel: string
  stageMessage: string
  percent: number
  completedStages: number
  totalStages: number
  currentLoaded: number
  currentTotal: number | null
  currentUnitLabel: string
  etaSeconds: number | null
  error: string | null
  timeline: BootstrapProgressTimelineEntry[]
}

const BOOTSTRAP_STAGE_ORDER: Array<{ key: BootstrapStageKey; label: string }> = [
  { key: 'prepare', label: 'Подготовка' },
  { key: 'products', label: 'Товары' },
  { key: 'placements', label: 'Остатки и размещения' },
  { key: 'sales', label: 'Продажи' },
  { key: 'finalize', label: 'Финализация' },
]

function createEmptyBootstrapProgress(): BootstrapProgressState {
  return {
    active: false,
    startedAt: null,
    updatedAt: null,
    finishedAt: null,
    stageKey: null,
    stageLabel: '',
    stageMessage: '',
    percent: 0,
    completedStages: 0,
    totalStages: BOOTSTRAP_STAGE_ORDER.length,
    currentLoaded: 0,
    currentTotal: null,
    currentUnitLabel: 'этапов',
    etaSeconds: null,
    error: null,
    timeline: BOOTSTRAP_STAGE_ORDER.map((stage) => ({
      key: stage.key,
      label: stage.label,
      status: 'pending',
      startedAt: null,
      finishedAt: null,
      detail: null,
    })),
  }
}

let bootstrapProgressState: BootstrapProgressState = createEmptyBootstrapProgress()

function getBootstrapStageIndex(stageKey: BootstrapStageKey | null | undefined): number {
  if (!stageKey) return -1
  return BOOTSTRAP_STAGE_ORDER.findIndex((stage) => stage.key === stageKey)
}

function recalcBootstrapProgress() {
  const totalStages = BOOTSTRAP_STAGE_ORDER.length
  const completedStages = bootstrapProgressState.timeline.filter((step) => step.status === 'done').length
  const activeIndex = getBootstrapStageIndex(bootstrapProgressState.stageKey)
  let fraction = 0
  if (bootstrapProgressState.active && activeIndex >= 0) {
    const total = bootstrapProgressState.currentTotal
    if (typeof total === 'number' && Number.isFinite(total) && total > 0) {
      fraction = Math.max(0, Math.min(1, bootstrapProgressState.currentLoaded / total))
    } else if (bootstrapProgressState.currentLoaded > 0) {
      fraction = 0.5
    }
  }

  let percent = Math.round(((completedStages + fraction) / totalStages) * 100)
  if (bootstrapProgressState.finishedAt && !bootstrapProgressState.error) percent = 100
  if (bootstrapProgressState.error) percent = Math.max(1, percent)

  const activeEntry = bootstrapProgressState.stageKey
    ? bootstrapProgressState.timeline.find((step) => step.key === bootstrapProgressState.stageKey) ?? null
    : null
  const stageStartedMs = activeEntry?.startedAt ? Date.parse(activeEntry.startedAt) : NaN
  const updatedMs = bootstrapProgressState.updatedAt ? Date.parse(bootstrapProgressState.updatedAt) : NaN
  const total = bootstrapProgressState.currentTotal
  let etaSeconds: number | null = null
  if (
    bootstrapProgressState.active
    && Number.isFinite(stageStartedMs)
    && Number.isFinite(updatedMs)
    && typeof total === 'number'
    && Number.isFinite(total)
    && total > 0
    && bootstrapProgressState.currentLoaded > 0
    && bootstrapProgressState.currentLoaded < total
  ) {
    const elapsedMs = Math.max(1, updatedMs - stageStartedMs)
    etaSeconds = Math.max(0, Math.round((elapsedMs * (total - bootstrapProgressState.currentLoaded)) / bootstrapProgressState.currentLoaded / 1000))
  }

  bootstrapProgressState = {
    ...bootstrapProgressState,
    percent,
    completedStages,
    totalStages,
    etaSeconds,
  }
}

function cloneBootstrapProgress() {
  return {
    ok: true,
    ...bootstrapProgressState,
    timeline: bootstrapProgressState.timeline.map((step) => ({ ...step })),
  }
}

function startBootstrapProgress() {
  const startedAt = new Date().toISOString()
  bootstrapProgressState = createEmptyBootstrapProgress()
  bootstrapProgressState.active = true
  bootstrapProgressState.startedAt = startedAt
  bootstrapProgressState.updatedAt = startedAt
  recalcBootstrapProgress()
}

function setBootstrapStageActive(stageKey: BootstrapStageKey, stageMessage: string, opts?: { loaded?: number; total?: number | null; unitLabel?: string; detail?: string | null }) {
  const now = new Date().toISOString()
  const stage = BOOTSTRAP_STAGE_ORDER.find((item) => item.key === stageKey)
  if (!stage) return
  bootstrapProgressState.active = true
  bootstrapProgressState.updatedAt = now
  bootstrapProgressState.finishedAt = null
  bootstrapProgressState.error = null
  bootstrapProgressState.stageKey = stage.key
  bootstrapProgressState.stageLabel = stage.label
  bootstrapProgressState.stageMessage = stageMessage
  bootstrapProgressState.currentLoaded = Math.max(0, Math.trunc(opts?.loaded ?? 0))
  bootstrapProgressState.currentTotal = typeof opts?.total === 'number' && Number.isFinite(opts.total) && opts.total >= 0 ? Math.trunc(opts.total) : null
  bootstrapProgressState.currentUnitLabel = String(opts?.unitLabel ?? 'этапов')
  bootstrapProgressState.timeline = bootstrapProgressState.timeline.map((step) => {
    if (step.key !== stage.key) return step
    return {
      ...step,
      status: 'active',
      startedAt: step.startedAt ?? now,
      finishedAt: null,
      detail: opts?.detail ?? step.detail ?? null,
    }
  })
  recalcBootstrapProgress()
}

function setBootstrapStageProgress(stageKey: BootstrapStageKey, stageMessage: string, opts?: { loaded?: number; total?: number | null; unitLabel?: string; detail?: string | null }) {
  if (bootstrapProgressState.stageKey !== stageKey) {
    setBootstrapStageActive(stageKey, stageMessage, opts)
    return
  }
  const now = new Date().toISOString()
  bootstrapProgressState.updatedAt = now
  bootstrapProgressState.stageMessage = stageMessage
  bootstrapProgressState.currentLoaded = Math.max(0, Math.trunc(opts?.loaded ?? bootstrapProgressState.currentLoaded))
  bootstrapProgressState.currentTotal = typeof opts?.total === 'number' && Number.isFinite(opts.total) && opts.total >= 0 ? Math.trunc(opts.total) : bootstrapProgressState.currentTotal
  bootstrapProgressState.currentUnitLabel = String(opts?.unitLabel ?? bootstrapProgressState.currentUnitLabel ?? 'этапов')
  bootstrapProgressState.timeline = bootstrapProgressState.timeline.map((step) => step.key === stageKey ? { ...step, detail: opts?.detail ?? step.detail ?? null } : step)
  recalcBootstrapProgress()
}

function finishBootstrapStage(stageKey: BootstrapStageKey, detail?: string | null) {
  const now = new Date().toISOString()
  const stage = BOOTSTRAP_STAGE_ORDER.find((item) => item.key === stageKey)
  if (!stage) return
  bootstrapProgressState.updatedAt = now
  bootstrapProgressState.timeline = bootstrapProgressState.timeline.map((step) => {
    if (step.key !== stage.key) return step
    return {
      ...step,
      status: 'done',
      startedAt: step.startedAt ?? now,
      finishedAt: now,
      detail: detail ?? step.detail ?? null,
    }
  })
  bootstrapProgressState.stageKey = stage.key
  bootstrapProgressState.stageLabel = stage.label
  bootstrapProgressState.stageMessage = detail ?? `${stage.label} завершены`
  bootstrapProgressState.currentLoaded = 0
  bootstrapProgressState.currentTotal = null
  bootstrapProgressState.currentUnitLabel = 'этапов'
  recalcBootstrapProgress()
}

function failBootstrapProgress(errorMessage: string) {
  const now = new Date().toISOString()
  bootstrapProgressState.active = false
  bootstrapProgressState.updatedAt = now
  bootstrapProgressState.finishedAt = now
  bootstrapProgressState.error = errorMessage
  if (bootstrapProgressState.stageKey) {
    bootstrapProgressState.timeline = bootstrapProgressState.timeline.map((step) => {
      if (step.key !== bootstrapProgressState.stageKey) return step
      return {
        ...step,
        status: 'error',
        startedAt: step.startedAt ?? now,
        finishedAt: now,
        detail: errorMessage,
      }
    })
  }
  recalcBootstrapProgress()
}

function completeBootstrapProgress(detail?: string | null) {
  const now = new Date().toISOString()
  bootstrapProgressState.active = false
  bootstrapProgressState.updatedAt = now
  bootstrapProgressState.finishedAt = now
  bootstrapProgressState.error = null
  bootstrapProgressState.stageMessage = detail ?? 'Онлайн-данные готовы'
  bootstrapProgressState.currentLoaded = BOOTSTRAP_STAGE_ORDER.length
  bootstrapProgressState.currentTotal = BOOTSTRAP_STAGE_ORDER.length
  bootstrapProgressState.currentUnitLabel = 'этапов'
  recalcBootstrapProgress()
}

const singleInstanceLock = app.requestSingleInstanceLock()
if (!singleInstanceLock) {
app.quit()
}

function delay(ms: number) {
return new Promise((resolve) => setTimeout(resolve, ms))
}

function buildUniqueDesktopFilePath(fileName: string): string {
  const desktopDir = app.getPath('desktop')
  const dotIndex = fileName.lastIndexOf('.')
  const baseName = dotIndex > 0 ? fileName.slice(0, dotIndex) : fileName
  const extension = dotIndex > 0 ? fileName.slice(dotIndex) : ''

  let attempt = 0
  let candidate = join(desktopDir, fileName)
  while (existsSync(candidate)) {
    attempt += 1
    candidate = join(desktopDir, `${baseName} (${attempt})${extension}`)
  }
  return candidate
}

function getOrCreateLocalServerRuntimeConfig() {
  const storedPort = Number(dbGetAppSetting(LOCAL_SERVER_PORT_KEY) ?? DEFAULT_LOCAL_SERVER_PORT)
  const port = Number.isFinite(storedPort) && storedPort > 0 ? Math.trunc(storedPort) : DEFAULT_LOCAL_SERVER_PORT

  let token = String(dbGetAppSetting(LOCAL_SERVER_TOKEN_KEY) ?? '').trim()
  if (!token) {
    token = randomBytes(24).toString('base64url')
    dbSetAppSetting(LOCAL_SERVER_TOKEN_KEY, token)
  }

  let webhookToken = String(dbGetAppSetting(LOCAL_SERVER_WEBHOOK_TOKEN_KEY) ?? '').trim()
  if (!webhookToken) {
    webhookToken = randomBytes(24).toString('base64url')
    dbSetAppSetting(LOCAL_SERVER_WEBHOOK_TOKEN_KEY, webhookToken)
  }

  if (String(dbGetAppSetting(LOCAL_SERVER_PORT_KEY) ?? '').trim() !== String(port)) {
    dbSetAppSetting(LOCAL_SERVER_PORT_KEY, String(port))
  }

  return { port, token, webhookToken }
}

function getAppBootstrapState() {
  const storage = readPersistentStorageBootstrapState()
  const dbExists = false
  let secretsReady = false
  let productsCount = 0

  try {
    secretsReady = hasSecrets()
  } catch {
    secretsReady = false
  }

  try {
    productsCount = dbCountProducts()
  } catch {
    productsCount = 0
  }

  const skipInitialSync = String(dbGetAppSetting(BOOTSTRAP_SKIP_INITIAL_SYNC_KEY) ?? '').trim() === '1'

  return {
    ok: true,
    storageMode: 'session',
    storageRoot: storage.root,
    dbPath: null,
    secretsPath: storage.secretsPath,
    isFirstRun: productsCount === 0,
    dbExists,
    hasSecrets: secretsReady,
    productsCount,
    requiresInitialSync: productsCount === 0,
    skipInitialSync,
  }
}

type LocalServerWebhookDiag = {
  serverStartedAt?: string
  lastProbeAt?: string
  lastPushHitAt?: string
  lastPushAcceptedAt?: string
  lastPushAcceptedEvents?: number
  lastRemoteAddress?: string
  lastPathname?: string
  lastProbePathname?: string
}

function readLocalServerWebhookDiag(): LocalServerWebhookDiag {
  try {
    const raw = String(dbGetAppSetting(LOCAL_SERVER_WEBHOOK_DIAG_KEY) ?? '').trim()
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function mergeLocalServerWebhookDiag(patch: Partial<LocalServerWebhookDiag>) {
  const next = {
    ...readLocalServerWebhookDiag(),
    ...patch,
  }
  dbSetAppSetting(LOCAL_SERVER_WEBHOOK_DIAG_KEY, JSON.stringify(next))
  return next
}

function getInstallerMarkerDir() {
try {
const dir = getLifecycleMarkerRootDir()
mkdirSync(dir, { recursive: true })
return dir
} catch {
const dir = join(app.getPath('appData'), 'Ozonator')
mkdirSync(dir, { recursive: true })
return dir
}
}

function getInstallerReadyMarkerPath() {
return join(getInstallerMarkerDir(), 'installer-ready.marker')
}

function clearInstallerReadyMarker() {
try { rmSync(getInstallerReadyMarkerPath(), { force: true }) } catch {}
}

function writeInstallerReadyMarker() {
try { writeFileSync(getInstallerReadyMarkerPath(), '1', 'utf8') } catch {}
}

async function flushRendererDraftsForInstallerExit() {
if (!mainWindow || mainWindow.isDestroyed()) return
try {
await mainWindow.webContents.executeJavaScript("try { window.dispatchEvent(new Event('ozon:prepare-install-exit')) } catch {}", true)
} catch {}
}

async function requestInstallerShutdown(reason: string) {
if (installerShutdownInFlight) return await installerShutdownInFlight
const job = (async () => {
startupLog('installer-shutdown.request', { reason, pid: process.pid })
isQuitting = true
if (backgroundSyncTimer) {
clearInterval(backgroundSyncTimer)
backgroundSyncTimer = null
}
try {
await flushRendererDraftsForInstallerExit()
await delay(250)
} catch {}
try {
if (mainWindow && !mainWindow.isDestroyed()) {
try { mainWindow.hide() } catch {}
try { mainWindow.close() } catch {}
}
} catch {}
await delay(150)
writeInstallerReadyMarker()
await delay(150)
try { app.quit() } catch {}
setTimeout(() => {
try { app.exit(0) } catch {}
}, 1800)
})()
installerShutdownInFlight = job
try {
await job
} finally {
if (installerShutdownInFlight === job) installerShutdownInFlight = null
}
}

async function requestGracefulShutdown(reason: string) {
if (gracefulShutdownInFlight) return await gracefulShutdownInFlight
const job = (async () => {
startupLog('graceful-shutdown.request', { reason, pid: process.pid })
isQuitting = true
if (backgroundSyncTimer) {
clearInterval(backgroundSyncTimer)
backgroundSyncTimer = null
}
try {
await flushRendererDraftsForInstallerExit()
await delay(250)
} catch {}
try {
if (localHttpServer) {
  const srv = localHttpServer
  localHttpServer = null
  await srv.close().catch(() => {})
}
} catch {}
try {
if (mainWindow && !mainWindow.isDestroyed()) {
try { mainWindow.close() } catch {}
}
} catch {}
await delay(150)
try { app.quit() } catch {}
setTimeout(() => {
try { app.exit(0) } catch {}
}, 1800)
})()
gracefulShutdownInFlight = job
try {
await job
} finally {
if (gracefulShutdownInFlight === job) gracefulShutdownInFlight = null
}
}

function startupLog(...args: any[]) {
try {
const dir = app?.isReady?.() ? getPersistentRootDir() : app.getPath('temp')
mkdirSync(dir, { recursive: true })
const line = `[${new Date().toISOString()}] ` + args.map((a) => {
try { return typeof a === 'string' ? a : JSON.stringify(a) } catch { return String(a) }
}).join(' ') + '\n'
appendFileSync(join(dir, 'ozonator-startup.log'), line, 'utf8')
} catch {}
try { console.log('[startup]', ...args) } catch {}
}
function safeShowMainWindow(reason: string) {
try {
if (!mainWindow || mainWindow.isDestroyed()) return
startupLog('safeShowMainWindow', { reason, visible: mainWindow.isVisible() })
if (!mainWindow.isVisible()) {
try { mainWindow.show() } catch {}
}
try { mainWindow.focus() } catch {}
try { mainWindow.maximize() } catch {}
} catch (e: any) {
startupLog('safeShowMainWindow.error', e?.message ?? String(e))
}
}
app.on('second-instance', (_event, commandLine) => {
startupLog('app.second-instance', { commandLine })
if (Array.isArray(commandLine) && commandLine.some((arg) => String(arg).trim() === INSTALLER_CLOSE_REQUEST_FLAG)) {
void requestInstallerShutdown('installer-second-instance')
return
}
safeShowMainWindow('second-instance')
})

function chunk<T>(arr: T[], size: number): T[][] {
const out: T[][] = []
for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
return out
}
function createWindow() {
startupLog('createWindow.begin', { packaged: app.isPackaged, appPath: app.getAppPath(), __dirname })
mainWindow = new BrowserWindow({
width: 1200,
height: 760,
minWidth: 980,
minHeight: 620,
title: 'Ozonator',
show: false,
backgroundColor: '#F2F2F7',
autoHideMenuBar: true,
titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'hidden',
titleBarOverlay: { color: '#F2F2F7', symbolColor: '#1d1d1f', height: 40 },
webPreferences: {
preload: join(__dirname, '../preload/index.js'),
contextIsolation: true,
nodeIntegration: false,
backgroundThrottling: false,
},
})
if (startupShowTimer) {
clearTimeout(startupShowTimer)
startupShowTimer = null
}
startupShowTimer = setTimeout(() => safeShowMainWindow('show-timeout-fallback'), 2500)
mainWindow.once('ready-to-show', () => {
startupLog('event.ready-to-show')
safeShowMainWindow('ready-to-show')
})
mainWindow.webContents.on('did-finish-load', () => {
startupLog('event.did-finish-load', { url: mainWindow?.webContents?.getURL?.() })
safeShowMainWindow('did-finish-load')
})
mainWindow.webContents.on('did-fail-load', (_e, code, desc, url, isMainFrame) => {
startupLog('event.did-fail-load', { code, desc, url, isMainFrame })
try {
if (isMainFrame && mainWindow && !mainWindow.isDestroyed()) {
        const html = `<!doctype html><html><body style="font-family:Segoe UI,sans-serif;padding:16px">
          <h3>Ozonator не смог загрузить интерфейс</h3>
          <div>Причина: ${String(desc || 'did-fail-load')} (code ${String(code)})</div>
          <div style="margin-top:8px;color:#555">Подробности в файле ozonator-startup.log в папке данных приложения.</div>
        </body></html>`
mainWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html)).catch(() => {})
}
} catch {}
safeShowMainWindow('did-fail-load')
})
mainWindow.webContents.on('render-process-gone', (_e, details) => {
startupLog('event.render-process-gone', details)
safeShowMainWindow('render-process-gone')
})
mainWindow.on('unresponsive', () => {
startupLog('event.window-unresponsive')
})
mainWindow.on('close', (event) => {
if (isQuitting) return
event.preventDefault()
startupLog('event.window-close-quit')
void requestGracefulShutdown('window-close')
})
mainWindow.on('closed', () => {
startupLog('event.window-closed')
if (startupShowTimer) {
clearTimeout(startupShowTimer)
startupShowTimer = null
}
mainWindow = null
})
const devUrl = process.env.ELECTRON_RENDERER_URL || process.env.VITE_DEV_SERVER_URL || (!app.isPackaged ? 'http://localhost:5173/' : null)
startupLog('renderer.target', { devUrl, packaged: app.isPackaged })
if (devUrl) {
mainWindow.loadURL(devUrl).catch((e) => startupLog('loadURL.error', e?.message ?? String(e)))
try { mainWindow.webContents.openDevTools({ mode: 'detach' }) } catch {}
} else {
const rendererFile = join(app.getAppPath(), 'out/renderer/index.html')
startupLog('renderer.file', rendererFile)
mainWindow.loadFile(rendererFile).catch((e) => startupLog('loadFile.error', e?.message ?? String(e)))
}
nativeTheme.themeSource = 'light'
}
if (hasInstallerCloseRequestFlag && singleInstanceLock) {
app.whenReady().then(() => {
startupLog('installer-close-helper.no-running-instance', { pid: process.pid })
clearInstallerReadyMarker()
writeInstallerReadyMarker()
setTimeout(() => {
try { app.exit(0) } catch {}
}, 150)
})
} else {
app.whenReady().then(async () => {
try {
startupLog('app.whenReady')
startupDbWasMissing = true
if (!safeStorage.isEncryptionAvailable()) {
console.warn('safeStorage encryption is not available on this machine.')
startupLog('safeStorage.unavailable')
}
ensureDb()
startupLog('ensureDb.ok', getAppBootstrapState())
startupLog('sales.preferred_period.loaded', loadPersistedRequestedSalesPeriod())
try {
  const localServerRuntime = getOrCreateLocalServerRuntimeConfig()
  localHttpServer = await startLocalHttpServer({
    host: '127.0.0.1',
    port: localServerRuntime.port,
    token: localServerRuntime.token,
    webhookToken: localServerRuntime.webhookToken,
    handlers: {
      syncProducts: async (payload: { salesPeriod?: SalesPeriod | null } | null | undefined) => await performProductsSync({ salesPeriod: payload?.salesPeriod ?? null }),
      refreshSales: async (payload: { period?: SalesPeriod | null } | null | undefined) => await handleRefreshSales(payload?.period ?? null),
      getDatasetRows: async (payload: { dataset?: string; period?: SalesPeriod | null } | null | undefined) => {
        try {
          return await handleGetDatasetRows(payload?.dataset, payload?.period ?? null)
        } catch (e: any) {
          const dataset = String(payload?.dataset ?? 'products').trim() || 'products'
          return { ok: false, error: e?.message ?? String(e), dataset, rows: [] }
        }
      },
      getProducts: async () => await handleGetProducts(),
      getSales: async (payload: { period?: SalesPeriod | null } | null | undefined) => {
        try {
          return await handleGetSales(payload?.period ?? null)
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e), rows: [] }
        }
      },
      getReturns: async () => {
        try {
          return await handleGetReturns()
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e), rows: [] }
        }
      },
      getStocks: async () => {
        try {
          return await handleGetStocks()
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e), rows: [] }
        }
      },
      getGridColumns: async (payload: { dataset?: string } | null | undefined) => {
        try {
          return await handleGetGridColumns(payload?.dataset)
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e), dataset: String(payload?.dataset ?? 'products'), cols: null }
        }
      },
      saveGridColumns: async (payload: { dataset?: string; cols?: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }> } | null | undefined) => {
        try {
          const cols = payload?.cols
          if (!cols) {
            return { ok: false, error: 'cols missing', dataset: String(payload?.dataset ?? 'products'), savedCount: 0 }
          }
          return await handleSaveGridColumns(payload?.dataset, cols)
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e), dataset: String(payload?.dataset ?? 'products'), savedCount: 0 }
        }
      },
      getSyncLog: async () => {
        try {
          return await handleGetSyncLog()
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e), logs: [] }
        }
      },
      ingestOzonPush: async (payload, meta) => {
        try {
          mergeLocalServerWebhookDiag({
            lastPushHitAt: new Date().toISOString(),
            lastRemoteAddress: meta?.remoteAddress ?? undefined,
            lastPathname: meta?.pathname ?? undefined,
          })

          const secrets = loadSecrets()
          const resp = await ingestOzonFboPushPayload({
            storeClientId: secrets.clientId,
            payload,
            pathname: meta?.pathname ?? null,
            remoteAddress: meta?.remoteAddress ?? null,
          })

          mergeLocalServerWebhookDiag({
            lastPushAcceptedAt: new Date().toISOString(),
            lastPushAcceptedEvents: Number(resp?.acceptedEventsCount ?? 0),
          })

          if (mainWindow && !mainWindow.isDestroyed()) {
            void mainWindow.webContents.executeJavaScript("try { window.dispatchEvent(new Event('ozon:logs-updated')) } catch {}", true).catch(() => {})
          }
          return resp
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e) }
        }
      },
      probeOzonPush: async (meta) => {
        try {
          const now = new Date().toISOString()
          mergeLocalServerWebhookDiag({
            lastProbeAt: now,
            lastRemoteAddress: meta?.remoteAddress ?? undefined,
            lastProbePathname: meta?.pathname ?? undefined,
          })
          logFboShipmentTrace('webhook.probe.received', {
            storeClientId: getActiveStoreClientIdSafe(),
            itemsCount: 1,
            meta: {
              probeAt: now,
              pathname: meta?.pathname ?? null,
              remoteAddress: meta?.remoteAddress ?? null,
              source: 'local-webhook-ping',
            },
          })
          return { ok: true, status: 'probe_received', probeAt: now }
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e) }
        }
      },
      clearLogs: async () => {
        try {
          return await handleClearLogs()
        } catch (e: any) {
          return { ok: false, error: e?.message ?? String(e) }
        }
      },
    },
  })
  mergeLocalServerWebhookDiag({ serverStartedAt: localHttpServer.config.startedAt })
  logFboShipmentTrace('webhook.server.status', {
    storeClientId: getActiveStoreClientIdSafe(),
    itemsCount: 1,
    meta: {
      baseUrl: localHttpServer.config.baseUrl,
      healthUrlLocal: localHttpServer.config.healthUrlLocal,
      webhookUrlLocal: localHttpServer.config.webhookUrlLocal,
      webhookProbeUrlLocal: localHttpServer.config.webhookProbeUrlLocal,
      startedAt: localHttpServer.config.startedAt,
      source: 'server-start',
    },
  })
  startupLog('local-http-server.started', { baseUrl: localHttpServer.config.baseUrl, webhookUrlLocal: localHttpServer.config.webhookUrlLocal, webhookProbeUrlLocal: localHttpServer.config.webhookProbeUrlLocal })
} catch (e: any) {
  startupLog('local-http-server.failed', { error: e?.message ?? String(e) })
}
setOzonApiCaptureHook((evt) => {
dbRecordApiRawResponse({
storeClientId: evt.storeClientId,
method: evt.method,
endpoint: evt.endpoint,
requestBody: evt.requestBody,
responseBody: evt.responseBody,
httpStatus: evt.httpStatus,
isSuccess: evt.isSuccess,
errorMessage: evt.errorMessage ?? null,
fetchedAt: evt.fetchedAt,
})
})
dbIngestLifecycleMarkers({ appVersion: app.getVersion() })
startupLog('dbIngestLifecycleMarkers.ok', { version: app.getVersion() })
clearInstallerReadyMarker()
createWindow()
ensureBackgroundSyncLoop()
app.on('before-quit', () => {
isQuitting = true
if (backgroundSyncTimer) {
clearInterval(backgroundSyncTimer)
backgroundSyncTimer = null
}
if (localHttpServer) {
  const srv = localHttpServer
  localHttpServer = null
  void srv.close().catch(() => {})
}
})
app.on('activate', () => {
startupLog('app.activate', { windows: BrowserWindow.getAllWindows().length })
if (BrowserWindow.getAllWindows().length === 0) createWindow()
else safeShowMainWindow('app-activate')
})
} catch (e: any) {
startupLog('fatal.startup', e?.stack ?? e?.message ?? String(e))
try {
dialog.showErrorBox('Ozonator — ошибка запуска', String(e?.message ?? e))
} catch {}
try {
if (!mainWindow) {
mainWindow = new BrowserWindow({ width: 900, height: 640, show: true, autoHideMenuBar: true })
        const html = `<!doctype html><html><body style="font-family:Segoe UI,sans-serif;padding:16px">
          <h3>Ozonator не запустился</h3>
          <pre style="white-space:pre-wrap">${String(e?.stack ?? e?.message ?? e)}</pre>
          <div style="color:#555">Подробности: ozonator-startup.log в папке данных приложения.</div>
        </body></html>`
mainWindow.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html)).catch(() => {})
}
} catch {}
}
})
}
process.on('uncaughtException', (e: any) => {
startupLog('process.uncaughtException', e?.stack ?? e?.message ?? String(e))
})
process.on('unhandledRejection', (e: any) => {
startupLog('process.unhandledRejection', e as any)
})
process.on('SIGTERM', () => {
  void requestGracefulShutdown('SIGTERM')
})
process.on('SIGINT', () => {
  void requestGracefulShutdown('SIGINT')
})
app.on('window-all-closed', () => {
if (process.platform !== 'darwin') app.quit()
})
function checkInternet(timeoutMs = 2500): Promise<boolean> {
return new Promise((resolve) => {
const request = net.request({ method: 'GET', url: 'https://api-seller.ozon.ru' })
const timer = setTimeout(() => {
try { request.abort() } catch {}
resolve(false)
}, timeoutMs)
request.on('response', () => {
clearTimeout(timer)
resolve(true)
})
request.on('error', () => {
clearTimeout(timer)
resolve(false)
})
request.end()
})
}
function getActiveStoreClientIdSafe(): string | null {
try {
return loadSecrets().clientId
} catch {
return null
}
}

function readDatasetRowsSafe(datasetRaw: string, period?: SalesPeriod | null) {
const storeClientId = getActiveStoreClientIdSafe()
const dataset = String(datasetRaw ?? '').trim() || 'products'
const rows = getLocalDatasetRows(storeClientId, dataset, { period: period ?? null })
return { storeClientId, dataset, rows }
}



function emitRendererDataUpdatedEvents() {
if (!mainWindow || mainWindow.isDestroyed()) return
const script = `
try {
window.dispatchEvent(new Event('ozon:products-updated'))
window.dispatchEvent(new Event('ozon:logs-updated'))
window.dispatchEvent(new Event('ozon:store-updated'))
} catch {}
`
try {
void mainWindow.webContents.executeJavaScript(script, true)
} catch {}
}

function getSalesWarmupScopeKey(period?: SalesPeriod | null) {
const from = typeof period?.from === 'string' ? period.from.trim() : ''
const to = typeof period?.to === 'string' ? period.to.trim() : ''
return `${from}|${to}`
}

function normalizeSalesPeriodInput(period?: SalesPeriod | null): SalesPeriod | null {
const from = typeof period?.from === 'string' ? period.from.trim() : ''
const to = typeof period?.to === 'string' ? period.to.trim() : ''
return from && to ? { from, to } : null
}

function rememberRequestedSalesPeriod(period?: SalesPeriod | null) {
const normalized = normalizeSalesPeriodInput(period)
if (normalized) {
latestRequestedSalesPeriod = normalized
persistRequestedSalesPeriod(normalized)
}
return normalized
}

function getPreferredBackgroundSalesPeriod() {
return latestRequestedSalesPeriod ?? loadPersistedRequestedSalesPeriod() ?? getDefaultRollingSalesPeriod()
}

function loadPersistedRequestedSalesPeriod() {
try {
const raw = dbGetAppSetting(SALES_PREFERRED_PERIOD_KEY)
if (!raw) return null
const parsed = JSON.parse(raw) as SalesPeriod
const normalized = normalizeSalesPeriodInput(parsed)
if (normalized) latestRequestedSalesPeriod = normalized
return normalized
} catch {
return null
}
}

function persistRequestedSalesPeriod(period?: SalesPeriod | null) {
const normalized = normalizeSalesPeriodInput(period)
if (!normalized) return null
try {
dbSetAppSetting(SALES_PREFERRED_PERIOD_KEY, JSON.stringify(normalized))
} catch {}
return normalized
}

async function runSalesRefreshSerial(secrets: ReturnType<typeof loadSecrets>, period?: SalesPeriod | null, reason = 'sales-refresh') {
const requestedPeriod = normalizeSalesPeriodInput(period) ?? getPreferredBackgroundSalesPeriod()
const requestedScopeKey = getSalesWarmupScopeKey(requestedPeriod)
if (salesRefreshInFlight) {
try {
await salesRefreshInFlight
} catch {
// ignore and continue with current request
}
if (hasExactLocalSalesSnapshot(getActiveStoreClientIdSafe(), requestedPeriod ?? null)) {
const rows = getLocalDatasetRows(getActiveStoreClientIdSafe(), 'sales', { period: requestedPeriod ?? null })
startupLog('sales.refresh.serial.reused', { reason, scopeKey: requestedScopeKey, rowsCount: Array.isArray(rows) ? rows.length : 0 })
return { rowsCount: Array.isArray(rows) ? rows.length : 0 }
}
}
const job = (async () => await refreshSalesRawSnapshotFromApi(secrets, requestedPeriod ?? null))()
salesRefreshInFlight = job
salesRefreshInFlightScopeKey = requestedScopeKey
startupLog('sales.refresh.serial.start', { reason, scopeKey: requestedScopeKey, period: requestedPeriod })
try {
return await job
} finally {
if (salesRefreshInFlight === job) {
 salesRefreshInFlight = null
 salesRefreshInFlightScopeKey = ''
}
}
}

function warmupSalesSnapshotInBackground(period?: SalesPeriod | null, reason = 'sales-read') {
const scopeKey = getSalesWarmupScopeKey(period)
latestSalesWarmupScopeKey = scopeKey
if (salesSnapshotWarmupInFlight.has(scopeKey)) return

const job = (async () => {
if (isQuitting) return
if (!hasSecrets()) return

const online = await checkInternet()
if (!online) return

let secrets = null
try {
secrets = loadSecrets()
} catch {
secrets = null
}
if (!secrets) return

try {
const exactBefore = hasExactLocalSalesSnapshot(getActiveStoreClientIdSafe(), period ?? null)
const warmed = exactBefore
  ? { refreshed: false, rowsCount: getLocalDatasetRows(getActiveStoreClientIdSafe(), 'sales', { period: period ?? null }).length }
  : await runSalesRefreshSerial(secrets, period ?? null, `warmup:${reason}`)
if (Number(warmed?.rowsCount ?? 0) > 0) {
const isLatestScope = latestSalesWarmupScopeKey === scopeKey
startupLog('sales-snapshot-warmup.refreshed', {
reason,
scopeKey,
rowsCount: Number(warmed?.rowsCount ?? 0),
isLatestScope,
})
if (isLatestScope) emitRendererDataUpdatedEvents()
}
} catch (e: any) {
startupLog('sales-snapshot-warmup.error', { reason, scopeKey, error: e?.message ?? String(e) })
}
})()

salesSnapshotWarmupInFlight.set(scopeKey, job)
void job.finally(() => {
if ((salesSnapshotWarmupInFlight.get(scopeKey) ?? null) === job) {
salesSnapshotWarmupInFlight.delete(scopeKey)
}
})
}

async function performProductsSync(args?: { salesPeriod?: SalesPeriod | null }) {
if (syncProductsInFlight) return await syncProductsInFlight
const job = (async () => {
let storeClientId: string | null = null
startBootstrapProgress()
setBootstrapStageActive('prepare', 'Проверяем ключи и доступ к Ozon', { loaded: 0, total: 1, unitLabel: 'шагов' })
try { storeClientId = loadSecrets().clientId } catch {}
const logId = dbLogStart('sync_products', storeClientId)
try {
const secrets = loadSecrets()
const existingOfferIds = new Set(dbGetProducts(secrets.clientId).map((p: any) => p.offer_id))
const incomingOfferIds = new Set<string>()
let added = 0
let lastId = ''
const limit = 1000
let pages = 0
let total = 0
let estimatedProductPages: number | null = null
finishBootstrapStage('prepare', 'Ключи и доступ к Ozon готовы')

setBootstrapStageActive('products', 'Загружаем товары из Ozon', { loaded: 0, total: null, unitLabel: 'страниц' })
for (let guard = 0; guard < 200; guard++) {
const { items, lastId: next, total: totalMaybe } = await ozonProductList(secrets, { lastId, limit })
pages += 1
total += items.length
if (typeof totalMaybe === 'number' && Number.isFinite(totalMaybe) && totalMaybe > 0) {
  estimatedProductPages = Math.max(1, Math.ceil(totalMaybe / limit))
}
setBootstrapStageProgress('products', 'Получаем карточки товаров', {
  loaded: pages,
  total: estimatedProductPages,
  unitLabel: 'страниц',
  detail: estimatedProductPages != null
    ? `Загружено ${pages} из ${estimatedProductPages} страниц товаров`
    : `Загружено ${pages} страниц товаров`,
})
const ids = items.map(i => i.product_id).filter(Boolean) as number[]
const infoList = await ozonProductInfoList(secrets, ids)
const infoMap = new Map<number, typeof infoList[number]>()
for (const p of infoList) infoMap.set(p.product_id, p)
const enriched = items.map((it) => {
const info = it.product_id ? infoMap.get(it.product_id) : undefined
return {
offer_id: it.offer_id,
product_id: it.product_id,
sku: (info?.ozon_sku ?? info?.sku ?? it.sku ?? null),
ozon_sku: (info?.ozon_sku ?? info?.sku ?? it.sku ?? null),
seller_sku: (info?.seller_sku ?? it.offer_id ?? null),
fbo_sku: info?.fbo_sku ?? null,
fbs_sku: info?.fbs_sku ?? null,
barcode: info?.barcode ?? null,
brand: info?.brand ?? null,
category: info?.category ?? null,
type: info?.type ?? null,
name: info?.name ?? null,
photo_url: info?.photo_url ?? null,
is_visible: info?.is_visible ?? null,
hidden_reasons: info?.hidden_reasons ?? null,
created_at: info?.created_at ?? null,
archived: it.archived ?? false,
store_client_id: secrets.clientId,
}
})
for (const it of enriched) {
const offer = String((it as any).offer_id)
if (offer) incomingOfferIds.add(offer)
if (!existingOfferIds.has(offer)) {
existingOfferIds.add(offer)
added += 1
}
}
dbUpsertProducts(enriched)
      await delay(0)
if (!next) break
if (next === lastId) break
lastId = next
if (typeof totalMaybe === 'number' && total >= totalMaybe) break
}
dbDeleteProductsMissingForStore(secrets.clientId, Array.from(incomingOfferIds))
const syncedCount = dbCountProducts(secrets.clientId)
finishBootstrapStage('products', `Товары загружены: ${syncedCount}`)

let placementRowsCount = 0
let placementSyncError: string | null = null
let placementCacheKept = false
setBootstrapStageActive('placements', 'Загружаем остатки и размещения', { loaded: 0, total: null, unitLabel: 'запросов' })
try {
const productsForStore = dbGetProducts(secrets.clientId)
const ozonSkuList = Array.from(new Set(productsForStore.map((p) => String(p.sku ?? '').trim()).filter(Boolean)))
const sellerSkuList = Array.from(new Set(productsForStore.map((p) => String(p.offer_id ?? '').trim()).filter(Boolean)))
if (ozonSkuList.length > 0 || sellerSkuList.length > 0) {
const warehouses = await ozonWarehouseList(secrets)
if (!Array.isArray(warehouses) || warehouses.length === 0) {
placementSyncError = 'Ozon не вернул список складов; текущие данные по складам/зонам сохранены без перезаписи.'
placementCacheKept = true
} else {
const allPlacementRows: Array<{
warehouse_id: number
warehouse_name?: string | null
sku: string
ozon_sku?: string | null
seller_sku?: string | null
placement_zone?: string | null
}> = []
const placementRowKeys = new Set<string>()
let placementApiCallCount = 0
const totalPlacementCalls = warehouses.reduce((sum, wh) => {
  const wid = Number((wh as any).warehouse_id)
  if (!Number.isFinite(wid)) return sum
  return sum + Math.ceil(ozonSkuList.length / 500) + Math.ceil(sellerSkuList.length / 500)
}, 0)
const appendPlacementRows = (
warehouseId: number,
warehouseName: string | null,
zones: Array<{
sku: string
ozon_sku?: string | null
seller_sku?: string | null
placement_zone: string | null
}>
) => {
for (const z of zones) {
const rowKey = [
String(warehouseId),
String(z.ozon_sku ?? ''),
String(z.seller_sku ?? ''),
String(z.placement_zone ?? ''),
].join('::')
if (placementRowKeys.has(rowKey)) continue
placementRowKeys.add(rowKey)
allPlacementRows.push({
warehouse_id: warehouseId,
warehouse_name: warehouseName,
sku: z.sku,
ozon_sku: z.ozon_sku ?? null,
seller_sku: z.seller_sku ?? null,
placement_zone: z.placement_zone ?? null,
})
}
}
for (const wh of warehouses) {
const wid = Number(wh.warehouse_id)
if (!Number.isFinite(wid)) continue
for (const part of chunk(ozonSkuList, 500)) {
placementApiCallCount += 1
setBootstrapStageProgress('placements', 'Запрашиваем зоны размещения по складам', {
  loaded: placementApiCallCount - 1,
  total: totalPlacementCalls,
  unitLabel: 'запросов',
  detail: `Выполнено ${Math.max(0, placementApiCallCount - 1)} из ${totalPlacementCalls} запросов по складам`,
})
const zones = await ozonPlacementZoneInfo(secrets, { warehouseId: wid, skus: part })
appendPlacementRows(wid, wh.name ?? null, zones)
setBootstrapStageProgress('placements', 'Запрашиваем зоны размещения по складам', {
  loaded: placementApiCallCount,
  total: totalPlacementCalls,
  unitLabel: 'запросов',
  detail: `Выполнено ${placementApiCallCount} из ${totalPlacementCalls} запросов по складам`,
})
}
for (const part of chunk(sellerSkuList, 500)) {
placementApiCallCount += 1
setBootstrapStageProgress('placements', 'Запрашиваем зоны размещения по складам', {
  loaded: placementApiCallCount - 1,
  total: totalPlacementCalls,
  unitLabel: 'запросов',
  detail: `Выполнено ${Math.max(0, placementApiCallCount - 1)} из ${totalPlacementCalls} запросов по складам`,
})
const zones = await ozonPlacementZoneInfo(secrets, { warehouseId: wid, skus: part })
appendPlacementRows(wid, wh.name ?? null, zones)
setBootstrapStageProgress('placements', 'Запрашиваем зоны размещения по складам', {
  loaded: placementApiCallCount,
  total: totalPlacementCalls,
  unitLabel: 'запросов',
  detail: `Выполнено ${placementApiCallCount} из ${totalPlacementCalls} запросов по складам`,
})
}
}
if (allPlacementRows.length === 0 && placementApiCallCount > 0) {
placementSyncError = 'Ozon не вернул зоны размещения ни по одному SKU; текущие данные по складам/зонам сохранены.'
placementCacheKept = true
} else {
placementRowsCount = dbReplaceProductPlacementsForStore(secrets.clientId, allPlacementRows)
}
}
} else {
placementRowsCount = dbReplaceProductPlacementsForStore(secrets.clientId, [])
}
} catch (placementErr: any) {
placementSyncError = placementErr?.message ?? String(placementErr)
}
finishBootstrapStage('placements', placementSyncError ? `Остатки: ${placementSyncError}` : `Остатки и размещения обновлены: ${placementRowsCount}`)

setBootstrapStageActive('sales', 'Загружаем продажи', { loaded: 0, total: 1, unitLabel: 'этапов' })
const localSnapshots = refreshCoreLocalDatasetSnapshots(secrets.clientId)
let salesRowsCount = 0
let salesSyncError: string | null = null
try {
const requestedSalesPeriod = rememberRequestedSalesPeriod(args?.salesPeriod ?? null)
const salesRefresh = await runSalesRefreshSerial(secrets, requestedSalesPeriod ?? null, 'syncProducts')
salesRowsCount = Number(salesRefresh?.rowsCount ?? 0)
} catch (salesErr: any) {
salesSyncError = salesErr?.message ?? String(salesErr)
}
finishBootstrapStage('sales', salesSyncError ? `Продажи: ${salesSyncError}` : `Продажи загружены: ${salesRowsCount}`)

setBootstrapStageActive('finalize', 'Финализируем данные', { loaded: 0, total: 1, unitLabel: 'этапов' })
if (!secrets.storeName) {
try {
const name = await ozonGetStoreName(secrets)
if (name) updateStoreName(name)
} catch {
}
}
dbLogFinish(logId, {
status: 'success',
itemsCount: syncedCount,
storeClientId: secrets.clientId,
meta: {
added,
storeClientId: secrets.clientId,
storeName: loadSecrets().storeName ?? null,
placementRowsCount,
placementSyncError,
placementCacheKept,
localProductsRowsCount: localSnapshots.productsRowsCount,
localStocksRowsCount: localSnapshots.stocksRowsCount,
salesRowsCount,
salesSyncError,
},
})
finishBootstrapStage('finalize', 'Онлайн-данные готовы')
completeBootstrapProgress('Онлайн-данные загружены')
return { ok: true, itemsCount: syncedCount, pages, placementRowsCount, placementSyncError, salesRowsCount, salesSyncError }
} catch (e: any) {
failBootstrapProgress(e?.message ?? String(e))
dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
return { ok: false, error: e?.message ?? String(e) }
}
})()
syncProductsInFlight = job
try {
return await job
} finally {
if (syncProductsInFlight === job) syncProductsInFlight = null
}
}

async function runBackgroundSyncTick(reason: string) {
if (isQuitting) return
if (!mainWindow || mainWindow.isDestroyed()) return
if (!hasSecrets()) return
const online = await checkInternet()
if (!online) return
const resp = await performProductsSync({ salesPeriod: getPreferredBackgroundSalesPeriod() })
if (resp?.ok) {
startupLog('background-sync.ok', {
reason,
itemsCount: Number(resp?.itemsCount ?? 0),
windowVisible: mainWindow.isVisible(),
})
emitRendererDataUpdatedEvents()
} else if (resp?.error) {
startupLog('background-sync.error', { reason, error: String(resp.error) })
}
}

function ensureBackgroundSyncLoop() {
if (backgroundSyncTimer) {
clearInterval(backgroundSyncTimer)
backgroundSyncTimer = null
}
backgroundSyncTimer = setInterval(() => {
void runBackgroundSyncTick('interval')
}, 60 * 1000)
setTimeout(() => {
void runBackgroundSyncTick('startup-delay')
}, 15 * 1000)
}



function translateSalesRefreshError(messageRaw: unknown, rowsCount = 0): string {
  const message = String(messageRaw ?? '').trim()
  if (/HTTP\s*400/.test(message)) {
    return rowsCount > 0
      ? 'Ozon отклонил часть дополнительной догрузки продаж за выбранный период. Основные данные уже загружены, подробности сохранены в журнале синхронизации.'
      : 'Ozon не принял запрос на дополнительную догрузку продаж за выбранный период. Попробуй сократить период или повторить загрузку позже.'
  }
  if (/HTTP\s*429/.test(message)) {
    return rowsCount > 0
      ? 'Ozon временно ограничил частоту запросов. Основные данные уже загружены, подробности сохранены в журнале синхронизации.'
      : 'Ozon временно ограничил частоту запросов. Повтори загрузку позже.'
  }
  if (/timeout/i.test(message)) {
    return rowsCount > 0
      ? 'Ozon ответил не вовремя при дополнительной догрузке продаж. Основные данные уже загружены, подробности сохранены в журнале синхронизации.'
      : 'Ozon не успел ответить при загрузке продаж. Повтори попытку позже.'
  }
  return rowsCount > 0
    ? 'Во время дополнительной догрузки продаж возникла неполадка. Основные данные уже загружены, подробности сохранены в журнале синхронизации.'
    : 'Во время загрузки продаж возникла неполадка. Подробности сохранены в журнале синхронизации.'
}

async function handleRefreshSales(period: SalesPeriod | null | undefined) {
  const requestedPeriod = rememberRequestedSalesPeriod(period ?? null)
  try {
    const secrets = loadSecrets()
    const refreshed = await runSalesRefreshSerial(secrets, requestedPeriod ?? null, 'refreshSales')
    return { ok: true, rowsCount: Number(refreshed?.rowsCount ?? 0), rateLimited: false }
  } catch (e: any) {
    const technicalMessage = e?.message ?? String(e)
    const isRateLimited = /HTTP\s*429/.test(technicalMessage)
    try {
      const rows = getLocalDatasetRows(getActiveStoreClientIdSafe(), 'sales', { period: requestedPeriod ?? null })
      if (Array.isArray(rows) && rows.length > 0) {
        const friendly = translateSalesRefreshError(technicalMessage, rows.length)
        startupLog('sales.refresh.nonblocking_warning', {
          period: period ?? null,
          rowsCount: rows.length,
          message: friendly,
          technicalMessage,
        })
        return { ok: true, rowsCount: rows.length, rateLimited: isRateLimited, warning: friendly }
      }
    } catch {
      // ignore
    }
    const friendly = translateSalesRefreshError(technicalMessage, 0)
    return { ok: false, error: friendly, rowsCount: 0, rateLimited: isRateLimited }
  }
}

async function handleGetDatasetRows(datasetRaw: unknown, period: SalesPeriod | null | undefined) {
  const dataset = String(datasetRaw ?? 'products').trim() || 'products'
  if (dataset === 'sales') {
    const requestedPeriod = rememberRequestedSalesPeriod(period ?? null)
    const rowsAll = getLocalDatasetRows(getActiveStoreClientIdSafe(), 'sales', { period: requestedPeriod ?? null })
    const MAX_UI_ROWS = 8000
    const truncated = Array.isArray(rowsAll) && rowsAll.length > MAX_UI_ROWS
    const rows = truncated ? rowsAll.slice(0, MAX_UI_ROWS) : rowsAll
    if (truncated) startupLog('sales.ui.truncated', { total: rowsAll.length, sent: rows.length })
    const hasExactSnapshot = hasExactLocalSalesSnapshot(getActiveStoreClientIdSafe(), requestedPeriod ?? null)
    const shouldWarmup = !hasExactSnapshot
    if (shouldWarmup) {
      setTimeout(() => warmupSalesSnapshotInBackground(requestedPeriod ?? null, 'local-server:getDatasetRows'), 0)
    }
    return { ok: true, dataset, rows, truncated, totalRows: rowsAll.length, warmupScheduled: shouldWarmup, exactSnapshot: hasExactSnapshot }
  }
  const { rows } = readDatasetRowsSafe(dataset, period ?? null)
  return { ok: true, dataset, rows }
}

async function handleGetProducts() {
  const { rows } = readDatasetRowsSafe('products', null)
  return { ok: true, products: rows }
}

async function handleGetSales(period: SalesPeriod | null | undefined) {
  const requestedPeriod = rememberRequestedSalesPeriod(period ?? null)
  const rowsAll = getLocalDatasetRows(getActiveStoreClientIdSafe(), 'sales', { period: requestedPeriod ?? null })
  const MAX_UI_ROWS = 8000
  const truncated = Array.isArray(rowsAll) && rowsAll.length > MAX_UI_ROWS
  const rows = truncated ? rowsAll.slice(0, MAX_UI_ROWS) : rowsAll
  if (truncated) startupLog('sales.ui.truncated', { total: rowsAll.length, sent: rows.length, reason: 'local-server:getSales' })
  const hasExactSnapshot = hasExactLocalSalesSnapshot(getActiveStoreClientIdSafe(), requestedPeriod ?? null)
  const shouldWarmup = !hasExactSnapshot
  if (shouldWarmup) {
    setTimeout(() => warmupSalesSnapshotInBackground(requestedPeriod ?? null, 'local-server:getSales'), 0)
  }
  return { ok: true, rows, truncated, totalRows: rowsAll.length, warmupScheduled: shouldWarmup, exactSnapshot: hasExactSnapshot }
}

async function handleGetReturns() {
  const { rows } = readDatasetRowsSafe('returns', null)
  return { ok: true, rows }
}

async function handleGetStocks() {
  const { rows } = readDatasetRowsSafe('stocks', null)
  return { ok: true, rows }
}

async function handleGetGridColumns(datasetRaw: unknown) {
  return { ok: true, ...dbGetGridColumns(String(datasetRaw ?? 'products')) }
}

async function handleSaveGridColumns(
  datasetRaw: unknown,
  cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }>,
) {
  return { ok: true, ...dbSaveGridColumns(String(datasetRaw ?? 'products'), cols) }
}

async function handleGetSyncLog() {
  let storeClientId: string | null = null
  try {
    storeClientId = loadSecrets().clientId
  } catch {
    storeClientId = null
  }
  const logs = dbGetSyncLog(storeClientId)
  return { ok: true, logs }
}

async function handleClearLogs() {
  dbClearLogs()
  return { ok: true }
}

ipcMain.handle('local-server:getConfig', async () => {
  try {
    if (!localHttpServer) return { ok: false, error: 'local server not started' }
    return { ok: true, ...localHttpServer.config, ...readLocalServerWebhookDiag() }
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e) }
  }
})

ipcMain.handle('local-server:probe', async () => {
  try {
    if (!localHttpServer) return { ok: false, error: 'local server not started' }
    const resp = await fetch(localHttpServer.config.webhookProbeUrlLocal, { method: 'GET' })
    const text = await resp.text()
    let parsed: any = null
    try { parsed = JSON.parse(text) } catch { parsed = { ok: false, error: 'Invalid JSON from local webhook probe', __raw_text: text } }
    return {
      ok: Boolean(parsed?.ok),
      ...parsed,
      httpStatus: resp.status,
      webhookProbeUrlLocal: localHttpServer.config.webhookProbeUrlLocal,
    }
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e) }
  }
})

ipcMain.handle('app:getBootstrapState', async () => {
  try {
    return getAppBootstrapState()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e) }
  }
})

ipcMain.handle('app:getBootstrapProgress', async () => {
  try {
    return cloneBootstrapProgress()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e) }
  }
})

ipcMain.handle('app:setBootstrapSkipInitialSync', async (_e, payload: { skipInitialSync?: boolean } | undefined) => {
  try {
    const skipInitialSync = !!payload?.skipInitialSync
    dbSetAppSetting(BOOTSTRAP_SKIP_INITIAL_SYNC_KEY, skipInitialSync ? '1' : '0')
    return { ok: true, skipInitialSync }
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e) }
  }
})

ipcMain.handle('secrets:status', async () => {
return {
hasSecrets: hasSecrets(),
encryptionAvailable: safeStorage.isEncryptionAvailable(),
}
})
ipcMain.handle('secrets:save', async (_e, secrets: { clientId: string; apiKey: string }) => {
saveSecrets({ clientId: String(secrets.clientId).trim(), apiKey: String(secrets.apiKey).trim() })
return { ok: true }
})
ipcMain.handle('secrets:load', async () => {
const s = loadSecrets()
return { ok: true, secrets: { clientId: s.clientId, apiKey: s.apiKey, storeName: s.storeName ?? null } }
})
ipcMain.handle('secrets:delete', async () => {
deleteSecrets()
return { ok: true }
})
ipcMain.handle('net:check', async () => {
return { online: await checkInternet() }
})
ipcMain.handle('admin:getSettings', async () => {
try {
return { ok: true, ...dbGetAdminSettings() }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e), logRetentionDays: 30 }
}
})
ipcMain.handle('admin:saveSettings', async (_e, payload: { logRetentionDays?: number }) => {
try {
const saved = dbSaveAdminSettings({ logRetentionDays: Number(payload?.logRetentionDays) })
return { ok: true, ...saved }
} catch (e: any) {
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('ozon:testAuth', async () => {
let storeClientId: string | null = null
try { storeClientId = loadSecrets().clientId } catch {}
const logId = dbLogStart('check_auth', storeClientId)
try {
const secrets = loadSecrets()
await ozonTestAuth(secrets)
try {
const name = await ozonGetStoreName(secrets)
if (name) updateStoreName(name)
} catch {
}
dbLogFinish(logId, { status: 'success', storeClientId: secrets.clientId })
const refreshed = loadSecrets()
return { ok: true, storeName: refreshed.storeName ?? null }
} catch (e: any) {
dbLogFinish(logId, { status: 'error', errorMessage: e?.message ?? String(e), errorDetails: e?.details, storeClientId })
return { ok: false, error: e?.message ?? String(e) }
}
})
ipcMain.handle('ozon:syncProducts', async (_e, args?: { salesPeriod?: SalesPeriod | null }) => {
const resp = await performProductsSync(args)
if (resp?.ok) emitRendererDataUpdatedEvents()
return resp
})
ipcMain.handle('data:refreshSales', async (_e, args?: { period?: SalesPeriod | null }) => {
  return await handleRefreshSales(args?.period ?? null)
})
ipcMain.handle('data:getDatasetRows', async (_e, args?: { dataset?: string; period?: SalesPeriod | null }) => {
  try {
    return await handleGetDatasetRows(args?.dataset, args?.period ?? null)
  } catch (e: any) {
    const dataset = String(args?.dataset ?? 'products').trim() || 'products'
    return { ok: false, error: e?.message ?? String(e), dataset, rows: [] }
  }
})
ipcMain.handle('data:getProducts', async () => {
  try {
    return await handleGetProducts()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), products: [] }
  }
})
ipcMain.handle('data:getSales', async (_e, args?: { period?: SalesPeriod | null }) => {
  try {
    return await handleGetSales(args?.period ?? null)
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), rows: [] }
  }
})
ipcMain.handle('data:getReturns', async () => {
  try {
    return await handleGetReturns()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), rows: [] }
  }
})
ipcMain.handle('data:getStocks', async () => {
  try {
    return await handleGetStocks()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), rows: [] }
  }
})
ipcMain.handle('ui:getGridColumns', async (_e, args: { dataset: string }) => {
  try {
    return await handleGetGridColumns(args?.dataset)
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), dataset: String(args?.dataset ?? 'products'), cols: null }
  }
})
ipcMain.handle('ui:saveGridColumns', async (_e, args: { dataset: string; cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }> }) => {
  try {
    return await handleSaveGridColumns(args?.dataset, args?.cols)
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), dataset: String(args?.dataset ?? 'products'), savedCount: 0 }
  }
})
ipcMain.handle('data:getSyncLog', async () => {
  try {
    return await handleGetSyncLog()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e), logs: [] }
  }
})
ipcMain.handle('data:clearLogs', async () => {
  try {
    return await handleClearLogs()
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e) }
  }
})


ipcMain.handle('logs:saveReportToDesktop', async (_e, args?: { fileName?: string; content?: string }) => {
  try {
    const fileName = String(args?.fileName ?? '').trim()
    const content = typeof args?.content === 'string' ? args.content : ''
    if (!fileName) return { ok: false, error: 'EMPTY_FILE_NAME' }

    const filePath = buildUniqueDesktopFilePath(fileName)
    writeFileSync(filePath, content, 'utf8')
    return { ok: true, path: filePath }
  } catch (e: any) {
    return { ok: false, error: e?.message ?? String(e ?? 'SAVE_REPORT_FAILED') }
  }
})

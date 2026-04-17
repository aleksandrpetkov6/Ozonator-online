import { contextBridge, ipcRenderer } from 'electron'

type LocalServerConfig = {
  ok: boolean
  baseUrl?: string
  healthUrlLocal?: string
  token?: string
  webhookPath?: string
  webhookUrlLocal?: string
  webhookProbePath?: string
  webhookProbeUrlLocal?: string
  webhookToken?: string
  serverStartedAt?: string
  lastProbeAt?: string
  lastPushHitAt?: string
  lastPushAcceptedAt?: string
  lastPushAcceptedEvents?: number
}

let localCfg: LocalServerConfig | null = null
let localCfgPromise: Promise<LocalServerConfig | null> | null = null

async function getLocalServerConfig(): Promise<LocalServerConfig | null> {
  if (localCfg) return localCfg
  if (!localCfgPromise) {
    localCfgPromise = ipcRenderer.invoke('local-server:getConfig')
      .then((resp: any) => {
        if (resp && resp.ok && typeof resp.baseUrl === 'string' && typeof resp.token === 'string') {
          return {
            ok: true,
            baseUrl: resp.baseUrl,
            healthUrlLocal: typeof resp.healthUrlLocal === 'string' ? resp.healthUrlLocal : undefined,
            token: resp.token,
            webhookPath: typeof resp.webhookPath === 'string' ? resp.webhookPath : undefined,
            webhookUrlLocal: typeof resp.webhookUrlLocal === 'string' ? resp.webhookUrlLocal : undefined,
            webhookProbePath: typeof resp.webhookProbePath === 'string' ? resp.webhookProbePath : undefined,
            webhookProbeUrlLocal: typeof resp.webhookProbeUrlLocal === 'string' ? resp.webhookProbeUrlLocal : undefined,
            webhookToken: typeof resp.webhookToken === 'string' ? resp.webhookToken : undefined,
            serverStartedAt: typeof resp.serverStartedAt === 'string' ? resp.serverStartedAt : undefined,
            lastProbeAt: typeof resp.lastProbeAt === 'string' ? resp.lastProbeAt : undefined,
            lastPushHitAt: typeof resp.lastPushHitAt === 'string' ? resp.lastPushHitAt : undefined,
            lastPushAcceptedAt: typeof resp.lastPushAcceptedAt === 'string' ? resp.lastPushAcceptedAt : undefined,
            lastPushAcceptedEvents: typeof resp.lastPushAcceptedEvents === 'number' ? resp.lastPushAcceptedEvents : undefined,
          }
        }
        return null
      })
      .catch(() => null)
  }
  localCfg = await localCfgPromise
  return localCfg
}

async function tryCallLocalServer(method: 'GET' | 'POST', path: string, body?: any): Promise<any | null> {
  try {
    const cfg = await getLocalServerConfig()
    if (!cfg?.ok || !cfg.baseUrl || !cfg.token) return null
    const url = cfg.baseUrl + path
    const res = await fetch(url, {
      method,
      headers: {
        'Content-Type': 'application/json',
        'X-Ozonator-Token': cfg.token,
      } as any,
      body: method === 'POST' ? JSON.stringify(body ?? {}) : undefined,
    })
    const text = await res.text()
    try { return JSON.parse(text) } catch { return { ok: false, error: 'Invalid JSON from local server', __raw_text: text } }
  } catch {
    // If server is not reachable (e.g., port changed), drop cache and fallback to IPC.
    localCfg = null
    localCfgPromise = null
    return null
  }
}

contextBridge.exposeInMainWorld('api', {
  localServerConfig: () => ipcRenderer.invoke('local-server:getConfig'),
  localServerProbe: () => ipcRenderer.invoke('local-server:probe'),
  getBootstrapState: () => ipcRenderer.invoke('app:getBootstrapState'),
  getBootstrapProgress: () => ipcRenderer.invoke('app:getBootstrapProgress'),
  setBootstrapSkipInitialSync: (skipInitialSync: boolean) => ipcRenderer.invoke('app:setBootstrapSkipInitialSync', { skipInitialSync }),

  secretsStatus: () => ipcRenderer.invoke('secrets:status'),
  saveSecrets: (secrets: { storeName?: string; clientId: string; apiKey: string }) => ipcRenderer.invoke('secrets:save', secrets),
  loadSecrets: () => ipcRenderer.invoke('secrets:load'),
  deleteSecrets: () => ipcRenderer.invoke('secrets:delete'),
  netCheck: () => ipcRenderer.invoke('net:check'),

  getAdminSettings: () => ipcRenderer.invoke('admin:getSettings'),
  saveAdminSettings: (payload: { logRetentionDays: number }) => ipcRenderer.invoke('admin:saveSettings', payload),

  testAuth: () => ipcRenderer.invoke('ozon:testAuth'),

  syncProducts: async (salesPeriod?: { from?: string; to?: string } | null) => {
    const resp = await tryCallLocalServer('POST', '/sync/products', { salesPeriod: salesPeriod ?? null })
    if (resp) return resp
    return ipcRenderer.invoke('ozon:syncProducts', { salesPeriod: salesPeriod ?? null })
  },

  refreshSales: async (period?: { from?: string; to?: string } | null) => {
    const resp = await tryCallLocalServer('POST', '/sync/sales', { period: period ?? null })
    if (resp) return resp
    return ipcRenderer.invoke('data:refreshSales', { period: period ?? null })
  },

  getDatasetRows: async (dataset: string, options?: { period?: { from?: string; to?: string } | null }) => {
    const resp = await tryCallLocalServer('POST', '/data/dataset', { dataset, period: options?.period ?? null })
    if (resp) return resp
    return ipcRenderer.invoke('data:getDatasetRows', { dataset, period: options?.period ?? null })
  },

  getProducts: async () => {
    const resp = await tryCallLocalServer('GET', '/data/products')
    if (resp) return resp
    return ipcRenderer.invoke('data:getProducts')
  },

  getSales: async (period?: { from?: string; to?: string }) => {
    const resp = await tryCallLocalServer('POST', '/data/sales', { period: period ?? null })
    if (resp) return resp
    return ipcRenderer.invoke('data:getSales', { period: period ?? null })
  },

  getReturns: async () => {
    const resp = await tryCallLocalServer('GET', '/data/returns')
    if (resp) return resp
    return ipcRenderer.invoke('data:getReturns')
  },

  getStocks: async () => {
    const resp = await tryCallLocalServer('GET', '/data/stocks')
    if (resp) return resp
    return ipcRenderer.invoke('data:getStocks')
  },

  getGridColumns: async (dataset: string) => {
    const resp = await tryCallLocalServer('POST', '/ui/grid-columns/get', { dataset })
    if (resp) return resp
    return ipcRenderer.invoke('ui:getGridColumns', { dataset })
  },

  saveGridColumns: async (dataset: string, cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }>) => {
    const resp = await tryCallLocalServer('POST', '/ui/grid-columns/save', { dataset, cols })
    if (resp) return resp
    return ipcRenderer.invoke('ui:saveGridColumns', { dataset, cols })
  },

  getSyncLog: async () => {
    const resp = await tryCallLocalServer('GET', '/logs/sync')
    if (resp) return resp
    return ipcRenderer.invoke('data:getSyncLog')
  },

  clearLogs: async () => {
    const resp = await tryCallLocalServer('POST', '/logs/clear', {})
    if (resp) return resp
    return ipcRenderer.invoke('data:clearLogs')
  },

  saveLogReportToDesktop: (fileName: string, content: string) => ipcRenderer.invoke('logs:saveReportToDesktop', { fileName, content }),
})

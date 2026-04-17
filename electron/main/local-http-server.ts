import * as http from 'http'
import { randomBytes } from 'crypto'

export type LocalHttpServerConfig = {
  baseUrl: string
  healthUrlLocal: string
  host: string
  port: number
  token: string
  webhookToken: string
  webhookPath: string
  webhookUrlLocal: string
  webhookProbePath: string
  webhookProbeUrlLocal: string
  startedAt: string
}

export type LocalHttpServerHandlers = {
  syncProducts: (payload: { salesPeriod?: any | null }) => Promise<any>
  refreshSales: (payload: { period?: any | null }) => Promise<any>
  getDatasetRows: (payload: { dataset?: string; period?: any | null }) => Promise<any>
  getProducts: () => Promise<any>
  getSales: (payload: { period?: any | null }) => Promise<any>
  getReturns: () => Promise<any>
  getStocks: () => Promise<any>
  getGridColumns: (payload: { dataset: string }) => Promise<any>
  saveGridColumns: (payload: { dataset: string; cols: Array<{ id: string; w: number; visible: boolean; hiddenBucket: 'main' | 'add' }> }) => Promise<any>
  getSyncLog: () => Promise<any>
  clearLogs: () => Promise<any>
  ingestOzonPush: (payload: any, meta: {
    pathname: string
    searchParams: URLSearchParams
    headers: http.IncomingHttpHeaders
    remoteAddress: string | null
  }) => Promise<any>
  probeOzonPush: (meta: {
    pathname: string
    searchParams: URLSearchParams
    headers: http.IncomingHttpHeaders
    remoteAddress: string | null
  }) => Promise<any>
}

export type LocalHttpServerHandle = {
  config: LocalHttpServerConfig
  close: () => Promise<void>
}

type RouteHandler = (payload: any) => Promise<any>

function readRequestBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = []
    req.on('data', (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(String(c))))
    req.on('end', () => {
      if (chunks.length === 0) return resolve(null)
      const text = Buffer.concat(chunks).toString('utf8')
      try { resolve(JSON.parse(text)) } catch { resolve({ __raw_text: text }) }
    })
    req.on('error', () => resolve(null))
  })
}

function sendJson(res: http.ServerResponse, status: number, body: any) {
  const json = JSON.stringify(body ?? null)
  res.statusCode = status
  res.setHeader('Content-Type', 'application/json; charset=utf-8')
  res.setHeader('Cache-Control', 'no-store')
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'content-type,x-ozonator-token')
  res.end(json)
}

function ok(body: any) {
  return { ok: true, ...body }
}

function fail(error: any) {
  return { ok: false, error: error?.message ?? String(error) }
}

function normalizeHost(input: string | null | undefined): string {
  const host = String(input ?? '').trim()
  if (!host) return '127.0.0.1'
  return host
}

export async function startLocalHttpServer(args: {
  handlers: LocalHttpServerHandlers
  host?: string | null
  port?: number | null
  token?: string | null
  webhookToken?: string | null
}): Promise<LocalHttpServerHandle> {
  const host = normalizeHost(args.host)
  const token = String(args.token ?? '').trim() || randomBytes(24).toString('base64url')
  const webhookToken = String(args.webhookToken ?? '').trim() || randomBytes(24).toString('base64url')
  const desiredPort = Number(args.port ?? 0)
  const port = Number.isFinite(desiredPort) && desiredPort >= 0 ? Math.trunc(desiredPort) : 0
  const healthPath = '/health'
  const webhookPath = `/webhooks/ozon/fbo-state/${encodeURIComponent(webhookToken)}`
  const webhookProbePath = `${webhookPath}/ping`

  const routes: Record<string, { method: string; handler: RouteHandler }> = {
    [healthPath]: { method: 'GET', handler: async () => ok({ status: 'ok' }) },

    '/sync/products': { method: 'POST', handler: async (payload) => args.handlers.syncProducts(payload ?? {}) },
    '/sync/sales': { method: 'POST', handler: async (payload) => args.handlers.refreshSales(payload ?? {}) },

    '/data/dataset': { method: 'POST', handler: async (payload) => args.handlers.getDatasetRows(payload ?? {}) },
    '/data/products': { method: 'GET', handler: async () => args.handlers.getProducts() },
    '/data/sales': { method: 'POST', handler: async (payload) => args.handlers.getSales(payload ?? {}) },
    '/data/returns': { method: 'GET', handler: async () => args.handlers.getReturns() },
    '/data/stocks': { method: 'GET', handler: async () => args.handlers.getStocks() },

    '/ui/grid-columns/get': { method: 'POST', handler: async (payload) => args.handlers.getGridColumns(payload ?? {}) },
    '/ui/grid-columns/save': { method: 'POST', handler: async (payload) => args.handlers.saveGridColumns(payload ?? {}) },

    '/logs/sync': { method: 'GET', handler: async () => args.handlers.getSyncLog() },
    '/logs/clear': { method: 'POST', handler: async () => args.handlers.clearLogs() },
  }

  const server = http.createServer(async (req, res) => {
    try {
      const method = String(req.method ?? 'GET').toUpperCase()
      const url = new URL(req.url ?? '/', `http://${host}`)
      const pathname = url.pathname

      if (method === 'OPTIONS') return sendJson(res, 200, ok({}))

      if (method === 'POST' && pathname === webhookPath) {
        const payload = await readRequestBody(req)
        const out = await args.handlers.ingestOzonPush(payload, {
          pathname,
          searchParams: url.searchParams,
          headers: req.headers,
          remoteAddress: req.socket?.remoteAddress ?? null,
        })
        return sendJson(res, 200, out ?? ok({ accepted: true }))
      }

      if (method === 'GET' && pathname === webhookProbePath) {
        const out = await args.handlers.probeOzonPush({
          pathname,
          searchParams: url.searchParams,
          headers: req.headers,
          remoteAddress: req.socket?.remoteAddress ?? null,
        })
        return sendJson(res, 200, out ?? ok({ accepted: true }))
      }

      const route = routes[pathname]
      if (!route || route.method !== method) {
        return sendJson(res, 404, fail('Not found'))
      }

      if (pathname !== '/health') {
        const provided = String(req.headers['x-ozonator-token'] ?? '').trim()
        if (!provided || provided !== token) {
          return sendJson(res, 401, fail('Unauthorized'))
        }
      }

      const payload = method === 'POST' ? await readRequestBody(req) : null
      const out = await route.handler(payload)
      return sendJson(res, 200, out ?? ok({}))
    } catch (e: any) {
      return sendJson(res, 500, fail(e))
    }
  })

  const actualPort: number = await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen({ host, port }, () => {
      const addr = server.address()
      if (addr && typeof addr === 'object') return resolve(addr.port)
      resolve(port)
    })
  })

  const config: LocalHttpServerConfig = {
    baseUrl: `http://${host}:${actualPort}`,
    healthUrlLocal: `http://${host}:${actualPort}${healthPath}`,
    host,
    port: actualPort,
    token,
    webhookToken,
    webhookPath,
    webhookUrlLocal: `http://${host}:${actualPort}${webhookPath}`,
    webhookProbePath,
    webhookProbeUrlLocal: `http://${host}:${actualPort}${webhookProbePath}`,
    startedAt: new Date().toISOString(),
  }

  return {
    config,
    close: async () => {
      await new Promise<void>((resolve) => {
        try {
          server.close(() => resolve())
        } catch {
          resolve()
        }
      })
    },
  }
}

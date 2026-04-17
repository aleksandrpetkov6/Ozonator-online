import type { Secrets } from './types'

/**
 * Ozon Seller API client (без внешних зависимостей).
 * Цель: получить расширенные поля по товарам (SKU/штрихкод/бренд/категория/видимость/дата создания/наименование),
 * чтобы в интерфейсе не было прочерков.
 */

const OZON_BASE = 'https://api-seller.ozon.ru'

export type OzonApiCaptureEvent = {
  storeClientId: string | null
  method: 'GET' | 'POST'
  endpoint: string
  requestBody: any
  responseBody: any
  httpStatus: number
  isSuccess: boolean
  errorMessage?: string | null
  fetchedAt: string
}

let ozonApiCaptureHook: ((event: OzonApiCaptureEvent) => void | Promise<void>) | null = null

export function setOzonApiCaptureHook(fn: ((event: OzonApiCaptureEvent) => void | Promise<void>) | null) {
  ozonApiCaptureHook = fn
}

function headers(secrets: Secrets) {
  return {
    'Client-Id': secrets.clientId,
    'Api-Key': secrets.apiKey,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  }
}

function normalizeError(message: string, details?: any) {
  const err: any = new Error(message)
  err.details = details
  return err
}

async function parseJsonSafe(text: string) {
  try { return JSON.parse(text) } catch { return null }
}

const OZON_RETRYABLE_STATUSES = new Set([429, 500, 502, 503, 504])
const OZON_MAX_RETRIES = 3
const OZON_RETRY_DELAY_MS = 1200
const OZON_REQUEST_TIMEOUT_MS = 25_000
const OZON_REQUEST_MIN_GAP_MS = 450

let ozonRequestQueue: Promise<void> = Promise.resolve()
let ozonRequestNextAllowedAt = 0

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForOzonRequestTurn() {
  const previous = ozonRequestQueue
  let releaseCurrent: (() => void) | null = null

  ozonRequestQueue = new Promise<void>((resolve) => {
    releaseCurrent = resolve
  })

  await previous

  const waitMs = ozonRequestNextAllowedAt - Date.now()
  if (waitMs > 0) await sleep(waitMs)

  return () => {
    ozonRequestNextAllowedAt = Date.now() + OZON_REQUEST_MIN_GAP_MS
    releaseCurrent?.()
  }
}

function getRetryAfterMs(res: Response): number | null {
  const raw = String(res.headers.get('retry-after') ?? '').trim()
  if (!raw) return null
  const asSeconds = Number(raw)
  if (Number.isFinite(asSeconds) && asSeconds >= 0) {
    return Math.min(30_000, Math.max(0, Math.trunc(asSeconds * 1000)))
  }
  const asDate = Date.parse(raw)
  if (Number.isNaN(asDate)) return null
  return Math.min(30_000, Math.max(0, asDate - Date.now()))
}

async function ozonRequest(secrets: Secrets, method: 'GET'|'POST', endpoint: string, body?: any) {
  const url = `${OZON_BASE}${endpoint}`

  for (let attempt = 0; attempt <= OZON_MAX_RETRIES; attempt += 1) {
    const releaseTurn = await waitForOzonRequestTurn()
    let res: Response
    let text = ''
    let json: any = null
    let retryWaitMs: number | null = null

    try {
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), OZON_REQUEST_TIMEOUT_MS)

      try {
        res = await fetch(url, {
          method,
          headers: headers(secrets) as any,
          body: method === 'POST' ? JSON.stringify(body ?? {}) : undefined,
          signal: controller.signal,
        })
        text = await res.text()
      } catch (error: any) {
        if (error?.name === 'AbortError') {
          throw normalizeError(`Ozon API error: timeout after ${OZON_REQUEST_TIMEOUT_MS} ms`, { endpoint, body, timeoutMs: OZON_REQUEST_TIMEOUT_MS })
        }
        throw error
      } finally {
        clearTimeout(timeoutId)
      }

      json = await parseJsonSafe(text)
      const shouldRetry = !res.ok && OZON_RETRYABLE_STATUSES.has(res.status) && attempt < OZON_MAX_RETRIES

      if (ozonApiCaptureHook) {
        const fetchedAt = new Date().toISOString()
        await ozonApiCaptureHook({
          storeClientId: String(secrets?.clientId ?? '').trim() || null,
          method,
          endpoint,
          requestBody: body ?? null,
          responseBody: json ?? { __raw_text: text },
          httpStatus: res.status,
          isSuccess: res.ok,
          errorMessage: res.ok ? null : `Ozon API error: HTTP ${res.status}`,
          fetchedAt,
        })
      }

      if (shouldRetry) {
        const retryAfterMs = getRetryAfterMs(res)
        retryWaitMs = retryAfterMs ?? (OZON_RETRY_DELAY_MS * (attempt + 1))
      } else if (!res.ok) {
        throw normalizeError(`Ozon API error: HTTP ${res.status}`, { status: res.status, endpoint, body, response: json ?? text })
      } else {
        return json
      }
    } finally {
      releaseTurn()
    }

    if (retryWaitMs != null) {
      await sleep(retryWaitMs)
      continue
    }
  }

  throw normalizeError('Ozon API error: retry loop exhausted', { endpoint, body })
}

async function ozonPost(secrets: Secrets, endpoint: string, body: any) {
  return ozonRequest(secrets, 'POST', endpoint, body)
}

async function ozonGet(secrets: Secrets, endpoint: string) {
  return ozonRequest(secrets, 'GET', endpoint)
}

// ---------------- Types ----------------

type ListItemV3 = {
  offer_id: string
  product_id?: number
  sku?: string
  archived?: boolean
}

export type OzonProductInfo = {
  product_id: number
  offer_id: string
  sku: string | null
  ozon_sku: string | null
  seller_sku: string | null
  fbo_sku: string | null
  fbs_sku: string | null
  barcode: string | null
  brand: string | null
  category: string | null
  type: string | null
  name: string | null
  photo_url: string | null
  is_visible: boolean | number | null
  hidden_reasons: string | null
  created_at: string | null
}

type ProductInfoV2 = {
  id?: number
  product_id?: number
  offer_id?: string
  sku?: number | string
  barcode?: string
  barcodes?: string[]
  category_id?: number
  created_at?: string
  archived?: boolean
  primary_image?: string
  image?: string
  images?: Array<string | { url?: string; file_name?: string; default?: string }> | null
  visible?: boolean
  description_category_id?: number
  type_id?: number
  visibility_details?: any
  visibilityDetails?: any
  name?: string
  product_name?: string
  title?: string
  status?: {
    decline_reasons?: any[]
    item_errors?: any[]
  }
  errors?: any[]
}

type AttrValue = { value?: string; dictionary_value_id?: number }

type Attribute = { id: number; name?: string; values?: AttrValue[]; [key: string]: any }

type ProductAttributesV3 = {
  id?: number
  product_id?: number
  offer_id?: string
  barcode?: string
  category_id?: number
  description_category_id?: number
  type_id?: number
  attributes?: Attribute[]
  complex_attributes?: Array<{ attributes?: Attribute[] }>
  [key: string]: any
}

function collectTextCandidates(input: any, out: string[] = [], depth = 0): string[] {
  if (input == null || depth > 5) return out

  if (typeof input === 'string') {
    const t = input.trim()
    if (t) out.push(t)
    return out
  }

  if (typeof input === 'number' || typeof input === 'boolean') {
    out.push(String(input))
    return out
  }

  if (Array.isArray(input)) {
    for (const v of input) collectTextCandidates(v, out, depth + 1)
    return out
  }

  if (typeof input === 'object') {
    const preferredKeys = [
      'value', 'name', 'text', 'label',
      'dictionary_value', 'dictionaryValue',
      'dictionary_value_name', 'dictionaryValueName',
      'display_value', 'displayValue',
    ]
    for (const k of preferredKeys) {
      if (Object.prototype.hasOwnProperty.call(input, k)) {
        collectTextCandidates((input as any)[k], out, depth + 1)
      }
    }
  }

  return out
}

function normalizeBrandText(raw: any): string | null {
  const candidates = collectTextCandidates(raw)
  for (const c of candidates) {
    const t = String(c).trim()
    if (!t) continue
    if (/^\d+$/.test(t)) continue
    if (t === '[object Object]') continue
    return t
  }
  return null
}

function extractBrandFromAttributes(attrs: Array<any>): string | null {
  const brandIds = new Set([85, 31])

  for (const a of attrs) {
    if (!a) continue
    if (!brandIds.has(Number((a as any).id))) continue

    const values = Array.isArray((a as any).values) ? (a as any).values : []

    // Основной контракт для бренда:
    // attributes[].id (85/31) -> values[0].value
    const direct = normalizeBrandText((values[0] as any)?.value)
    if (direct) return direct

    // Fallback: иногда структура values отличается (словарные/нестандартные формы).
    const fromValues = normalizeBrandText(values)
    if (fromValues) return fromValues

    const fromAttr = normalizeBrandText(a)
    if (fromAttr) return fromAttr
  }

  // Последний fallback: поиск по названию атрибута, если в категории id отличается.
  for (const a of attrs) {
    const name = String((a as any)?.name ?? '').trim().toLowerCase()
    if (!name) continue
    if (name.includes('бренд') || name === 'brand') {
      const values = Array.isArray((a as any).values) ? (a as any).values : []
      const direct = normalizeBrandText((values[0] as any)?.value)
      if (direct) return direct

      const v = normalizeBrandText(values) ?? normalizeBrandText(a)
      if (v) return v
    }
  }

  return null
}

// ---------------- Helpers ----------------

function chunk<T>(arr: T[], size: number) {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

function stringifyReason(x: any): string {
  if (x == null) return ''
  if (typeof x === 'string') return x
  if (typeof x === 'number' || typeof x === 'boolean') return String(x)
  if (typeof x === 'object') {
    if (typeof (x as any).message === 'string') return (x as any).message
    if (typeof (x as any).error === 'string') return (x as any).error
    try { return JSON.stringify(x) } catch { return String(x) }
  }
  return String(x)
}

function collectReasonPartsFromAny(src: any, out: string[]) {
  if (src == null) return

  if (Array.isArray(src)) {
    for (const it of src) collectReasonPartsFromAny(it, out)
    return
  }

  if (typeof src === 'string' || typeof src === 'number' || typeof src === 'boolean') {
    const s = String(src).trim()
    if (s) out.push(s)
    return
  }

  if (typeof src === 'object') {
    const obj = src as any
    const direct = pickFirstString(
      obj.reason,
      obj.message,
      obj.text,
      obj.name,
      obj.error,
      obj.description,
      obj.title,
      obj.code,
    )
    if (direct) {
      out.push(direct)
      return
    }

    for (const v of Object.values(obj)) {
      if (v == null) continue
      if (typeof v === 'object') collectReasonPartsFromAny(v, out)
      else {
        const sv = String(v).trim()
        if (sv) out.push(sv)
      }
    }
    return
  }

  const fallback = stringifyReason(src).trim()
  if (fallback) out.push(fallback)
}

function buildHiddenReasons(info: ProductInfoV2): string | null {
  const parts: string[] = []

  // Основной источник по задаче: visibility_details.reasons из /v3/product/info/list.
  const vis = (info as any).visibility_details ?? (info as any).visibilityDetails
  collectReasonPartsFromAny(vis?.reasons, parts)

  // Fallback на старые поля — только если visibility_details.reasons пустой.
  if (!parts.length) {
    const dr = info.status?.decline_reasons
    if (Array.isArray(dr)) {
      for (const r of dr) collectReasonPartsFromAny(r, parts)
    }

    const ie = info.status?.item_errors
    if (Array.isArray(ie)) {
      for (const r of ie) collectReasonPartsFromAny(r, parts)
    }

    const e = info.errors
    if (Array.isArray(e)) {
      for (const r of e) collectReasonPartsFromAny(r, parts)
    }
  }

  if (!parts.length) return null
  return Array.from(new Set(parts)).slice(0, 12).join('; ')
}


function toNumId(v: any): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v)
    if (Number.isFinite(n)) return n
  }
  return null
}

type CategoryTreeNames = { categoryName?: string | null; typeName?: string | null }
type CategoryTreeMaps = {
  byPair: Map<string, CategoryTreeNames>
  byTypeId: Map<number, CategoryTreeNames>
  byDescriptionCategoryId: Map<number, CategoryTreeNames>
}

async function fetchCategoryTreeMaps(secrets: Secrets): Promise<CategoryTreeMaps> {
  const byPair = new Map<string, CategoryTreeNames>()
  const byTypeId = new Map<number, CategoryTreeNames>()
  const byDescriptionCategoryId = new Map<number, CategoryTreeNames>()

  const candidates: Array<() => Promise<any>> = [
    () => ozonPost(secrets, '/v1/description-category/tree', { language: 'RU' }),
    () => ozonPost(secrets, '/v1/description-category/tree', { language: 'DEFAULT' }),
    () => ozonPost(secrets, '/v1/description-category/tree', {}),
    () => ozonGet(secrets, '/v1/description-category/tree'),
  ]

  let payload: any = null
  for (const fn of candidates) {
    try {
      payload = await fn()
      break
    } catch (e: any) {
      const st = e?.details?.status
      if (st && st !== 404) {
        // пробуем следующий вариант формы запроса
      }
    }
  }

  if (!payload) return { byPair, byTypeId, byDescriptionCategoryId }

  const seen = new Set<any>()
  const walk = (node: any) => {
    if (!node || typeof node !== 'object') return
    if (seen.has(node)) return
    seen.add(node)

    if (Array.isArray(node)) {
      for (const it of node) walk(it)
      return
    }

    const n: any = node
    const descriptionCategoryId = toNumId(n.description_category_id ?? n.descriptionCategoryId)
    const typeId = toNumId(n.type_id ?? n.typeId ?? n.type?.id ?? n.type?.type_id ?? n.type?.typeId)
    const categoryName = pickFirstString(n.category_name, n.categoryName, n.description_category_name, n.descriptionCategoryName)
    const typeName = pickFirstString(n.type_name, n.typeName, n.type?.name, n.type?.type_name, n.type?.typeName)

    if ((descriptionCategoryId || typeId) && (categoryName || typeName)) {
      const names: CategoryTreeNames = { categoryName: categoryName ?? null, typeName: typeName ?? null }

      if (descriptionCategoryId && typeId) {
        byPair.set(`${descriptionCategoryId}:${typeId}`, names)
      }

      if (typeId && !byTypeId.has(typeId)) {
        byTypeId.set(typeId, names)
      }

      if (descriptionCategoryId && !byDescriptionCategoryId.has(descriptionCategoryId)) {
        byDescriptionCategoryId.set(descriptionCategoryId, names)
      }
    }

    for (const v of Object.values(n)) walk(v)
  }

  walk(payload)

  return { byPair, byTypeId, byDescriptionCategoryId }
}

async function fetchAttributesMap(
  secrets: Secrets,
  lookupItems: Array<{ product_id?: number | null; offer_id?: string | null }>
) {
  const map = new Map<number, { brand?: string | null; barcode?: string | null; category?: string | null; descriptionCategoryId?: number | null; typeId?: number | null }>()


  // Бренд для существующего товара тянем через метод характеристик:
  // основной путь: /v4/product/info/attributes (singular),
  // fallback: /v3/product/info/attributes,
  // затем оставляем совместимые fallback на старые plural-варианты.
  async function callWithFallback(body: any) {
    const endpoints = [
      '/v4/product/info/attributes',
      '/v3/product/info/attributes',
      '/v4/products/info/attributes',
      '/v3/products/info/attributes',
    ] as const

    let lastErr: any = null
    for (const endpoint of endpoints) {
      try {
        return await ozonPost(secrets, endpoint, body)
      } catch (e: any) {
        lastErr = e
        if (e?.details?.status !== 404) throw e
      }
    }

    throw lastErr ?? normalizeError('Ozon attributes endpoint not available')
  }

  for (const pack of chunk(lookupItems, 900)) {
    const ids = Array.from(new Set(pack
      .map((x) => toNumId((x as any)?.product_id))
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
    ))
    const offerIds = Array.from(new Set(pack
      .map((x) => (typeof (x as any)?.offer_id === 'string' ? (x as any).offer_id.trim() : ''))
      .filter((v): v is string => Boolean(v))
    ))

    if (!ids.length && !offerIds.length) continue

    let last_id = ''
    for (let guard = 0; guard < 20; guard++) {
      const filter: any = { visibility: 'ALL' }
      if (ids.length) filter.product_id = ids
      if (offerIds.length) filter.offer_id = offerIds

      const body = {
        filter,
        limit: 1000,
        last_id,
      }

      const json: any = await callWithFallback(body)

      const data = json?.data ?? json?.result ?? json ?? {}

      // Встречались варианты ответа:
      // 1) { result: { items: [...], last_id: "" } }
      // 2) { result: [...], last_id: "" }
      // 3) { result: [...] }
      // 4) { items: [...] }
      // 5) просто массив (редко)
      const list: ProductAttributesV3[] = Array.isArray(data)
        ? (data as any)
        : Array.isArray((data as any)?.items)
          ? (data as any).items
          : Array.isArray((data as any)?.result)
            ? (data as any).result
            : Array.isArray((data as any)?.result?.items)
              ? (data as any).result.items
              : []

      for (const x of list) {
        const pid = Number((x as any).id ?? (x as any).product_id ?? (x as any).productId)
        if (!pid) continue

        const attrsFlat: Attribute[] = []
        if (Array.isArray((x as any).attributes)) attrsFlat.push(...((x as any).attributes as Attribute[]))
        if (Array.isArray((x as any).complex_attributes)) {
          for (const ca of (x as any).complex_attributes) {
            if (Array.isArray((ca as any)?.attributes)) attrsFlat.push(...((ca as any).attributes as Attribute[]))
          }
        }

        // Бренд: берём текст из атрибутов (в т.ч. вложенных) по id 85/31.
        // Если у Ozon в категории другой id, дополнительно ищем атрибут по имени «Бренд/Brand».
        let brand: string | null = extractBrandFromAttributes(attrsFlat)

        const barcode = x.barcode ? String(x.barcode) : null
        const category = (x.category_id != null) ? String(x.category_id) : null
        const descriptionCategoryId = toNumId((x as any).description_category_id ?? (x as any).descriptionCategoryId)
        const typeId = toNumId((x as any).type_id ?? (x as any).typeId ?? (x as any).type?.id)

        const prev = map.get(pid) ?? {}
        map.set(pid, {
          brand: prev.brand ?? brand,
          barcode: prev.barcode ?? barcode,
          category: prev.category ?? category,
          descriptionCategoryId: prev.descriptionCategoryId ?? descriptionCategoryId,
          typeId: prev.typeId ?? typeId,
        })
      }

      const next = (!Array.isArray(data))
        ? ((data as any).last_id ?? (data as any).lastId ?? (data as any).result?.last_id ?? (data as any).result?.lastId)
        : undefined
      if (!next) break
      if (next === last_id) break
      last_id = String(next)
    }
  }

  return map
}

function pickFirstString(...vals: any[]): string | null {
  for (const v of vals) {
    if (typeof v === 'string' && v.trim()) return v.trim()
  }
  return null
}

function findNestedStringByKeys(input: any, normalizedKeys: Set<string>, depth = 0): string | null {
  if (input == null || depth > 5) return null

  if (Array.isArray(input)) {
    for (const item of input) {
      const found = findNestedStringByKeys(item, normalizedKeys, depth + 1)
      if (found) return found
    }
    return null
  }

  if (typeof input !== 'object') return null

  for (const [rawKey, rawVal] of Object.entries(input)) {
    const normKey = String(rawKey).toLowerCase().replace(/[^a-z0-9]/g, '')
    if (normalizedKeys.has(normKey)) {
      if (typeof rawVal === 'string' && rawVal.trim()) return rawVal.trim()
      if (typeof rawVal === 'number' && Number.isFinite(rawVal)) return String(rawVal)
    }

    const found = findNestedStringByKeys(rawVal, normalizedKeys, depth + 1)
    if (found) return found
  }

  return null
}

function extractProductSkuFields(raw: any): { ozon_sku: string | null; seller_sku: string | null; fbo_sku: string | null; fbs_sku: string | null } {
  const directOzonSku = (() => {
    const v = raw?.ozon_sku ?? raw?.ozonSku ?? raw?.sku
    if (typeof v === 'string' && v.trim()) return v.trim()
    if (typeof v === 'number' && Number.isFinite(v)) return String(v)
    return null
  })()

  const directSellerSku = pickFirstString(
    raw?.seller_sku,
    raw?.sellerSku,
    raw?.offer_sku,
    raw?.offerSku,
    raw?.offer_id,
    raw?.offerId,
    raw?.offerIdForSeller,
  )

  const fboKeys = new Set(['fbosku', 'skufbo'])
  const fbsKeys = new Set(['fbssku', 'skufbs'])

  const directFboSku = pickFirstString(raw?.fbo_sku, raw?.fboSku, raw?.sku_fbo, raw?.skuFbo)
  const directFbsSku = pickFirstString(raw?.fbs_sku, raw?.fbsSku, raw?.sku_fbs, raw?.skuFbs)

  return {
    ozon_sku: directOzonSku,
    seller_sku: directSellerSku,
    fbo_sku: directFboSku ?? findNestedStringByKeys(raw, fboKeys),
    fbs_sku: directFbsSku ?? findNestedStringByKeys(raw, fbsKeys),
  }
}

function normalizePhotoUrl(v: unknown): string | null {
  if (typeof v !== 'string') return null
  const s = v.trim()
  if (!s) return null
  if (s.startsWith('//')) return `https:${s}`
  return s
}

function extractPhotoUrl(x: ProductInfoV2): string | null {
  const direct = pickFirstString(
    x.primary_image,
    x.image,
    (x as any).primaryImage,
    (x as any).image_url,
    (x as any).imageUrl,
    (x as any).photo,
    (x as any).photo_url,
    (x as any).picture,
  )
  if (direct) return normalizePhotoUrl(direct)

  const rawImages = Array.isArray(x.images) ? x.images : []
  const imageObjects = rawImages.filter((item): item is Record<string, any> => !!item && typeof item === 'object')

  const isPrimaryFlag = (v: unknown) => v === true || v === 1 || v === '1' || String(v).toLowerCase() === 'true'
  const pickFromImageObject = (item: Record<string, any>) => (
    normalizePhotoUrl(item.url)
    ?? normalizePhotoUrl(item.file_name)
    ?? normalizePhotoUrl(item.fileName)
    ?? normalizePhotoUrl(typeof item.default === 'string' ? item.default : null)
    ?? normalizePhotoUrl(item.image)
  )

  for (const item of imageObjects) {
    if (
      isPrimaryFlag(item.is_primary)
      || isPrimaryFlag(item.isPrimary)
      || isPrimaryFlag(item.primary)
      || isPrimaryFlag(item.main)
      || isPrimaryFlag(item.is_main)
      || isPrimaryFlag(item.preview)
    ) {
      const v = pickFromImageObject(item)
      if (v) return v
    }
  }

  for (const item of rawImages) {
    if (typeof item === 'string') {
      const v = normalizePhotoUrl(item)
      if (v) return v
      continue
    }
    if (!item || typeof item !== 'object') continue
    const v = pickFromImageObject(item as Record<string, any>)
    if (v) return v
  }
  return null
}

function isReasonableStoreName(s: string): boolean {
  const t = s.trim()
  if (!t) return false
  // Название магазина обычно короткое. Длинные строки часто оказываются "мусором" (токены/идентификаторы).
  if (t.length > 120) return false
  // Похоже на base64/hex/токен — не принимаем как имя магазина.
  if (/^[A-Za-z0-9+/_-]{40,}$/.test(t)) return false
  return true
}

function deepPickFirstString(root: any, keys: string[]): string | null {
  const want = new Set(keys.map(k => k.toLowerCase()))
  const seen = new Set<any>()
  const q: any[] = [root]
  let guard = 0

  while (q.length && guard < 5000) {
    const cur = q.shift()
    guard++

    if (cur == null) continue

    if (typeof cur !== 'object') continue

    if (seen.has(cur)) continue
    seen.add(cur)

    if (Array.isArray(cur)) {
      for (const it of cur) {
        if (it && typeof it === 'object') q.push(it)
      }
      continue
    }

    for (const [kRaw, v] of Object.entries(cur)) {
      const k = String(kRaw).toLowerCase()

      if (want.has(k)) {
        if (typeof v === 'string' && isReasonableStoreName(v)) return v.trim()

        if (v && typeof v === 'object') {
          const candidate = pickFirstString(
            (v as any).name,
            (v as any).title,
            (v as any).value,
            (v as any).company_name,
            (v as any).companyName,
            (v as any).seller_name,
            (v as any).sellerName,
            (v as any).shop_name,
            (v as any).shopName,
          )
          if (candidate && isReasonableStoreName(candidate)) return candidate.trim()
        }
      }

      if (v && typeof v === 'object' && !seen.has(v)) q.push(v)
    }
  }

  return null
}


// ---------------- Public API ----------------

export async function ozonTestAuth(secrets: Secrets) {
  // Лёгкий запрос — просто проверка, что ключи валидны
  await ozonPost(secrets, '/v3/product/list', { filter: {}, last_id: '', limit: 1 })
  return true
}

function buildPostingListBody(limit = 1000) {
  const to = new Date()
  const since = new Date(to.getTime() - (90 * 24 * 60 * 60 * 1000))

  return {
    dir: 'DESC',
    filter: {
      since: since.toISOString(),
      to: to.toISOString(),
    },
    limit,
    offset: 0,
  }
}

export async function ozonPostingFbsList(secrets: Secrets, body?: any) {
  return ozonPost(secrets, '/v3/posting/fbs/list', body ?? buildPostingListBody())
}

export async function ozonPostingFboList(secrets: Secrets, body?: any) {
  return ozonPost(secrets, '/v2/posting/fbo/list', body ?? buildPostingListBody())
}

function buildPostingGetBody(postingNumber: string, includeExtendedFields = false) {
  const normalizedPostingNumber = String(postingNumber ?? '').trim()
  if (!normalizedPostingNumber) throw new Error('Не указан posting_number')

  const body: any = { posting_number: normalizedPostingNumber }

  if (includeExtendedFields) {
    body.with = {
      analytics_data: true,
      financial_data: true,
      barcodes: true,
      related_postings: true,
    }
  }

  return body
}

export async function ozonPostingFbsGet(secrets: Secrets, postingNumber: string) {
  return ozonPost(secrets, '/v3/posting/fbs/get', buildPostingGetBody(postingNumber, true))
}

export async function ozonPostingFboGet(secrets: Secrets, postingNumber: string) {
  return ozonPost(secrets, '/v2/posting/fbo/get', buildPostingGetBody(postingNumber, true))
}

/**
 * Совместимый запрос деталей FBO-отправления.
 *
 * Норма (РД): любые вызовы Seller API обязаны проходить через ozonRequest/ozonPost,
 * чтобы заполнялись raw-cache и endpoint registry (через capture hook).
 *
 * Эта функция нужна для случаев, когда мы пробуем несколько вариантов `with.*`
 * (compat probing), но при этом не имеем права делать прямой fetch из других модулей.
 */
export async function ozonPostingFboGetCompat(secrets: Secrets, body: any) {
  const incoming = (body && typeof body === 'object') ? body : {}
  const postingNumber = String((incoming as any).posting_number ?? (incoming as any).postingNumber ?? '').trim()
  if (!postingNumber) throw new Error('Не указан posting_number')

  // Канонизируем поле под Seller API: posting_number (snake_case).
  const reqBody: any = { ...incoming, posting_number: postingNumber }
  if ('postingNumber' in reqBody) delete reqBody.postingNumber

  return ozonPost(secrets, '/v2/posting/fbo/get', reqBody)
}

export async function ozonGetStoreName(secrets: Secrets): Promise<string | null> {
  // Ozon API периодически меняет версию/эндпойнт: поэтому делаем несколько попыток.
  const candidates: Array<() => Promise<any>> = [
    () => ozonGet(secrets, '/v1/seller/info'),
    () => ozonPost(secrets, '/v1/seller/info', {}),
    () => ozonGet(secrets, '/v1/client/info'),
    () => ozonPost(secrets, '/v1/client/info', {}),
  ]

  for (const fn of candidates) {
    try {
      const j: any = await fn()
      const r = j?.result ?? j?.data ?? j
      const name = pickFirstString(
        r?.name,
        r?.company_name,
        r?.companyName,
        r?.seller_name,
        r?.sellerName,
        r?.shop_name,
        r?.shopName,
        j?.name,
        j?.company_name,
        j?.companyName,
      )
      if (name) return name

      // На части кабинетов нужное поле лежит глубже (например result.company.name).
      // Поэтому делаем «глубокий» поиск по дереву ответа, но с защитой от мусорных длинных строк.
      const deep = deepPickFirstString(r, [
        'name',
        'company_name',
        'companyName',
        'seller_name',
        'sellerName',
        'shop_name',
        'shopName'
      ]) ?? deepPickFirstString(j, [
        'name',
        'company_name',
        'companyName',
        'seller_name',
        'sellerName',
        'shop_name',
        'shopName'
      ])
      if (deep) return deep
    } catch (e: any) {
      const st = e?.details?.status
      if (st && st !== 404) {
        // если это не "не найдено" — считаем, что сеть/ключи/прокси и т.п.
        // не фейлим весь вызов — просто попробуем следующий вариант
      }
    }
  }

  return null
}

type OzonWarehouse = {
  warehouse_id: number
  name: string | null
}

export type OzonPlacementZoneInfo = {
  warehouse_id: number
  /**
   * Legacy canonical key (ozon_sku if present, otherwise seller_sku).
   */
  sku: string
  ozon_sku?: string | null
  seller_sku?: string | null
  placement_zone: string | null
}

function pickWarehouseName(raw: any): string | null {
  const v = pickFirstString(
    raw?.name,
    raw?.warehouse_name,
    raw?.warehouseName,
    raw?.warehouse,
    raw?.warehouse_title,
    raw?.warehouseTitle,
    raw?.title,
    raw?.place_name,
    raw?.placeName
  )
  return v ? v.trim() : null
}

function pickWarehouseId(raw: any): number | null {
  const cand = raw?.warehouse_id ?? raw?.warehouseId ?? raw?.id
  const n = (typeof cand === 'number') ? cand : Number(String(cand ?? '').trim())
  return Number.isFinite(n) ? Math.trunc(n) : null
}

function flattenObjects(root: any): any[] {
  const out: any[] = []
  const seen = new Set<any>()
  const q: any[] = [root]
  while (q.length) {
    const cur = q.shift()
    if (cur == null) continue
    if (typeof cur !== 'object') continue
    if (seen.has(cur)) continue
    seen.add(cur)

    if (Array.isArray(cur)) {
      for (const v of cur) q.push(v)
      continue
    }

    out.push(cur)
    for (const v of Object.values(cur)) q.push(v)
  }
  return out
}

export async function ozonWarehouseList(secrets: Secrets): Promise<OzonWarehouse[]> {
  const candidates: Array<() => Promise<any>> = [
    () => ozonPost(secrets, '/v2/warehouse/list', {}),
    () => ozonGet(secrets, '/v2/warehouse/list'),
    () => ozonPost(secrets, '/v1/warehouse/list', {}),
    () => ozonGet(secrets, '/v1/warehouse/list'),
  ]

  let lastErr: any = null
  for (const fn of candidates) {
    try {
      const j = await fn()
      const objs = flattenObjects(j)
      const rows: OzonWarehouse[] = []
      const seen = new Set<number>()
      for (const obj of objs) {
        const id = pickWarehouseId(obj)
        if (id == null) continue
        if (seen.has(id)) continue
        const name = pickWarehouseName(obj)
        seen.add(id)
        rows.push({ warehouse_id: id, name: name ?? null })
      }
      if (rows.length) return rows.sort((a, b) => a.warehouse_id - b.warehouse_id)
    } catch (e) {
      lastErr = e
      continue
    }
  }

  if (lastErr) throw lastErr
  return []
}

function extractPlacementZoneItems(json: any, warehouseId: number): OzonPlacementZoneInfo[] {
  const objs = flattenObjects(json)
  const out: OzonPlacementZoneInfo[] = []
  const seen = new Set<string>()

  for (const obj of objs) {
    const ozonSkuRaw =
      obj?.sku ??
      obj?.sku_id ??
      obj?.skuId ??
      obj?.product_sku ??
      obj?.productSku ??
      obj?.item_sku

    const sellerSkuRaw =
      obj?.seller_sku ??
      obj?.sellerSku ??
      obj?.offer_sku ??
      obj?.offerSku ??
      obj?.offer_id ??
      obj?.offerId

    let ozonSku = (typeof ozonSkuRaw === 'string' || typeof ozonSkuRaw === 'number') ? String(ozonSkuRaw).trim() : ''
    let sellerSku = (typeof sellerSkuRaw === 'string' || typeof sellerSkuRaw === 'number') ? String(sellerSkuRaw).trim() : ''

    // В ответах Ozon поле `sku` иногда приходит как SKU продавца (offer_id / seller_sku).
    // Если отдельного seller_sku нет и `sku` нечисловой — сохраняем его как seller_sku,
    // чтобы вкладка «Остатки» смогла сматчить размещение с товаром по offer_id.
    if (!sellerSku && ozonSku && !/^\d+$/.test(ozonSku)) {
      sellerSku = ozonSku
      ozonSku = ''
    }

    const canonicalSku = ozonSku || sellerSku
    if (!canonicalSku) continue

    const rowWarehouseId = pickWarehouseId(obj) ?? warehouseId
    const zone = pickFirstString(
      obj?.placement_zone,
      obj?.placementZone,
      obj?.placement_zone_name,
      obj?.placementZoneName,
      obj?.storage_zone,
      obj?.storageZone,
      obj?.zone,
      obj?.zone_name,
      obj?.zoneName,
      obj?.name
    )

    const key = `${rowWarehouseId}::${ozonSku || '-'}::${sellerSku || '-'}::${zone ?? ''}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push({
      warehouse_id: rowWarehouseId,
      sku: canonicalSku,
      ozon_sku: ozonSku || null,
      seller_sku: sellerSku || null,
      placement_zone: zone ? zone.trim() : null,
    })
  }

  return out
}

export async function ozonPlacementZoneInfo(
  secrets: Secrets,
  args: { warehouseId: number; skus: string[] }
): Promise<OzonPlacementZoneInfo[]> {
  const warehouseId = Math.trunc(Number(args.warehouseId))
  const skus = Array.from(new Set((args.skus ?? []).map((x) => String(x ?? '').trim()).filter(Boolean)))
  if (!Number.isFinite(warehouseId) || warehouseId <= 0 || skus.length === 0) return []

  const payloads = [
    { warehouse_id: warehouseId, skus: skus.map((sku) => ({ sku })) },
    { warehouse_id: warehouseId, skus },
    { warehouse_id: warehouseId, sku: skus },
  ]

  let lastErr: any = null
  for (const payload of payloads) {
    try {
      const res = await ozonPost(secrets, '/v1/product/placement-zone/info', payload)
      const rows = extractPlacementZoneItems(res, warehouseId)
      if (rows.length > 0) return rows
    } catch (e) {
      lastErr = e
    }
  }

  if (lastErr) throw lastErr
  return []
}

export async function ozonProductList(secrets: Secrets, opts: { lastId: string; limit: number }) {
  const json = await ozonPost(secrets, '/v3/product/list', {
    filter: {},
    last_id: opts.lastId,
    limit: opts.limit,
  })

  const result = json?.result
  const itemsRaw: any[] = result?.items ?? []
  const lastId: string = String(result?.last_id ?? result?.lastId ?? '')
  const total: number | null = (typeof result?.total === 'number') ? result.total : null

  // В разных версиях API / прокси product_id может прийти как number ИЛИ как строка.
  // Если не привести строку к числу — ids окажутся пустыми, и расширенная информация не подтянется.
  const items: ListItemV3[] = (Array.isArray(itemsRaw) ? itemsRaw : []).map((x) => {
    const pidRaw = (x.product_id ?? x.productId ?? x.id) as any
    const pidNum =
      (typeof pidRaw === 'number') ? pidRaw :
      (typeof pidRaw === 'string' && pidRaw.trim() !== '') ? Number(pidRaw) :
      NaN

    return {
      offer_id: String(x.offer_id ?? ''),
      product_id: Number.isFinite(pidNum) ? pidNum : undefined,
      sku: (typeof x.sku === 'string' || typeof x.sku === 'number') ? String(x.sku) : undefined,
      archived: (typeof x.archived === 'boolean') ? x.archived : undefined,
    }
  }).filter((x) => x.offer_id)

  return { items, lastId, total }
}

export async function ozonProductInfoList(secrets: Secrets, productIds: number[]): Promise<OzonProductInfo[]> {
  if (!productIds.length) return []

  const out: OzonProductInfo[] = []

  function extractItems(json: any): ProductInfoV2[] {
    const r = json?.result
    if (Array.isArray(r)) return r as any
    if (Array.isArray(r?.items)) return r.items as any
    if (Array.isArray(json?.items)) return json.items as any
    if (Array.isArray(r?.result)) return r.result as any
    return []
  }

  async function fetchInfoChunk(ids: number[]) {
    // На части аккаунтов /v2/product/info/list возвращает 404.
    // Поэтому основной путь — /v3/product/info/list, а /v2 используем как fallback.
    try {
      const j3 = await ozonPost(secrets, '/v3/product/info/list', { product_id: ids })
      const items3 = extractItems(j3)
      if (items3.length) return items3
    } catch (e: any) {
      if (e?.details?.status !== 404) throw e
    }

    const j2 = await ozonPost(secrets, '/v2/product/info/list', { product_id: ids })
    return extractItems(j2)
  }

  let categoryTreeMaps: CategoryTreeMaps | null = null
  try {
    categoryTreeMaps = await fetchCategoryTreeMaps(secrets)
  } catch {
    categoryTreeMaps = null
  }

  for (const ids of chunk(productIds, 200)) {
    const items = await fetchInfoChunk(ids)

    for (const x of items) {
      const pid = Number(x.id ?? x.product_id)
      if (!pid) continue

      const barcode = (x.barcode && String(x.barcode)) || (Array.isArray(x.barcodes) && x.barcodes[0]) || null

      const categoryId = toNumId((x as any).category_id ?? (x as any).categoryId ?? (x as any).category?.id)
      const descriptionCategoryId = toNumId((x as any).description_category_id ?? (x as any).descriptionCategoryId)
      const typeId = toNumId((x as any).type_id ?? (x as any).typeId ?? (x as any).type?.id)
      const brandRaw = (x as any).brand ?? (x as any).brand_name ?? (x as any).brandName ?? null
      const visibleRaw = (x as any).visible ?? (x as any).is_visible ?? (x as any).isVisible ?? (x as any).visibility?.visible ?? null
      const isVisible = (typeof visibleRaw === 'boolean') ? visibleRaw : ((visibleRaw == null) ? null : Boolean(visibleRaw))

      const name = pickFirstString((x as any).name, (x as any).product_name, (x as any).productName, (x as any).title)

      let categoryNameFromTree: string | null = null
      let typeNameFromTree: string | null = null
      if (categoryTreeMaps) {
        const pair = (descriptionCategoryId && typeId) ? categoryTreeMaps.byPair.get(`${descriptionCategoryId}:${typeId}`) : null
        const byType = typeId ? categoryTreeMaps.byTypeId.get(typeId) : null
        const byDesc = descriptionCategoryId ? categoryTreeMaps.byDescriptionCategoryId.get(descriptionCategoryId) : null
        const treeNames = pair ?? byType ?? byDesc ?? null
        categoryNameFromTree = treeNames?.categoryName ?? null
        typeNameFromTree = treeNames?.typeName ?? null
      }

      const skuFields = extractProductSkuFields(x)

      out.push({
        product_id: pid,
        offer_id: String(x.offer_id ?? ''),
        sku: skuFields.ozon_sku ?? (x.sku != null ? String(x.sku) : null),
        ozon_sku: skuFields.ozon_sku ?? (x.sku != null ? String(x.sku) : null),
        seller_sku: skuFields.seller_sku ?? String(x.offer_id ?? ''),
        fbo_sku: skuFields.fbo_sku,
        fbs_sku: skuFields.fbs_sku,
        barcode,
        brand: (brandRaw != null && String(brandRaw).trim().length) ? String(brandRaw).trim() : null,
        category: categoryNameFromTree ?? (categoryId != null ? String(categoryId) : null),
        type: typeNameFromTree ?? (typeId != null ? String(typeId) : (descriptionCategoryId != null ? String(descriptionCategoryId) : null)),
        name,
        photo_url: extractPhotoUrl(x),
        is_visible: isVisible,
        hidden_reasons: buildHiddenReasons(x),
        created_at: x.created_at ?? null,
      })
    }
  }

  // Атрибуты: бренд (и иногда barcode/category)
  try {
    const attrLookupItems = out.map((p) => ({ product_id: p.product_id, offer_id: p.offer_id }))
    const attrMap = await fetchAttributesMap(secrets, attrLookupItems)
    for (const p of out) {
      const a = attrMap.get(p.product_id)
      if (!a) continue
      if (a.brand) p.brand = a.brand
      if (!p.barcode && a.barcode) p.barcode = a.barcode

      const currentTypeId = toNumId(p.type)
      const descriptionCategoryId = a.descriptionCategoryId ?? null
      const typeId = a.typeId ?? currentTypeId

      if (categoryTreeMaps) {
        const pair = (descriptionCategoryId && typeId) ? categoryTreeMaps.byPair.get(`${descriptionCategoryId}:${typeId}`) : null
        const byType = typeId ? categoryTreeMaps.byTypeId.get(typeId) : null
        const byDesc = descriptionCategoryId ? categoryTreeMaps.byDescriptionCategoryId.get(descriptionCategoryId) : null
        const treeNames = pair ?? byType ?? byDesc ?? null

        if (!p.category) {
          p.category = treeNames?.categoryName ?? a.category ?? p.category
        } else if (p.category === '-' || /^\d+$/.test(String(p.category))) {
          p.category = treeNames?.categoryName ?? p.category
        }

        if (treeNames?.typeName) {
          if (!p.type || p.type === '-' || /^\d+$/.test(String(p.type))) p.type = treeNames.typeName
        } else if ((!p.type || p.type === '-') && typeId != null) {
          p.type = String(typeId)
        }
      } else {
        if (!p.category && a.category) p.category = a.category
        if ((!p.type || p.type === '-') && typeId != null) p.type = String(typeId)
      }
    }
  } catch {
    // атрибуты не критичны — если упали, оставляем базовые поля
  }

  return out
}

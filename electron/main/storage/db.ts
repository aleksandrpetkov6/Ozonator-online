import { createHash } from 'crypto'
import { existsSync, rmSync } from 'fs'
import type { ProductPlacementRow, ProductRow, StockViewRow } from '../types'
import { getDatasetSnapshotDefaultMergeStrategy, getDatasetSnapshotSchemaVersion, inferStableRowKey } from '../data-contracts'
import { ensurePersistentStorageReady, getLifecycleMarkerPath } from './paths'

const DEFAULT_LOG_RETENTION_DAYS = 30
const MAX_JSON_LEN = 20000
const MAX_API_JSON_LEN = 750000
const MAX_RAW_CACHE_ROWS = 300
const MAX_SNAPSHOT_ROWS_DEFAULT = 20000
const MAX_SNAPSHOT_ROWS_SALES = 100000
const REINSTALL_UNINSTALL_SUPPRESS_WINDOW_MS = 10 * 60 * 1000

type AppLogType =
  | 'check_auth'
  | 'sync_products'
  | 'app_install'
  | 'app_update'
  | 'app_reinstall'
  | 'app_uninstall'
  | 'admin_settings'
  | 'sales_fbo_shipment_trace'
  | string

type GridColsDataset = string

type DatasetSnapshotMergeStrategy = 'replace' | 'incremental_upsert_backfill' | 'authoritative_upsert_prune_backfill'

type GridColHiddenBucket = 'main' | 'add'

type GridColLayoutItem = {
  id: string
  w: number
  visible: boolean
  hiddenBucket: GridColHiddenBucket
}

export type ApiRawCacheResponseRow = {
  endpoint: string
  response_body: string | null
  fetched_at: string
  store_client_id?: string | null
}

export type ApiRawCacheEntryRow = {
  endpoint: string
  request_body: string | null
  response_body: string | null
  fetched_at: string
  store_client_id?: string | null
}

export type ApiRawCacheStoredRow = ApiRawCacheEntryRow & {
  request_truncated: number
  response_truncated: number
  response_body_len: number | null
}

type RawCacheRow = ApiRawCacheStoredRow & {
  id: number
  method: string
  http_status: number | null
  is_success: number
  error_message: string | null
  response_sha256: string
}

type DatasetSnapshot = {
  store_client_id: string
  dataset: string
  scope_key: string
  rows: any[]
  fetched_at: string
  merge_strategy: DatasetSnapshotMergeStrategy
}

type SyncLogRow = {
  id: number
  type: AppLogType
  status: 'pending' | 'success' | 'error' | string
  started_at: string
  finished_at: string | null
  items_count: number | null
  error_message: string | null
  error_details: string | null
  meta: string | null
  store_client_id?: string | null
}

type CbrRateDay = {
  requested_date: string
  effective_date: string | null
  is_success: number
  error_message: string | null
  fetched_at: string
}

type CbrRateRow = {
  currencyCode: string
  nominal: number
  valueRub: number
  ratePerUnit: number
}

const GRID_COLS_KEY_PREFIX = 'grid_cols_layout:'

let initialized = false
let nextRawId = 1
let nextLogId = 1

const appSettings = new Map<string, string>()
const rawCache: RawCacheRow[] = []
const datasetSnapshots = new Map<string, DatasetSnapshot>()
const productsByKey = new Map<string, ProductRow>()
const productPlacementsByKey = new Map<string, ProductPlacementRow>()
const syncLogs: SyncLogRow[] = []
const cbrRateDays = new Map<string, CbrRateDay>()
const cbrRatesDaily = new Map<string, CbrRateRow[]>()

function nowIso() {
  return new Date().toISOString()
}

function normalizeText(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function normalizeStore(value: unknown): string {
  return normalizeText(value)
}

function buildCompositeKey(parts: Array<string | number | null | undefined>): string {
  return parts.map((part) => String(part ?? '').trim()).join('::')
}

function productKey(storeClientId: unknown, offerId: unknown): string {
  return buildCompositeKey([normalizeStore(storeClientId), normalizeText(offerId)])
}

function placementKey(row: Pick<ProductPlacementRow, 'store_client_id' | 'warehouse_id' | 'sku'>): string {
  return buildCompositeKey([normalizeStore(row.store_client_id), row.warehouse_id, row.sku])
}

function snapshotKey(storeClientId: unknown, dataset: unknown, scopeKey: unknown): string {
  return buildCompositeKey([normalizeStore(storeClientId), normalizeText(dataset), normalizeText(scopeKey)])
}

function safeJson(value: any): string | null {
  if (value == null) return null
  try {
    return JSON.stringify(value).slice(0, MAX_JSON_LEN)
  } catch {
    return JSON.stringify({ unserializable: true }).slice(0, MAX_JSON_LEN)
  }
}

function safeJsonWithLimit(value: any, limit: number): { text: string | null; truncated: boolean } {
  if (value == null) return { text: null, truncated: false }
  try {
    const raw = JSON.stringify(value)
    if (raw.length <= limit) return { text: raw, truncated: false }
    return { text: raw.slice(0, limit), truncated: true }
  } catch {
    const raw = JSON.stringify({ unserializable: true })
    return { text: raw.slice(0, limit), truncated: raw.length > limit }
  }
}

function parsePositiveInt(value: unknown, fallback: number): number {
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  const i = Math.trunc(n)
  if (i <= 0) return fallback
  return i
}

function normalizeRetentionDays(value: unknown): number {
  const n = parsePositiveInt(value, DEFAULT_LOG_RETENTION_DAYS)
  return Math.min(3650, Math.max(1, n))
}

function sha256Hex(input: string): string {
  return createHash('sha256').update(input).digest('hex')
}

function mergePreferIncoming<T>(incoming: T, existing: T): T {
  const incomingText = typeof incoming === 'string' ? incoming.trim() : incoming
  if (incomingText !== null && incomingText !== undefined && incomingText !== '' && (!(Array.isArray(incomingText)) || incomingText.length > 0)) {
    return incoming as T
  }
  return existing as T
}

function mergeBooleanish(incoming: any, existing: any): number | null {
  if (typeof incoming === 'boolean') return incoming ? 1 : 0
  if (incoming === 0 || incoming === 1) return Number(incoming)
  if (typeof incoming === 'number' && Number.isFinite(incoming)) return incoming ? 1 : 0
  if (typeof incoming === 'string') {
    const raw = incoming.trim().toLowerCase()
    if (['true', '1', 'yes', 'да'].includes(raw)) return 1
    if (['false', '0', 'no', 'нет'].includes(raw)) return 0
  }
  if (existing === 0 || existing === 1) return Number(existing)
  if (typeof existing === 'boolean') return existing ? 1 : 0
  return null
}

function getDatasetSnapshotMaxRows(datasetRaw: unknown): number {
  const dataset = normalizeText(datasetRaw).toLowerCase()
  if (dataset === 'sales') return MAX_SNAPSHOT_ROWS_SALES
  return MAX_SNAPSHOT_ROWS_DEFAULT
}

function inferDatasetSnapshotMergeStrategy(dataset: string, sourceKind?: string | null): DatasetSnapshotMergeStrategy {
  const value = getDatasetSnapshotDefaultMergeStrategy(dataset, sourceKind)
  return value === 'replace' || value === 'authoritative_upsert_prune_backfill' || value === 'incremental_upsert_backfill'
    ? value
    : 'incremental_upsert_backfill'
}

function normalizeSnapshotRow(row: any): Record<string, any> | null {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return null
  return { ...row }
}

function mergeRowsByBackfill(existing: Record<string, any>, incoming: Record<string, any>): Record<string, any> {
  const out = { ...existing }
  for (const [key, value] of Object.entries(incoming)) {
    const incomingText = typeof value === 'string' ? value.trim() : value
    if (incomingText === null || incomingText === undefined || incomingText === '') continue
    out[key] = value
  }
  return out
}

function mergeDatasetSnapshotRows(args: {
  dataset: string
  strategy: DatasetSnapshotMergeStrategy
  existingRows: any[]
  incomingRows: any[]
  maxRows: number
}) {
  const incoming = (Array.isArray(args.incomingRows) ? args.incomingRows : [])
    .map(normalizeSnapshotRow)
    .filter((row): row is Record<string, any> => Boolean(row))
  const existing = (Array.isArray(args.existingRows) ? args.existingRows : [])
    .map(normalizeSnapshotRow)
    .filter((row): row is Record<string, any> => Boolean(row))

  let rows: Record<string, any>[]
  if (args.strategy === 'replace' || args.strategy === 'authoritative_upsert_prune_backfill') {
    rows = incoming
  } else {
    const byKey = new Map<string, Record<string, any>>()
    const anonymous: Record<string, any>[] = []
    for (const row of existing) {
      const key = inferStableRowKey(args.dataset, row)
      if (key) byKey.set(key, row)
      else anonymous.push(row)
    }
    for (const row of incoming) {
      const key = inferStableRowKey(args.dataset, row)
      if (!key) {
        anonymous.push(row)
        continue
      }
      const prev = byKey.get(key)
      byKey.set(key, prev ? mergeRowsByBackfill(prev, row) : row)
    }
    rows = [...byKey.values(), ...anonymous]
  }

  const cappedRowsDropped = Math.max(0, rows.length - args.maxRows)
  if (rows.length > args.maxRows) rows = rows.slice(0, args.maxRows)

  return {
    rows,
    rowsCount: rows.length,
    mergeMeta: {
      existingRowsCount: existing.length,
      incomingRowsCount: incoming.length,
      cappedRowsDropped,
    },
  }
}

export function dbGetAppSetting(key: string): string | null {
  return appSettings.get(String(key ?? '').trim()) ?? null
}

export function dbSetAppSetting(key: string, value: string) {
  const normalizedKey = String(key ?? '').trim()
  if (!normalizedKey) return
  appSettings.set(normalizedKey, String(value ?? ''))
}

export function ensureDb() {
  if (initialized) return
  ensurePersistentStorageReady()
  if (!appSettings.has('log_retention_days')) appSettings.set('log_retention_days', String(DEFAULT_LOG_RETENTION_DAYS))
  initialized = true
}

export function dbRecordApiRawResponse(args: {
  storeClientId?: string | null
  method: string
  endpoint: string
  requestBody?: any
  responseBody?: any
  httpStatus?: number | null
  isSuccess?: boolean
  errorMessage?: string | null
  fetchedAt?: string
}) {
  const method = normalizeText(args.method).toUpperCase() || 'GET'
  const endpoint = normalizeText(args.endpoint)
  if (!endpoint) return
  const now = normalizeText(args.fetchedAt) || nowIso()
  const apiJsonLimit = method === 'LOCAL' && endpoint.startsWith('/__local__/sales-cache/postings')
    ? Math.max(MAX_API_JSON_LEN, 5_000_000)
    : MAX_API_JSON_LEN
  const req = safeJsonWithLimit(args.requestBody ?? null, apiJsonLimit)
  const res = safeJsonWithLimit(args.responseBody ?? null, apiJsonLimit)
  rawCache.push({
    id: nextRawId++,
    method,
    endpoint,
    request_body: req.text,
    response_body: res.text,
    fetched_at: now,
    store_client_id: normalizeStore(args.storeClientId) || null,
    request_truncated: req.truncated ? 1 : 0,
    response_truncated: res.truncated ? 1 : 0,
    response_body_len: res.text == null ? null : res.text.length,
    http_status: typeof args.httpStatus === 'number' && Number.isFinite(args.httpStatus) ? Math.trunc(args.httpStatus) : null,
    is_success: args.isSuccess === false ? 0 : 1,
    error_message: args.errorMessage ?? null,
    response_sha256: sha256Hex(res.text ?? ''),
  })
  while (rawCache.length > MAX_RAW_CACHE_ROWS) rawCache.shift()
}

function normalizeEndpoints(endpointsRaw: unknown): string[] {
  return Array.from(new Set((Array.isArray(endpointsRaw) ? endpointsRaw : [])
    .map((v) => normalizeText(v))
    .filter(Boolean)))
}

function filterRawRows(storeClientId: string | null | undefined, endpointsRaw: unknown, options?: { latestPerEndpoint?: boolean; requireRequest?: boolean; allowTruncated?: boolean }) {
  const endpoints = normalizeEndpoints(endpointsRaw)
  if (endpoints.length === 0) return []
  const endpointSet = new Set(endpoints)
  const scopedStoreClientId = normalizeStore(storeClientId)
  const rows = rawCache
    .filter((row) => row.is_success === 1)
    .filter((row) => endpointSet.has(row.endpoint))
    .filter((row) => !scopedStoreClientId || normalizeStore(row.store_client_id) === scopedStoreClientId)
    .filter((row) => row.response_body != null)
    .filter((row) => options?.allowTruncated || row.response_truncated === 0)
    .filter((row) => !options?.requireRequest || row.request_truncated === 0)
    .sort((a, b) => b.fetched_at.localeCompare(a.fetched_at) || b.id - a.id)

  if (!options?.latestPerEndpoint) return rows
  const out: RawCacheRow[] = []
  const seen = new Set<string>()
  for (const row of rows) {
    if (seen.has(row.endpoint)) continue
    out.push(row)
    seen.add(row.endpoint)
    if (seen.size >= endpoints.length) break
  }
  return out
}

export function dbGetLatestApiRawResponses(storeClientId: string | null | undefined, endpointsRaw: unknown): ApiRawCacheResponseRow[] {
  return filterRawRows(storeClientId, endpointsRaw, { latestPerEndpoint: true }).map((row) => ({
    endpoint: row.endpoint,
    response_body: row.response_body,
    fetched_at: row.fetched_at,
    store_client_id: row.store_client_id,
  }))
}

export function dbGetApiRawResponses(storeClientId: string | null | undefined, endpointsRaw: unknown): ApiRawCacheEntryRow[] {
  return filterRawRows(storeClientId, endpointsRaw, { requireRequest: true }).map((row) => ({
    endpoint: row.endpoint,
    request_body: row.request_body,
    response_body: row.response_body,
    fetched_at: row.fetched_at,
    store_client_id: row.store_client_id,
  }))
}

export function dbGetLatestApiRawStoredResponses(storeClientId: string | null | undefined, endpointsRaw: unknown): ApiRawCacheStoredRow[] {
  return filterRawRows(storeClientId, endpointsRaw, { latestPerEndpoint: true, allowTruncated: true }).map((row) => ({
    endpoint: row.endpoint,
    request_body: row.request_body,
    response_body: row.response_body,
    fetched_at: row.fetched_at,
    store_client_id: row.store_client_id,
    request_truncated: row.request_truncated,
    response_truncated: row.response_truncated,
    response_body_len: row.response_body_len,
  }))
}

export function dbSaveDatasetSnapshot(args: {
  storeClientId?: string | null
  dataset: string
  scopeKey?: string | null
  periodFrom?: string | null
  periodTo?: string | null
  schemaVersion?: number
  sourceKind?: string
  sourceEndpoints?: string[]
  mergeStrategy?: DatasetSnapshotMergeStrategy | null
  rows: any[]
  fetchedAt?: string
}) {
  const storeClientId = normalizeStore(args.storeClientId)
  const dataset = normalizeText(args.dataset)
  if (!dataset) throw new Error('Некорректный dataset для snapshot')
  const scopeKey = normalizeText(args.scopeKey)
  const fetchedAt = normalizeText(args.fetchedAt) || nowIso()
  const sourceKind = normalizeText(args.sourceKind) || 'projection'
  const sourceEndpoints = Array.from(new Set((Array.isArray(args.sourceEndpoints) ? args.sourceEndpoints : []).map(normalizeText).filter(Boolean)))
  const maxRows = getDatasetSnapshotMaxRows(dataset)
  const mergeStrategy = (args.mergeStrategy && normalizeText(args.mergeStrategy)
    ? args.mergeStrategy
    : inferDatasetSnapshotMergeStrategy(dataset, sourceKind)) as DatasetSnapshotMergeStrategy
  const incomingRowsRequestedCount = Array.isArray(args.rows) ? args.rows.length : 0
  const key = snapshotKey(storeClientId, dataset, scopeKey)
  const existing = datasetSnapshots.get(key)
  const existingRows = existing?.rows ?? []
  const merged = mergeDatasetSnapshotRows({
    dataset,
    strategy: mergeStrategy,
    existingRows,
    incomingRows: Array.isArray(args.rows) ? args.rows : [],
    maxRows,
  })
  datasetSnapshots.set(key, {
    store_client_id: storeClientId,
    dataset,
    scope_key: scopeKey,
    rows: merged.rows,
    fetched_at: fetchedAt,
    merge_strategy: mergeStrategy,
  })
  return {
    dataset,
    scopeKey,
    maxRows,
    mergeStrategy,
    sourceKind,
    sourceEndpointsCount: sourceEndpoints.length,
    existingRowsCount: Number(merged.mergeMeta?.existingRowsCount ?? existingRows.length),
    incomingRowsRequestedCount,
    incomingRowsAcceptedCount: Number(merged.mergeMeta?.incomingRowsCount ?? incomingRowsRequestedCount),
    incomingRowsDroppedByNormalize: Math.max(0, incomingRowsRequestedCount - Number(merged.mergeMeta?.incomingRowsCount ?? incomingRowsRequestedCount)),
    storedRowsCount: merged.rowsCount,
    cappedRowsDropped: Number(merged.mergeMeta?.cappedRowsDropped ?? 0),
    schemaVersion: Number.isFinite(Number(args.schemaVersion)) ? Math.max(1, Math.trunc(Number(args.schemaVersion))) : getDatasetSnapshotSchemaVersion(dataset),
  }
}

export function dbGetDatasetSnapshotRows(args: {
  storeClientId?: string | null
  dataset: string
  scopeKey?: string | null
}): any[] | null {
  const dataset = normalizeText(args.dataset)
  if (!dataset) return null
  const scopeKey = normalizeText(args.scopeKey)
  const storeClientId = normalizeStore(args.storeClientId)
  const exact = datasetSnapshots.get(snapshotKey(storeClientId, dataset, scopeKey))
  const fallback = datasetSnapshots.get(snapshotKey('', dataset, scopeKey))
  const snapshot = exact ?? fallback
  return snapshot ? snapshot.rows.map((row) => ({ ...row })) : null
}

export function dbGetAdminSettings() {
  const raw = dbGetAppSetting('log_retention_days')
  return {
    logRetentionDays: normalizeRetentionDays(raw ?? DEFAULT_LOG_RETENTION_DAYS),
  }
}

export function dbSaveAdminSettings(input: { logRetentionDays: number }) {
  const logRetentionDays = normalizeRetentionDays(input?.logRetentionDays)
  dbSetAppSetting('log_retention_days', String(logRetentionDays))
  dbLogEvent('admin_settings', { status: 'success', meta: { logRetentionDays } })
  return { logRetentionDays }
}

export function dbGetGridColumns(datasetRaw: unknown): { dataset: GridColsDataset; cols: GridColLayoutItem[] | null } {
  const dataset = normalizeText(datasetRaw) || 'products'
  const raw = dbGetAppSetting(`${GRID_COLS_KEY_PREFIX}${dataset}`)
  if (!raw) return { dataset, cols: null }
  try {
    const parsed = JSON.parse(raw)
    return { dataset, cols: Array.isArray(parsed) ? parsed as GridColLayoutItem[] : null }
  } catch {
    return { dataset, cols: null }
  }
}

export function dbSaveGridColumns(datasetRaw: unknown, colsRaw: unknown) {
  const dataset = normalizeText(datasetRaw) || 'products'
  const cols = Array.isArray(colsRaw) ? colsRaw.map((col: any) => ({
    id: normalizeText(col?.id),
    w: Math.max(40, Math.trunc(Number(col?.w) || 120)),
    visible: col?.visible !== false,
    hiddenBucket: col?.hiddenBucket === 'add' ? 'add' : 'main',
  })).filter((col) => col.id) : []
  dbSetAppSetting(`${GRID_COLS_KEY_PREFIX}${dataset}`, JSON.stringify(cols))
  return { dataset, savedCount: cols.length }
}

export function dbPruneLogsByRetention() {
  const days = dbGetAdminSettings().logRetentionDays
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000
  for (let i = syncLogs.length - 1; i >= 0; i -= 1) {
    const startedAt = Date.parse(syncLogs[i]?.started_at ?? '')
    if (Number.isFinite(startedAt) && startedAt < cutoff) syncLogs.splice(i, 1)
  }
}

export function dbIngestLifecycleMarkers(args: { appVersion: string }) {
  const appVersion = normalizeText(args?.appVersion)
  const installerMarkerPath = getLifecycleMarkerPath('installer')
  const uninstallMarkerPath = getLifecycleMarkerPath('uninstall')
  const installerExists = existsSync(installerMarkerPath)
  const uninstallExists = existsSync(uninstallMarkerPath)
  if (!installerExists && !uninstallExists) return { ingested: 0 }

  const now = nowIso()
  const suppressUninstall = installerExists && uninstallExists
  let ingested = 0

  if (uninstallExists && !suppressUninstall) {
    dbLogEvent('app_uninstall', { startedAt: now, finishedAt: now, meta: { appVersion, storageMode: 'session' } })
    ingested += 1
  }
  if (installerExists) {
    dbLogEvent(suppressUninstall ? 'app_reinstall' : 'app_install', { startedAt: now, finishedAt: now, meta: { appVersion, storageMode: 'session', suppressWindowMs: REINSTALL_UNINSTALL_SUPPRESS_WINDOW_MS } })
    ingested += 1
  }

  try { rmSync(installerMarkerPath, { force: true }) } catch {}
  try { rmSync(uninstallMarkerPath, { force: true }) } catch {}
  return { ingested }
}

export function dbUpsertProducts(items: Array<{
  offer_id: string
  product_id?: number
  sku?: string | null
  ozon_sku?: string | null
  seller_sku?: string | null
  fbo_sku?: string | null
  fbs_sku?: string | null
  barcode?: string | null
  brand?: string | null
  category?: string | null
  type?: string | null
  name?: string | null
  photo_url?: string | null
  is_visible?: number | boolean | null
  hidden_reasons?: string | null
  created_at?: string | null
  archived?: boolean
  store_client_id?: string | null
}>) {
  const updatedAt = nowIso()
  for (const row of Array.isArray(items) ? items : []) {
    const offerId = normalizeText(row?.offer_id)
    if (!offerId) continue
    const storeClientId = normalizeStore(row?.store_client_id)
    const key = productKey(storeClientId, offerId)
    const existing = productsByKey.get(key)
    const incomingSku = normalizeText(row?.sku) || null
    const incomingOzonSku = normalizeText(row?.ozon_sku ?? row?.sku) || null
    const incomingSellerSku = normalizeText(row?.seller_sku ?? row?.offer_id) || null
    const product: ProductRow = {
      offer_id: offerId,
      product_id: Number.isFinite(Number(row?.product_id)) ? Number(row.product_id) : (existing?.product_id ?? null),
      sku: mergePreferIncoming(incomingSku, existing?.sku ?? null),
      ozon_sku: mergePreferIncoming(incomingOzonSku, existing?.ozon_sku ?? null),
      seller_sku: mergePreferIncoming(incomingSellerSku, existing?.seller_sku ?? null),
      fbo_sku: mergePreferIncoming(normalizeText(row?.fbo_sku) || null, existing?.fbo_sku ?? null),
      fbs_sku: mergePreferIncoming(normalizeText(row?.fbs_sku) || null, existing?.fbs_sku ?? null),
      barcode: mergePreferIncoming(row?.barcode ?? null, existing?.barcode ?? null),
      brand: mergePreferIncoming(row?.brand ?? null, existing?.brand ?? null),
      category: mergePreferIncoming(row?.category ?? null, existing?.category ?? null),
      type: mergePreferIncoming(row?.type ?? null, existing?.type ?? null),
      name: mergePreferIncoming(row?.name ?? null, existing?.name ?? null),
      photo_url: mergePreferIncoming(row?.photo_url ?? null, existing?.photo_url ?? null),
      is_visible: mergeBooleanish(row?.is_visible, existing?.is_visible),
      hidden_reasons: mergePreferIncoming(row?.hidden_reasons ?? null, existing?.hidden_reasons ?? null),
      created_at: mergePreferIncoming(row?.created_at ?? null, existing?.created_at ?? null),
      store_client_id: storeClientId || (existing?.store_client_id ?? null),
      archived: typeof row?.archived === 'boolean' ? (row.archived ? 1 : 0) : (existing?.archived ?? null),
      updated_at: updatedAt,
    }
    productsByKey.set(key, product)
  }
}

export function dbReplaceProductPlacementsForStore(storeClientId: string, items: Array<{
  warehouse_id: number
  warehouse_name?: string | null
  sku?: string | null
  ozon_sku?: string | null
  seller_sku?: string | null
  placement_zone?: string | null
}>): number {
  const cleanStore = normalizeStore(storeClientId)
  if (!cleanStore) return 0
  const incomingKeys = new Set<string>()
  const updatedAt = nowIso()
  for (const row of Array.isArray(items) ? items : []) {
    const ozonSku = normalizeText(row?.ozon_sku) || null
    const sellerSku = normalizeText(row?.seller_sku) || null
    const legacySku = normalizeText(row?.sku) || null
    const sku = ozonSku || sellerSku || legacySku
    const warehouseId = Number(row?.warehouse_id)
    if (!sku || !Number.isFinite(warehouseId)) continue
    const normalized: ProductPlacementRow = {
      store_client_id: cleanStore,
      warehouse_id: Math.trunc(warehouseId),
      warehouse_name: normalizeText(row?.warehouse_name) || null,
      sku,
      ozon_sku: ozonSku,
      seller_sku: sellerSku,
      placement_zone: normalizeText(row?.placement_zone) || null,
      updated_at: updatedAt,
    }
    const key = placementKey(normalized)
    incomingKeys.add(key)
    productPlacementsByKey.set(key, normalized)
  }
  for (const [key, row] of productPlacementsByKey.entries()) {
    if (normalizeStore(row.store_client_id) !== cleanStore) continue
    if (!incomingKeys.has(key)) productPlacementsByKey.delete(key)
  }
  return dbGetProductPlacements(cleanStore).length
}

export function dbGetProductPlacements(storeClientId?: string | null): ProductPlacementRow[] {
  const scopedStoreClientId = normalizeStore(storeClientId)
  return Array.from(productPlacementsByKey.values())
    .filter((row) => !scopedStoreClientId || normalizeStore(row.store_client_id) === scopedStoreClientId)
    .map((row) => ({ ...row }))
    .sort((a, b) => normalizeText(a.sku).localeCompare(normalizeText(b.sku), 'ru') || Number(a.warehouse_id ?? 0) - Number(b.warehouse_id ?? 0))
}

export function dbGetStockViewRows(storeClientId?: string | null): StockViewRow[] {
  const products = dbGetProducts(storeClientId)
  const placementRows = dbGetProductPlacements(storeClientId)
  const placementsByOzonSku = new Map<string, ProductPlacementRow[]>()
  const placementsBySellerSku = new Map<string, ProductPlacementRow[]>()
  for (const row of placementRows) {
    const storeKey = normalizeStore(row.store_client_id)
    const legacySku = normalizeText(row.sku)
    const explicitOzonSku = normalizeText(row.ozon_sku)
    const explicitSellerSku = normalizeText(row.seller_sku)
    const ozonSku = explicitOzonSku || (/^\d+$/.test(legacySku) ? legacySku : '')
    const sellerSku = explicitSellerSku || (legacySku && legacySku !== ozonSku ? legacySku : '')
    if (ozonSku) {
      const key = `${storeKey}::${ozonSku}`
      const list = placementsByOzonSku.get(key)
      if (list) list.push(row)
      else placementsByOzonSku.set(key, [row])
    }
    if (sellerSku) {
      const key = `${storeKey}::${sellerSku}`
      const list = placementsBySellerSku.get(key)
      if (list) list.push(row)
      else placementsBySellerSku.set(key, [row])
    }
  }

  const out: StockViewRow[] = []
  for (const product of products) {
    const ozonSku = normalizeText(product.sku)
    const sellerSku = normalizeText(product.offer_id)
    const storeKey = normalizeStore(product.store_client_id)
    const matched: ProductPlacementRow[] = []
    const seen = new Set<string>()
    const appendUnique = (rows: ProductPlacementRow[]) => {
      for (const row of rows) {
        const key = buildCompositeKey([row.store_client_id, row.warehouse_id, row.ozon_sku ?? row.sku, row.seller_sku, row.placement_zone])
        if (seen.has(key)) continue
        seen.add(key)
        matched.push(row)
      }
    }
    if (ozonSku) appendUnique(placementsByOzonSku.get(`${storeKey}::${ozonSku}`) ?? [])
    if (sellerSku) appendUnique(placementsBySellerSku.get(`${storeKey}::${sellerSku}`) ?? [])
    if (matched.length === 0) {
      out.push({ ...product, warehouse_id: null, warehouse_name: null, placement_zone: null })
      continue
    }
    const zoneBuckets = new Map<string, ProductPlacementRow[]>()
    for (const placement of matched) {
      const zone = normalizeText(placement.placement_zone)
      const bucket = zoneBuckets.get(zone)
      if (bucket) bucket.push(placement)
      else zoneBuckets.set(zone, [placement])
    }
    const rowsToShow = zoneBuckets.size <= 1 ? [matched[0]] : Array.from(zoneBuckets.values()).map((bucket) => bucket[0]).filter(Boolean)
    for (const placement of rowsToShow) {
      out.push({
        ...product,
        warehouse_id: placement.warehouse_id ?? null,
        warehouse_name: placement.warehouse_name ?? null,
        placement_zone: placement.placement_zone ?? null,
      })
    }
  }
  return out
}

export function dbGetProducts(storeClientId?: string | null): ProductRow[] {
  const scopedStoreClientId = normalizeStore(storeClientId)
  return Array.from(productsByKey.values())
    .filter((row) => !scopedStoreClientId || normalizeStore(row.store_client_id) === scopedStoreClientId)
    .map((row) => ({ ...row }))
    .sort((a, b) => normalizeText(a.offer_id).localeCompare(normalizeText(b.offer_id), 'ru'))
}

export function dbCountProducts(storeClientId?: string | null): number {
  return dbGetProducts(storeClientId).length
}

export function dbDeleteProductsMissingForStore(storeClientId: string, keepOfferIds: string[]) {
  const cleanStore = normalizeStore(storeClientId)
  const keep = new Set((Array.isArray(keepOfferIds) ? keepOfferIds : []).map(normalizeText).filter(Boolean))
  let deleted = 0
  for (const [key, row] of productsByKey.entries()) {
    if (normalizeStore(row.store_client_id) !== cleanStore) continue
    if (keep.has(normalizeText(row.offer_id))) continue
    productsByKey.delete(key)
    deleted += 1
  }
  return deleted
}

export function dbLogStart(type: 'check_auth' | 'sync_products', storeClientId?: string | null): number {
  const row: SyncLogRow = {
    id: nextLogId++,
    type,
    status: 'pending',
    started_at: nowIso(),
    finished_at: null,
    items_count: null,
    error_message: null,
    error_details: null,
    meta: null,
    store_client_id: normalizeStore(storeClientId) || null,
  }
  syncLogs.push(row)
  return row.id
}

export function dbLogFinish(id: number, args: {
  status: 'success' | 'error'
  itemsCount?: number
  errorMessage?: string
  errorDetails?: any
  meta?: any
  storeClientId?: string | null
}) {
  const row = syncLogs.find((entry) => entry.id === id)
  if (!row) return
  row.status = args.status
  row.finished_at = nowIso()
  row.items_count = args.itemsCount ?? null
  row.error_message = args.errorMessage ?? null
  row.error_details = safeJson(args.errorDetails)
  row.meta = safeJson(args.meta)
  row.store_client_id = normalizeStore(args.storeClientId) || (row.store_client_id ?? null)
  dbPruneLogsByRetention()
}

export function dbLogEvent(type: AppLogType, args?: {
  status?: 'success' | 'error'
  startedAt?: string
  finishedAt?: string | null
  itemsCount?: number | null
  errorMessage?: string | null
  errorDetails?: any
  meta?: any
  storeClientId?: string | null
}) {
  const startedAt = normalizeText(args?.startedAt) || nowIso()
  syncLogs.push({
    id: nextLogId++,
    type,
    status: args?.status ?? 'success',
    started_at: startedAt,
    finished_at: args?.finishedAt === null ? null : (normalizeText(args?.finishedAt) || startedAt),
    items_count: args?.itemsCount ?? null,
    error_message: args?.errorMessage ?? null,
    error_details: safeJson(args?.errorDetails),
    meta: safeJson(args?.meta),
    store_client_id: normalizeStore(args?.storeClientId) || null,
  })
  dbPruneLogsByRetention()
}

export function dbGetSyncLog(storeClientId?: string | null) {
  const scopedStoreClientId = normalizeStore(storeClientId)
  return syncLogs
    .filter((row) => !scopedStoreClientId || !row.store_client_id || normalizeStore(row.store_client_id) === scopedStoreClientId)
    .map((row) => ({ ...row }))
    .sort((a, b) => b.id - a.id)
}

export function dbClearLogs() {
  syncLogs.length = 0
}

type CbrRateDaySaveArgs = {
  requestedDate: string
  effectiveDate?: string | null
  isSuccess: boolean
  errorMessage?: string | null
  fetchedAt?: string | null
}

type CbrDailyRateSaveArgs = {
  requestedDate: string
  effectiveDate?: string | null
  rates: Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }>
}

export function dbGetMissingCbrRateDays(requestedDates: string[]): string[] {
  const dates = Array.from(new Set((Array.isArray(requestedDates) ? requestedDates : []).map(normalizeText).filter(Boolean)))
  return dates.filter((date) => !cbrRateDays.has(date))
}

export function dbSaveCbrRateDay(args: CbrRateDaySaveArgs) {
  const requestedDate = normalizeText(args.requestedDate)
  if (!requestedDate) return
  cbrRateDays.set(requestedDate, {
    requested_date: requestedDate,
    effective_date: normalizeText(args.effectiveDate) || null,
    is_success: args.isSuccess ? 1 : 0,
    error_message: normalizeText(args.errorMessage) || null,
    fetched_at: normalizeText(args.fetchedAt) || nowIso(),
  })
}

export function dbSaveCbrRates(args: CbrDailyRateSaveArgs) {
  const requestedDate = normalizeText(args.requestedDate)
  if (!requestedDate) return
  const rows = (Array.isArray(args.rates) ? args.rates : [])
    .map((entry) => ({
      currencyCode: normalizeText(entry?.currencyCode).toUpperCase(),
      nominal: Number(entry?.nominal),
      valueRub: Number(entry?.valueRub),
      ratePerUnit: Number(entry?.ratePerUnit),
    }))
    .filter((entry) => /^[A-Z]{3}$/.test(entry.currencyCode) && Number.isFinite(entry.nominal) && entry.nominal > 0 && Number.isFinite(entry.valueRub) && entry.valueRub > 0 && Number.isFinite(entry.ratePerUnit) && entry.ratePerUnit > 0)
  cbrRatesDaily.set(requestedDate, rows.map((row) => ({ ...row, nominal: Math.max(1, Math.trunc(row.nominal)) })))
}

export function dbGetCbrRatesByDate(requestedDate: string): Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }> {
  const dateKey = normalizeText(requestedDate)
  if (!dateKey) return []
  return (cbrRatesDaily.get(dateKey) ?? []).map((row) => ({ ...row }))
}

import { ozonPostingFboList, ozonPostingFbsList } from './ozon'
import { applyCbrConversionsToSalesRows } from './cbr-rates'
import { buildSalesPaidByCustomerTrace, extractPostingsFromPayload, fetchSalesEndpointPages, fetchSalesPostingDetails, getSalesPostingDetailsKey, normalizeSalesRows, resolveFboShipmentDateFromSources, translateSalesCodeValue, type SalesPeriod } from './sales-sync'
import { dbGetApiRawResponses, dbGetDatasetSnapshotRows, dbGetLatestApiRawResponses, dbGetLatestApiRawStoredResponses, dbGetProducts, dbGetStockViewRows, dbLogEvent, dbRecordApiRawResponse, dbSaveDatasetSnapshot } from './storage/db'
import { buildAndPersistFboSalesSnapshot, mergeSalesRowsWithFboLocalDb, persistFboPostingsReport, persistFboPushShipmentEvents } from './storage/fbo-sales'
import { fetchFboPostingDetailsCompat } from './fbo-detail-compat'
import { fetchSalesPostingsReportRows, type SalesPostingsReportDownloadArtifact, type SalesPostingsReportRow } from './postings-report'
import type { Secrets } from './types'
import { getDatasetSnapshotDefaultMergeStrategy, getDatasetSnapshotSchemaVersion } from './data-contracts'
import { saveCurrentPersistentArtifacts } from './storage/persistent-artifacts'

const SALES_CACHE_SNAPSHOT_ENDPOINTS = {
  fbs: '/__local__/sales-cache/fbs',
  fbo: '/__local__/sales-cache/fbo',
  details: '/__local__/sales-cache/posting-details',
  postingsReport: '/__local__/sales-cache/postings-report',
} as const

const SALES_CACHE_SNAPSHOT_KEYS = [
  SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.details,
  SALES_CACHE_SNAPSHOT_ENDPOINTS.postingsReport,
] as const

const SALES_LEGACY_ENDPOINTS = ['/v3/posting/fbs/list', '/v2/posting/fbo/list'] as const
const DEFAULT_UI_SALES_DAYS = 30
const SALES_DEFAULT_ROLLING_SCOPE_KEY = '__sales_default_30d__'
const MOSCOW_TIME_ZONE = 'Europe/Moscow'
const FBO_SHIPMENT_TRACE_LOG_TYPE = 'sales_fbo_shipment_trace' as const

const FBO_SHIPMENT_TRACE_STAGE_LABELS: Record<string, string> = {
  'api.refresh.begin': 'FBO дата отгрузки: старт API-обновления',
  'api.refresh.list.loaded': 'FBO дата отгрузки: list-данные загружены',
  'api.refresh.fast_snapshot.returned': 'Продажи: быстрый snapshot по списку отправлений готов',
  'api.refresh.details.loaded': 'FBO дата отгрузки: детали загружены',
  'api.refresh.compat.loaded': 'FBO дата отгрузки: compat-детали загружены',
  'api.refresh.report.begin': 'Продажи дата доставки: старт отчёта postings',
  'api.refresh.report.created': 'Продажи дата доставки: отчёт postings создан',
  'api.refresh.report.strategy': 'Продажи дата доставки: стратегия отчёта postings выбрана',
  'api.refresh.report.polled': 'Продажи дата доставки: статус отчёта postings получен',
  'api.refresh.report.downloaded': 'Продажи дата доставки: файл отчёта postings скачан',
  'api.refresh.report.parsed': 'Продажи дата доставки: отчёт postings распарсен',
  'api.refresh.report.partial': 'Продажи дата доставки: отчёт postings собран частично',
  'api.refresh.report.persisted': 'Продажи дата доставки: строки отчёта учтены в текущей сессии',
  'api.refresh.report.snapshot.persisted': 'Продажи дата доставки: snapshot отчёта учтён в текущей сессии',
  'api.refresh.report.empty': 'Продажи дата доставки: отчёт postings не дал дат доставки',
  'api.refresh.report.error': 'Продажи дата доставки: ошибка отчёта postings',
  'api.refresh.snapshot.persisted': 'FBO дата отгрузки: текущая сессия заполнена',
  'api.refresh.rows.built': 'Продажи дата доставки: строки продаж собраны',
  'api.refresh.origin.rows.built': 'Продажи склад / кластер отгрузки: строки продаж собраны',
  'api.refresh.status.rows.built': 'Продажи статус: строки продаж собраны',
  'api.refresh.paid_by_customer.trace': 'Оплачено покупателем: диагностика собрана',
  'api.refresh.error': 'FBO дата отгрузки: ошибка API-обновления',
  'raw-cache.rebuild.begin': 'FBO дата отгрузки: старт пересборки из raw-cache',
  'raw-cache.rebuild.snapshot.persisted': 'FBO дата отгрузки: текущая сессия заполнена из raw-cache',
  'raw-cache.rebuild.rows.built': 'Продажи дата доставки: строки продаж собраны из raw-cache',
  'raw-cache.rebuild.origin.rows.built': 'Продажи склад / кластер отгрузки: строки продаж собраны из raw-cache',
  'raw-cache.rebuild.status.rows.built': 'Продажи статус: строки продаж собраны из raw-cache',
  'raw-cache.rebuild.paid_by_customer.trace': 'Оплачено покупателем: диагностика собрана из raw-cache',
  'raw-cache.rebuild.error': 'FBO дата отгрузки: ошибка пересборки из raw-cache',
  'push.ingest.received': 'FBO дата отгрузки: push получен',
  'push.ingest.persisted': 'FBO дата отгрузки: push учтён в текущей сессии',
  'push.ingest.error': 'FBO дата отгрузки: ошибка обработки push',
  'webhook.server.status': 'FBO дата отгрузки: webhook-контур активен',
  'webhook.probe.received': 'FBO дата отгрузки: ping webhook получен',
} as const

export type LocalDatasetName = string

function padDatePart(value: number): string {
  return String(value).padStart(2, '0')
}

function getTodayDateInputForTimeZone(timeZone: string): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date())

  const year = parts.find((part) => part.type === 'year')?.value ?? ''
  const month = parts.find((part) => part.type === 'month')?.value ?? ''
  const day = parts.find((part) => part.type === 'day')?.value ?? ''
  const candidate = `${year}-${month}-${day}`
  if (/^\d{4}-\d{2}-\d{2}$/.test(candidate)) return candidate

  const fallback = new Date()
  return `${fallback.getFullYear()}-${padDatePart(fallback.getMonth() + 1)}-${padDatePart(fallback.getDate())}`
}

function dateInputToUtcDate(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null
  const [yearRaw, monthRaw, dayRaw] = value.split('-')
  const year = Number(yearRaw)
  const month = Number(monthRaw)
  const day = Number(dayRaw)
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null
  return new Date(Date.UTC(year, month - 1, day))
}

function toDateInputValue(date: Date): string {
  return `${date.getUTCFullYear()}-${padDatePart(date.getUTCMonth() + 1)}-${padDatePart(date.getUTCDate())}`
}

export function getDefaultRollingSalesPeriod(days = DEFAULT_UI_SALES_DAYS): { from: string; to: string } {
  const safeDays = Math.max(1, Math.trunc(Number(days) || DEFAULT_UI_SALES_DAYS))
  const todayRaw = getTodayDateInputForTimeZone(MOSCOW_TIME_ZONE)
  const end = dateInputToUtcDate(todayRaw) ?? new Date()
  const start = new Date(end.getTime())
  start.setUTCDate(start.getUTCDate() - safeDays)
  return {
    from: toDateInputValue(start),
    to: toDateInputValue(end),
  }
}

function isDefaultRollingSalesPeriod(period: SalesPeriod | null | undefined): boolean {
  return sameSalesPeriod(normalizeSalesPeriod(period), getDefaultRollingSalesPeriod())
}

function normalizeTextValue(value: any): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function uniqueSample(values: unknown[], limit = 10): string[] {
  const out: string[] = []
  for (const value of values) {
    const normalized = normalizeTextValue(value)
    if (!normalized || out.includes(normalized)) continue
    out.push(normalized)
    if (out.length >= limit) break
  }
  return out
}


function persistSalesPostingsCsvArtifacts(artifacts: SalesPostingsReportDownloadArtifact[]): { files: Array<{ path: string; schema: string; reportCode: string; headers: string[] }>; cleanedLegacyFilesCount: number } {
  const safeArtifacts = Array.isArray(artifacts) ? artifacts : []
  if (safeArtifacts.length === 0) return { files: [], cleanedLegacyFilesCount: 0 }

  const prepared = safeArtifacts.map((artifact) => ({
    groupPath: ['reports', 'postings'],
    slot: normalizeTextValue(artifact?.schema).toLowerCase() || 'unknown',
    suffix: 'report',
    extension: 'csv',
    content: String(artifact?.csvText ?? ''),
    headers: Array.isArray(artifact?.headerNames) ? artifact.headerNames.map((v) => normalizeTextValue(v)).filter(Boolean) : [],
    reportCode: normalizeTextValue(artifact?.reportCode),
    mergeMode: 'csv_append_missing' as const,
    identityHeaders: ['Номер отправления', 'SKU'],
    preserveOtherFiles: true,
  }))

  const saved = saveCurrentPersistentArtifacts(prepared)
  return {
    files: saved.saved.map((item, index) => ({
      path: item.path,
      schema: prepared[index].slot,
      reportCode: prepared[index].reportCode,
      headers: item.headers,
    })),
    cleanedLegacyFilesCount: saved.cleanedLegacyFilesCount,
  }
}


function pushTraceSample(target: string[], value: string, limit = 10) {
  const normalized = normalizeTextValue(value)
  if (!normalized || target.includes(normalized) || target.length >= limit) return
  target.push(normalized)
}

function countRowsByDeliveryModel(rows: any[], modelRaw: string): number {
  const model = normalizeDeliveryModelKey(modelRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => normalizeDeliveryModelKey(row?.delivery_model) === model).length
}

function countRowsByDeliveryModelWithShipmentDate(rows: any[], modelRaw: string): number {
  const model = normalizeDeliveryModelKey(modelRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_model) === model
    && Boolean(normalizeTextValue(row?.shipment_date))
  )).length
}

function countRowsByDeliveryModelWithShipmentOrigin(rows: any[], modelRaw: string): number {
  const model = normalizeDeliveryModelKey(modelRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_model) === model
    && Boolean(normalizeTextValue(row?.shipment_origin))
  )).length
}

function countRowsByDeliveryModelWithDeliveryDate(rows: any[], modelRaw: string): number {
  const model = normalizeDeliveryModelKey(modelRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_model) === model
    && Boolean(normalizeTextValue(row?.delivery_date))
  )).length
}

function countRowsByDeliveryModelWithStatus(rows: any[], modelRaw: string): number {
  const model = normalizeDeliveryModelKey(modelRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_model) === model
    && Boolean(normalizeTextValue(row?.status))
  )).length
}

function countRowsWithStatus(rows: SalesShipmentReportRow[]): number {
  return (Array.isArray(rows) ? rows : []).filter((row) => Boolean(normalizeTextValue(row?.status))).length
}

function countRowsByDeliverySchemaWithStatus(rows: SalesShipmentReportRow[], schemaRaw: string): number {
  const schema = normalizeDeliveryModelKey(schemaRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_schema) === schema
    && Boolean(normalizeTextValue(row?.status))
  )).length
}

function countRowsWithDeliveredStatus(rows: any[]): number {
  return (Array.isArray(rows) ? rows : []).filter((row) => normalizeSalesReportStatusValue(row?.status) === 'Доставлен').length
}

function countRowsByDeliveryModelWithDeliveredStatus(rows: any[], modelRaw: string): number {
  const model = normalizeDeliveryModelKey(modelRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_model) === model
    && normalizeSalesReportStatusValue(row?.status) === 'Доставлен'
  )).length
}

function countPostingDetailsByKind(postingDetailsByKey: Map<string, any>, kind: 'FBO' | 'FBS'): number {
  let count = 0
  const prefix = `${kind}|`
  for (const key of postingDetailsByKey.keys()) {
    if (String(key).startsWith(prefix)) count += 1
  }
  return count
}

function countRowsWithShipmentDate(rows: SalesShipmentReportRow[]): number {
  return (Array.isArray(rows) ? rows : []).filter((row) => Boolean(normalizeTextValue(row?.shipment_date))).length
}

function countRowsWithShipmentOrigin(rows: SalesShipmentReportRow[]): number {
  return (Array.isArray(rows) ? rows : []).filter((row) => Boolean(normalizeTextValue(row?.shipment_origin))).length
}

function countRowsWithDeliveryDate(rows: SalesShipmentReportRow[]): number {
  return (Array.isArray(rows) ? rows : []).filter((row) => Boolean(normalizeTextValue(row?.delivery_date))).length
}

function countRowsByDeliverySchema(rows: SalesShipmentReportRow[], schemaRaw: string): number {
  const schema = normalizeDeliveryModelKey(schemaRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => normalizeDeliveryModelKey(row?.delivery_schema) === schema).length
}

function countRowsByDeliverySchemaWithDeliveryDate(rows: SalesShipmentReportRow[], schemaRaw: string): number {
  const schema = normalizeDeliveryModelKey(schemaRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_schema) === schema
    && Boolean(normalizeTextValue(row?.delivery_date))
  )).length
}

function countRowsByDeliverySchemaWithShipmentOrigin(rows: SalesShipmentReportRow[], schemaRaw: string): number {
  const schema = normalizeDeliveryModelKey(schemaRaw)
  return (Array.isArray(rows) ? rows : []).filter((row) => (
    normalizeDeliveryModelKey(row?.delivery_schema) === schema
    && Boolean(normalizeTextValue(row?.shipment_origin))
  )).length
}


function getFboPostingNumbersFromPayloads(payloads: Array<{ endpoint: string; payload: any }>): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  for (const payload of payloads) {
    if (!String(payload?.endpoint ?? '').includes('/posting/fbo/')) continue
    for (const posting of extractPostingsFromPayload(payload?.payload)) {
      const postingNumber = normalizeTextValue(posting?.posting_number ?? posting?.postingNumber)
      if (!postingNumber || seen.has(postingNumber)) continue
      seen.add(postingNumber)
      out.push(postingNumber)
    }
  }
  return out
}

type FboPushShipmentEvent = {
  posting_number: string
  event_type: 'type_state_changed'
  new_state: 'posting_transferring_to_delivery'
  state: 'posting_transferring_to_delivery'
  changed_state_date: string
}

function normalizePushEventType(value: unknown): string {
  return normalizeTextValue(value).toLowerCase().replace(/[^a-z_]/g, '')
}

function normalizePushState(value: unknown): string {
  const raw = normalizeTextValue(value)
  if (!raw) return ''
  return raw
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^A-Za-z0-9]+/g, '_')
    .toLowerCase()
    .replace(/^_+|_+$/g, '')
}

function normalizePushChangedStateDate(value: unknown): string {
  const raw = normalizeTextValue(value)
  if (!raw) return ''
  const parsed = new Date(raw)
  return Number.isNaN(parsed.getTime()) ? raw : parsed.toISOString()
}

function collectPostingNumbersFromObject(source: any): string[] {
  const out: string[] = []
  const pushOne = (value: any) => {
    const postingNumber = normalizeTextValue(value)
    if (!postingNumber || out.includes(postingNumber)) return
    out.push(postingNumber)
  }

  if (!source || typeof source !== 'object') return out

  const paths = [
    'posting_number',
    'postingNumber',
    'posting.number',
    'posting.posting_number',
    'posting.postingNumber',
    'posting.number',
    'result.posting_number',
    'result.postingNumber',
    'result.posting.number',
    'data.posting_number',
    'data.postingNumber',
    'data.posting.number',
    'payload.posting_number',
    'payload.postingNumber',
    'payload.posting.number',
    'message.posting_number',
    'message.postingNumber',
    'message.posting.number',
    'event.posting_number',
    'event.postingNumber',
    'event.posting.number',
  ]
  for (const path of paths) pushOne(getByPath(source, path))

  const lists = [
    getByPath(source, 'posting_numbers'),
    getByPath(source, 'postingNumbers'),
    getByPath(source, 'postings'),
    getByPath(source, 'data.posting_numbers'),
    getByPath(source, 'data.postingNumbers'),
    getByPath(source, 'data.postings'),
    getByPath(source, 'payload.posting_numbers'),
    getByPath(source, 'payload.postingNumbers'),
    getByPath(source, 'payload.postings'),
    getByPath(source, 'message.posting_numbers'),
    getByPath(source, 'message.postingNumbers'),
    getByPath(source, 'message.postings'),
    getByPath(source, 'event.posting_numbers'),
    getByPath(source, 'event.postingNumbers'),
    getByPath(source, 'event.postings'),
  ]
  for (const list of lists) {
    if (!Array.isArray(list)) continue
    for (const item of list) {
      if (item && typeof item === 'object') {
        pushOne(getByPath(item, 'posting_number'))
        pushOne(getByPath(item, 'postingNumber'))
        pushOne(getByPath(item, 'number'))
      } else {
        pushOne(item)
      }
    }
  }

  return out
}

function collectFboShipmentPushEvents(payload: any): FboPushShipmentEvent[] {
  const out: FboPushShipmentEvent[] = []
  const seen = new Set<string>()
  const visited = new Set<any>()

  const walk = (value: any, inheritedPostingNumbers: string[]) => {
    if (!value || typeof value !== 'object') return
    if (visited.has(value)) return
    visited.add(value)

    const postingNumbers = Array.from(new Set([...inheritedPostingNumbers, ...collectPostingNumbersFromObject(value)]))
    const eventType = normalizePushEventType(pickFirstPresent(value, [
      'event_type',
      'eventType',
      'type',
      'event.event_type',
      'event.eventType',
      'event.type',
    ]))
    const nextState = normalizePushState(pickFirstPresent(value, [
      'new_state',
      'newState',
      'state',
      'status',
      'event.new_state',
      'event.newState',
      'event.state',
      'event.status',
    ]))
    const changedStateDate = normalizePushChangedStateDate(
      pickFirstPresent(value, [
        'changed_state_date',
        'changedStateDate',
        'date',
        'created_at',
        'createdAt',
        'event.changed_state_date',
        'event.changedStateDate',
        'event.date',
        'event.created_at',
        'event.createdAt',
      ]),
    )

    if ((eventType === 'type_state_changed' || eventType === 'state_changed') && nextState == 'posting_transferring_to_delivery' && changedStateDate && postingNumbers) {
      for (const postingNumber of postingNumbers) {
        const key = `${postingNumber}|${changedStateDate}`
        if (seen.has(key)) continue
        seen.add(key)
        out.push({
          posting_number: postingNumber,
          event_type: 'type_state_changed',
          new_state: 'posting_transferring_to_delivery',
          state: 'posting_transferring_to_delivery',
          changed_state_date: changedStateDate,
        })
      }
    }

    if (Array.isArray(value)) {
      for (const item of value) walk(item, postingNumbers)
      return
    }
    for (const nested of Object.values(value as Record<string, unknown>)) {
      if (!nested || typeof nested !== 'object') continue
      walk(nested, postingNumbers)
    }
  }

  walk(payload, collectPostingNumbersFromObject(payload))
  out.sort((left, right) => right.changed_state_date.localeCompare(left.changed_state_date))
  return out
}

export function logFboShipmentTrace(stage: string, args: {
  storeClientId?: string | null
  period?: SalesPeriod | null | undefined
  status?: 'success' | 'error'
  itemsCount?: number | null
  errorMessage?: string | null
  meta?: Record<string, any>
}) {
  dbLogEvent(FBO_SHIPMENT_TRACE_LOG_TYPE, {
    status: args.status ?? 'success',
    itemsCount: typeof args.itemsCount === 'number' ? args.itemsCount : null,
    errorMessage: args.errorMessage ?? null,
    storeClientId: args.storeClientId ?? null,
    meta: {
      stage,
      stageRu: FBO_SHIPMENT_TRACE_STAGE_LABELS[stage] ?? stage,
      period: normalizeSalesPeriod(args.period ?? null),
      ...(args.meta ?? {}),
    },
  })
}

function logPaidByCustomerTrace(stage: string, args: {
  storeClientId?: string | null
  period?: SalesPeriod | null | undefined
  rows?: any[]
  payloads: Array<{ endpoint: string; payload: any }>
  postingDetailsByKey: Map<string, any>
  reportRows?: SalesShipmentReportRow[]
}) {
  const trace = buildSalesPaidByCustomerTrace(args.payloads, args.postingDetailsByKey, args.rows as any, args.reportRows as any)
  logFboShipmentTrace(stage, {
    storeClientId: args.storeClientId ?? null,
    period: args.period,
    itemsCount: trace.finalRowsWithPaidByCustomer,
    meta: {
      traceKind: 'paid_by_customer',
      ...trace,
    },
  })
}

function getByPath(source: any, path: string): any {
  if (!source || typeof source !== 'object') return undefined
  let cur = source
  for (const part of String(path ?? '').split('.').filter(Boolean)) {
    if (cur == null || typeof cur !== 'object' || !(part in cur)) return undefined
    cur = cur[part]
  }
  return cur
}

function pickFirstPresent(source: any, paths: string[]): any {
  for (const path of paths) {
    const value = getByPath(source, path)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function getFboCompatShipmentDate(detail: any): string {
  return normalizeTextValue(resolveFboShipmentDateFromSources(detail))
}

function hasFboCompatDetail(detail: any): boolean {
  const cluster = normalizeTextValue(pickFirstPresent(detail, [
    'financial_data.cluster_to',
    'result.financial_data.cluster_to',
    'cluster_to',
    'result.cluster_to',
  ]))
  if (!cluster) return false

  return Boolean(getFboCompatShipmentDate(detail))
}

function collectFboPostingNumbersNeedingCompat(
  fboPayloads: Array<{ endpoint: string; payload: any }>,
  postingDetailsByKey: Map<string, any>,
): string[] {
  const out: string[] = []
  const seen = new Set<string>()

  for (const envelope of fboPayloads) {
    const postings = Array.isArray(envelope?.payload?.result?.postings)
      ? envelope.payload.result.postings
      : (Array.isArray(envelope?.payload?.postings) ? envelope.payload.postings : [])
    for (const posting of postings) {
      const postingNumber = normalizeTextValue(posting?.posting_number ?? posting?.postingNumber)
      if (!postingNumber || seen.has(postingNumber)) continue
      seen.add(postingNumber)
      const key = getSalesPostingDetailsKey('FBO', postingNumber)
      if (hasFboCompatDetail(postingDetailsByKey.get(key))) continue
      out.push(postingNumber)
    }
  }

  return out
}

function parseJsonTextSafe(text: string | null | undefined) {
  if (typeof text !== 'string' || !text.trim()) return null
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}


type SalesShipmentReportRow = Pick<SalesPostingsReportRow, 'posting_number' | 'order_number' | 'delivery_schema' | 'shipment_date' | 'shipment_origin' | 'delivery_date' | 'status' | 'sku' | 'offer_id' | 'product_name' | 'in_process_at' | 'price' | 'quantity' | 'paid_by_customer' | 'raw_row'>

function normalizeSalesShipmentReportRawRow(value: unknown): Record<string, string> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  const out: Record<string, string> = {}
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (!key) continue
    out[String(key)] = normalizeTextValue(entry)
  }
  return out
}

function normalizeSalesShipmentReportNumber(value: unknown): number | '' {
  return typeof value === 'number' && Number.isFinite(value) ? value : ''
}

function inspectPersistedPostingsReportSnapshot(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const latestStored = dbGetLatestApiRawStoredResponses(storeClientId ?? null, [SALES_CACHE_SNAPSHOT_ENDPOINTS.postingsReport])[0] ?? null
  const latest = dbGetLatestApiRawResponses(storeClientId ?? null, [SALES_CACHE_SNAPSHOT_ENDPOINTS.postingsReport])[0] ?? null
  const payload = parseJsonTextSafe((latest?.response_body ?? latestStored?.response_body ?? null)) as any
  const rows = Array.isArray(payload?.rows) ? payload.rows : []
  const normalizedRows = rows
    .map((row: any): SalesShipmentReportRow => ({
      posting_number: normalizeTextValue(row?.posting_number),
      order_number: normalizeTextValue(row?.order_number),
      delivery_schema: normalizeTextValue(row?.delivery_schema),
      shipment_date: normalizeTextValue(row?.shipment_date),
      shipment_origin: normalizeTextValue(row?.shipment_origin),
      delivery_date: normalizeTextValue(row?.delivery_date),
      status: normalizeTextValue(row?.status),
      sku: normalizeTextValue(row?.sku),
      offer_id: normalizeTextValue(row?.offer_id),
      product_name: normalizeTextValue(row?.product_name),
      in_process_at: normalizeTextValue((row as any)?.in_process_at),
      price: normalizeSalesShipmentReportNumber(row?.price),
      quantity: normalizeSalesShipmentReportNumber(row?.quantity),
      paid_by_customer: normalizeSalesShipmentReportNumber(row?.paid_by_customer),
      raw_row: normalizeSalesShipmentReportRawRow(row?.raw_row),
    }))
    .filter((row: SalesShipmentReportRow) => Boolean(row.posting_number))

  const snapshotPeriod = normalizeSalesPeriod(payload?.period ?? null)
  const expectedPeriod = normalizeSalesPeriod(requestedPeriod)
  return {
    found: Boolean(latestStored),
    fetchedAt: normalizeTextValue(latestStored?.fetched_at ?? latest?.fetched_at),
    rowsCount: normalizedRows.length,
    periodMatches: sameSalesPeriod(snapshotPeriod, expectedPeriod),
    rowsWithDeliveryDate: countRowsWithDeliveryDate(normalizedRows),
    rowsWithShipmentOrigin: countRowsWithShipmentOrigin(normalizedRows),
    rowsWithStatus: countRowsWithStatus(normalizedRows),
    rowsFboWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(normalizedRows, 'fbo'),
    rowsFbsWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(normalizedRows, 'fbs'),
    responseTruncated: Number(latestStored?.response_truncated ?? 0),
    responseBodyLen: latestStored?.response_body_len ?? null,
    csvHeaderCount: Array.isArray(payload?.csvHeaderNames) ? payload.csvHeaderNames.length : 0,
    csvHeaderNames: Array.isArray(payload?.csvHeaderNames) ? payload.csvHeaderNames.map((value: any) => normalizeTextValue(value)).filter(Boolean) : [],
    savedCsvFilesCount: Array.isArray(payload?.savedCsvFiles) ? payload.savedCsvFiles.length : 0,
    savedCsvPaths: Array.isArray(payload?.savedCsvFiles) ? payload.savedCsvFiles.map((item: any) => normalizeTextValue(item?.path)).filter(Boolean) : [],
  }
}

function normalizeDeliveryModelKey(value: unknown): string {
  const raw = normalizeTextValue(value).toLowerCase().replace(/[^a-z]/g, '')
  if (!raw) return ''
  if (raw.includes('rfbs')) return 'rfbs'
  if (raw.includes('fbo')) return 'fbo'
  if (raw.includes('fbs')) return 'fbs'
  return ''
}

function normalizeSalesReportStatusValue(value: unknown): string {
  const raw = normalizeTextValue(value)
  if (!raw) return ''
  return translateSalesCodeValue(raw, 'status') || raw
}

function buildSalesPostingsReportMaps(rows: SalesShipmentReportRow[]): { shipmentDateByKey: Map<string, string>; shipmentOriginByKey: Map<string, string>; deliveryDateByKey: Map<string, string>; statusByKey: Map<string, string> } {
  const shipmentDateByKey = new Map<string, string>()
  const shipmentOriginByKey = new Map<string, string>()
  const deliveryDateByKey = new Map<string, string>()
  const statusByKey = new Map<string, string>()

  const save = (target: Map<string, string>, key: string, value: string) => {
    if (!key || !value) return
    const prev = normalizeTextValue(target.get(key))
    if (!prev || value > prev) target.set(key, value)
  }

  for (const row of rows) {
    const postingNumber = normalizeTextValue(row?.posting_number)
    if (!postingNumber) continue

    const shipmentDate = normalizeTextValue(row?.shipment_date)
    const shipmentOrigin = normalizeTextValue(row?.shipment_origin)
    const deliveryDate = normalizeTextValue(row?.delivery_date)
    const status = normalizeSalesReportStatusValue(row?.status)
    const modelKey = normalizeDeliveryModelKey(row?.delivery_schema)
    const keys = [`*|${postingNumber}`]
    if (modelKey) keys.push(`${modelKey}|${postingNumber}`)

    for (const key of keys) {
      save(shipmentDateByKey, key, shipmentDate)
      if (shipmentOrigin && !normalizeTextValue(shipmentOriginByKey.get(key))) shipmentOriginByKey.set(key, shipmentOrigin)
      save(deliveryDateByKey, key, deliveryDate)
      if (status && !normalizeTextValue(statusByKey.get(key))) statusByKey.set(key, status)
    }
  }

  return { shipmentDateByKey, shipmentOriginByKey, deliveryDateByKey, statusByKey }
}

function buildSalesShipmentReportRowsFromSnapshotMap(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
): SalesShipmentReportRow[] {
  const snapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.postingsReport)
  if (!snapshot || typeof snapshot !== 'object') return []

  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const snapshotPeriod = normalizeSalesPeriod(snapshot?.period ?? null)
  const useSnapshot = sameSalesPeriod(snapshotPeriod, normalizedRequestedPeriod)
    || (!normalizedRequestedPeriod.from && !normalizedRequestedPeriod.to && Boolean(snapshot?.rows))
  if (!useSnapshot) return []

  const rows: any[] = Array.isArray(snapshot?.rows) ? snapshot.rows : []
  return rows
    .map((row: any): SalesShipmentReportRow => ({
      posting_number: normalizeTextValue(row?.posting_number),
      order_number: normalizeTextValue(row?.order_number),
      delivery_schema: normalizeTextValue(row?.delivery_schema),
      shipment_date: normalizeTextValue(row?.shipment_date),
      shipment_origin: normalizeTextValue(row?.shipment_origin),
      delivery_date: normalizeTextValue(row?.delivery_date),
      status: normalizeTextValue(row?.status),
      sku: normalizeTextValue(row?.sku),
      offer_id: normalizeTextValue(row?.offer_id),
      product_name: normalizeTextValue(row?.product_name),
      in_process_at: normalizeTextValue((row as any)?.in_process_at),
      price: normalizeSalesShipmentReportNumber(row?.price),
      quantity: normalizeSalesShipmentReportNumber(row?.quantity),
      paid_by_customer: normalizeSalesShipmentReportNumber(row?.paid_by_customer),
      raw_row: normalizeSalesShipmentReportRawRow(row?.raw_row),
    }))
    .filter((row: SalesShipmentReportRow) => Boolean(row.posting_number))
}

function applySalesShipmentReportDates(rows: any[], reportRows: SalesShipmentReportRow[]): {
  rows: any[]
  trace: Record<string, any>
} {
  const safeRows = Array.isArray(rows) ? rows : []
  const safeReportRows = Array.isArray(reportRows) ? reportRows : []
  const baseTrace = {
    reportRowsCount: safeReportRows.length,
    reportRowsWithDeliveryDate: countRowsWithDeliveryDate(safeReportRows),
    reportRowsWithShipmentOrigin: countRowsWithShipmentOrigin(safeReportRows),
    reportRowsWithStatus: countRowsWithStatus(safeReportRows),
    reportRowsFboWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(safeReportRows, 'fbo'),
    reportRowsFboWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(safeReportRows, 'fbo'),
    reportRowsFboWithStatus: countRowsByDeliverySchemaWithStatus(safeReportRows, 'fbo'),
    reportRowsFbsWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(safeReportRows, 'fbs'),
    reportRowsFbsWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(safeReportRows, 'fbs'),
    reportRowsFbsWithStatus: countRowsByDeliverySchemaWithStatus(safeReportRows, 'fbs'),
    reportDeliveryDateKeyCount: 0,
    reportShipmentOriginKeyCount: 0,
    reportStatusKeyCount: 0,
    deliveryDateMatchedRows: 0,
    deliveryDateResolvedRows: 0,
    deliveryDateClearedRows: 0,
    shipmentOriginMatchedRows: 0,
    shipmentOriginResolvedRows: 0,
    shipmentOriginClearedRows: 0,
    statusMatchedRows: 0,
    statusResolvedRows: 0,
    statusClearedRows: 0,
    deliveredRowsWithClearedDetails: 0,
    finalRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(safeRows, 'fbo') + countRowsByDeliveryModelWithDeliveryDate(safeRows, 'fbs') + countRowsByDeliveryModelWithDeliveryDate(safeRows, 'rfbs'),
    finalRowsWithoutDeliveryDate: safeRows.length - (countRowsByDeliveryModelWithDeliveryDate(safeRows, 'fbo') + countRowsByDeliveryModelWithDeliveryDate(safeRows, 'fbs') + countRowsByDeliveryModelWithDeliveryDate(safeRows, 'rfbs')),
    finalRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'fbo') + countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'fbs') + countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'rfbs'),
    finalRowsWithoutShipmentOrigin: safeRows.length - (countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'fbo') + countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'fbs') + countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'rfbs')),
    finalRowsWithStatus: countRowsByDeliveryModelWithStatus(safeRows, 'fbo') + countRowsByDeliveryModelWithStatus(safeRows, 'fbs') + countRowsByDeliveryModelWithStatus(safeRows, 'rfbs'),
    finalRowsWithoutStatus: safeRows.length - (countRowsByDeliveryModelWithStatus(safeRows, 'fbo') + countRowsByDeliveryModelWithStatus(safeRows, 'fbs') + countRowsByDeliveryModelWithStatus(safeRows, 'rfbs')),
    finalDeliveredRows: countRowsWithDeliveredStatus(safeRows),
    fboRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(safeRows, 'fbo'),
    fbsRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(safeRows, 'fbs'),
    rfbsRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(safeRows, 'rfbs'),
    fboRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'fbo'),
    fbsRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'fbs'),
    rfbsRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(safeRows, 'rfbs'),
    fboRowsWithStatus: countRowsByDeliveryModelWithStatus(safeRows, 'fbo'),
    fbsRowsWithStatus: countRowsByDeliveryModelWithStatus(safeRows, 'fbs'),
    rfbsRowsWithStatus: countRowsByDeliveryModelWithStatus(safeRows, 'rfbs'),
    fboRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(safeRows, 'fbo'),
    fbsRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(safeRows, 'fbs'),
    rfbsRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(safeRows, 'rfbs'),
    missingDeliveryDatePostingNumbers: [] as string[],
    missingShipmentOriginPostingNumbers: [] as string[],
    missingStatusPostingNumbers: [] as string[],
    reportDeliveryDateSample: uniqueSample(safeReportRows.filter((row) => normalizeTextValue(row?.delivery_date)).map((row) => row.delivery_date), 10),
    reportShipmentOriginSample: uniqueSample(safeReportRows.filter((row) => normalizeTextValue(row?.shipment_origin)).map((row) => row.shipment_origin), 10),
    reportStatusSample: uniqueSample(safeReportRows.filter((row) => normalizeTextValue(row?.status)).map((row) => normalizeSalesReportStatusValue(row?.status)), 10),
  }
  if (safeRows.length === 0) return { rows: safeRows, trace: baseTrace }

  const { shipmentDateByKey, shipmentOriginByKey, deliveryDateByKey, statusByKey } = buildSalesPostingsReportMaps(safeReportRows)

  const missingDeliveryDatePostingNumbers: string[] = []
  const missingShipmentOriginPostingNumbers: string[] = []
  const missingStatusPostingNumbers: string[] = []
  let deliveryDateMatchedRows = 0
  let deliveryDateResolvedRows = 0
  let deliveryDateClearedRows = 0
  let shipmentOriginMatchedRows = 0
  let shipmentOriginResolvedRows = 0
  let shipmentOriginClearedRows = 0
  let statusMatchedRows = 0
  let statusResolvedRows = 0
  let statusClearedRows = 0
  let deliveredRowsWithClearedDetails = 0

  const nextRows = safeRows.map((row) => {
    const postingNumber = normalizeTextValue(row?.posting_number)
    if (!postingNumber) return row

    const modelKey = normalizeDeliveryModelKey(row?.delivery_model)
    const reportShipmentDate = normalizeTextValue(
      (modelKey ? shipmentDateByKey.get(`${modelKey}|${postingNumber}`) : '')
      || shipmentDateByKey.get(`*|${postingNumber}`),
    )
    const reportDeliveryDate = normalizeTextValue(
      (modelKey ? deliveryDateByKey.get(`${modelKey}|${postingNumber}`) : '')
      || deliveryDateByKey.get(`*|${postingNumber}`),
    )
    const reportShipmentOrigin = normalizeTextValue(
      (modelKey ? shipmentOriginByKey.get(`${modelKey}|${postingNumber}`) : '')
      || shipmentOriginByKey.get(`*|${postingNumber}`),
    )
    const reportStatus = normalizeTextValue(
      (modelKey ? statusByKey.get(`${modelKey}|${postingNumber}`) : '')
      || statusByKey.get(`*|${postingNumber}`),
    )

    let changed = false
    const nextRow = { ...row }

    if (modelKey !== 'fbo' && !normalizeTextValue(row?.shipment_date) && reportShipmentDate) {
      nextRow.shipment_date = reportShipmentDate
      changed = true
    }

    const hasDeliveryDateKey = deliveryDateByKey.has(`*|${postingNumber}`) || Boolean(modelKey && deliveryDateByKey.has(`${modelKey}|${postingNumber}`))
    if (hasDeliveryDateKey) {
      deliveryDateMatchedRows += 1
    }

    const prevDeliveryDate = normalizeTextValue(row?.delivery_date)
    const nextDeliveryDate = hasDeliveryDateKey ? (reportDeliveryDate || '') : (safeReportRows.length > 0 ? '' : prevDeliveryDate)
    if (prevDeliveryDate !== nextDeliveryDate) {
      nextRow.delivery_date = nextDeliveryDate
      changed = true
      if (!nextDeliveryDate && prevDeliveryDate) deliveryDateClearedRows += 1
    }
    if (nextDeliveryDate) deliveryDateResolvedRows += 1
    else pushTraceSample(missingDeliveryDatePostingNumbers, postingNumber, 10)

    const hasShipmentOriginKey = shipmentOriginByKey.has(`*|${postingNumber}`) || Boolean(modelKey && shipmentOriginByKey.has(`${modelKey}|${postingNumber}`))
    if (hasShipmentOriginKey) {
      shipmentOriginMatchedRows += 1
    }

    const prevShipmentOrigin = normalizeTextValue(row?.shipment_origin)
    const nextShipmentOrigin = hasShipmentOriginKey ? (reportShipmentOrigin || '') : prevShipmentOrigin
    if (prevShipmentOrigin !== nextShipmentOrigin) {
      nextRow.shipment_origin = nextShipmentOrigin
      changed = true
      if (!nextShipmentOrigin && prevShipmentOrigin) shipmentOriginClearedRows += 1
    }
    if (nextShipmentOrigin) shipmentOriginResolvedRows += 1
    else pushTraceSample(missingShipmentOriginPostingNumbers, postingNumber, 10)

    const hasStatusKey = statusByKey.has(`*|${postingNumber}`) || Boolean(modelKey && statusByKey.has(`${modelKey}|${postingNumber}`))
    if (hasStatusKey) {
      statusMatchedRows += 1
    }

    const prevStatus = normalizeTextValue(row?.status)
    const nextStatus = hasStatusKey ? (reportStatus || '') : ''
    if (prevStatus !== nextStatus) {
      nextRow.status = nextStatus
      changed = true
      if (!nextStatus && prevStatus) statusClearedRows += 1
    }
    if (nextStatus) statusResolvedRows += 1
    else pushTraceSample(missingStatusPostingNumbers, postingNumber, 10)

    if (nextStatus === 'Доставлен') {
      const prevStatusDetails = normalizeTextValue(nextRow?.status_details)
      const prevCarrierStatusDetails = normalizeTextValue(nextRow?.carrier_status_details)
      if (prevStatusDetails || prevCarrierStatusDetails) {
        nextRow.status_details = ''
        nextRow.carrier_status_details = ''
        changed = true
        deliveredRowsWithClearedDetails += 1
      }
    }

    return changed ? nextRow : row
  })

  return {
    rows: nextRows,
    trace: {
      ...baseTrace,
      reportDeliveryDateKeyCount: deliveryDateByKey.size,
      reportShipmentOriginKeyCount: shipmentOriginByKey.size,
      reportStatusKeyCount: statusByKey.size,
      deliveryDateMatchedRows,
      deliveryDateResolvedRows,
      deliveryDateClearedRows,
      shipmentOriginMatchedRows,
      shipmentOriginResolvedRows,
      shipmentOriginClearedRows,
      statusMatchedRows,
      statusResolvedRows,
      statusClearedRows,
      deliveredRowsWithClearedDetails,
      finalRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(nextRows, 'fbo') + countRowsByDeliveryModelWithDeliveryDate(nextRows, 'fbs') + countRowsByDeliveryModelWithDeliveryDate(nextRows, 'rfbs'),
      finalRowsWithoutDeliveryDate: nextRows.length - (countRowsByDeliveryModelWithDeliveryDate(nextRows, 'fbo') + countRowsByDeliveryModelWithDeliveryDate(nextRows, 'fbs') + countRowsByDeliveryModelWithDeliveryDate(nextRows, 'rfbs')),
      finalRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'fbo') + countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'fbs') + countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'rfbs'),
      finalRowsWithoutShipmentOrigin: nextRows.length - (countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'fbo') + countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'fbs') + countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'rfbs')),
      finalRowsWithStatus: countRowsByDeliveryModelWithStatus(nextRows, 'fbo') + countRowsByDeliveryModelWithStatus(nextRows, 'fbs') + countRowsByDeliveryModelWithStatus(nextRows, 'rfbs'),
      finalRowsWithoutStatus: nextRows.length - (countRowsByDeliveryModelWithStatus(nextRows, 'fbo') + countRowsByDeliveryModelWithStatus(nextRows, 'fbs') + countRowsByDeliveryModelWithStatus(nextRows, 'rfbs')),
      finalDeliveredRows: countRowsWithDeliveredStatus(nextRows),
      fboRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(nextRows, 'fbo'),
      fbsRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(nextRows, 'fbs'),
      rfbsRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(nextRows, 'rfbs'),
      fboRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'fbo'),
      fbsRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'fbs'),
      rfbsRowsWithShipmentOrigin: countRowsByDeliveryModelWithShipmentOrigin(nextRows, 'rfbs'),
      fboRowsWithStatus: countRowsByDeliveryModelWithStatus(nextRows, 'fbo'),
      fbsRowsWithStatus: countRowsByDeliveryModelWithStatus(nextRows, 'fbs'),
      rfbsRowsWithStatus: countRowsByDeliveryModelWithStatus(nextRows, 'rfbs'),
      fboRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(nextRows, 'fbo'),
      fbsRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(nextRows, 'fbs'),
      rfbsRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(nextRows, 'rfbs'),
      missingDeliveryDatePostingNumbers,
      missingShipmentOriginPostingNumbers,
      missingStatusPostingNumbers,
      reportDeliveryDateSample: uniqueSample(safeReportRows.filter((row) => normalizeTextValue(row?.delivery_date)).map((row) => row.delivery_date), 10),
      reportShipmentOriginSample: uniqueSample(safeReportRows.filter((row) => normalizeTextValue(row?.shipment_origin)).map((row) => row.shipment_origin), 10),
      reportStatusSample: uniqueSample(safeReportRows.filter((row) => normalizeTextValue(row?.status)).map((row) => normalizeSalesReportStatusValue(row?.status)), 10),
    },
  }
}


function normalizeSalesPeriodValue(value: any): string | null {
  const raw = typeof value === 'string' ? value.trim() : ''
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null
}

function normalizeSalesPeriod(period: SalesPeriod | null | undefined): { from: string | null; to: string | null } {
  let from = normalizeSalesPeriodValue(period?.from)
  let to = normalizeSalesPeriodValue(period?.to)
  if (!from && to) from = to
  if (from && !to) to = from
  if (from && to && from > to) [from, to] = [to, from]
  return { from, to }
}

function sameSalesPeriod(
  left: { from?: string | null; to?: string | null } | null | undefined,
  right: { from?: string | null; to?: string | null } | null | undefined,
) {
  return (left?.from ?? null) === (right?.from ?? null) && (left?.to ?? null) === (right?.to ?? null)
}

type SalesRowsDateSpan = { from: string; to: string; count: number }

function buildSalesDateSpan(values: unknown[]): SalesRowsDateSpan {
  let from = ''
  let to = ''
  let count = 0
  for (const value of values) {
    const day = extractSalesRowPeriodDay(value)
    if (!day) continue
    count += 1
    if (!from || day < from) from = day
    if (!to || day > to) to = day
  }
  return { from, to, count }
}

function buildRowsDateSpan(rows: any[], fields: string[]): SalesRowsDateSpan {
  const values: unknown[] = []
  for (const row of Array.isArray(rows) ? rows : []) {
    values.push(pickFirstPresent(row, fields))
  }
  return buildSalesDateSpan(values)
}

function formatDateSpanLabel(from: string, to: string): string {
  if (from && to) return from === to ? from : `${from}..${to}`
  return from || to || ''
}

function extractSalesRowPeriodDay(value: unknown): string {
  const raw = typeof value === 'string' ? value.trim() : ''
  if (!raw) return ''

  const isoHead = raw.slice(0, 10)
  if (/^\d{4}-\d{2}-\d{2}$/.test(isoHead)) return isoHead

  const ruMatch = raw.match(/^(\d{2})\.(\d{2})\.(\d{2}|\d{4})/)
  if (ruMatch) {
    const day = ruMatch[1]
    const month = ruMatch[2]
    const yearRaw = ruMatch[3]
    const year = yearRaw.length === 2 ? `20${yearRaw}` : yearRaw
    return `${year}-${month}-${day}`
  }

  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) return ''
  return parsed.toISOString().slice(0, 10)
}

function filterSalesRowsStrictByPeriod(
  rows: any[],
  requestedPeriod: SalesPeriod | null | undefined,
): any[] {
  const normalized = normalizeSalesPeriod(requestedPeriod)
  let from = normalized.from
  let to = normalized.to

  if (!from && !to) return Array.isArray(rows) ? rows : []
  if (!from && to) from = to
  if (from && !to) to = from
  if (!from || !to) return Array.isArray(rows) ? rows : []
  if (from > to) [from, to] = [to, from]

  return (Array.isArray(rows) ? rows : []).filter((row) => {
    const day = extractSalesRowPeriodDay(row?.in_process_at)
      || extractSalesRowPeriodDay(row?.accepted_at)
      || extractSalesRowPeriodDay(row?.delivery_date)
      || extractSalesRowPeriodDay(row?.shipment_date)
    return Boolean(day && day >= from! && day <= to!)
  })
}

function buildSalesSnapshotTraceMeta(args: {
  rows: any[]
  scopeKey: string
  sourceKind: string
  saveResult?: any
  requestedPeriod?: SalesPeriod | null | undefined
}) {
  const span = buildRowsDateSpan(Array.isArray(args.rows) ? args.rows : [], ['in_process_at', 'accepted_at', 'delivery_date', 'shipment_date'])
  const droppedByNormalize = Number(args.saveResult?.incomingRowsDroppedByNormalize ?? 0)
  const droppedByCap = Number(args.saveResult?.cappedRowsDropped ?? 0)
  return {
    requestedPeriodFrom: normalizeSalesPeriod(args.requestedPeriod).from,
    requestedPeriodTo: normalizeSalesPeriod(args.requestedPeriod).to,
    snapshotScopeKey: args.scopeKey,
    snapshotSourceKind: args.sourceKind,
    snapshotRowsRequested: Array.isArray(args.rows) ? args.rows.length : 0,
    snapshotRowsStored: Number(args.saveResult?.storedRowsCount ?? 0),
    snapshotRowsDroppedByLimit: droppedByNormalize + droppedByCap,
    snapshotRowsDroppedByNormalize: droppedByNormalize,
    snapshotRowsDroppedByCap: droppedByCap,
    snapshotMaxRows: Number(args.saveResult?.maxRows ?? 0),
    snapshotMergeStrategy: normalizeTextValue(args.saveResult?.mergeStrategy),
    snapshotRowsSpan: formatDateSpanLabel(span.from, span.to),
  }
}

function buildSalesReadTraceMeta(args: {
  rows: any[]
  scopeKey: string
  sourceKind: string
  requestedPeriod?: SalesPeriod | null | undefined
}) {
  const span = buildRowsDateSpan(Array.isArray(args.rows) ? args.rows : [], ['in_process_at', 'accepted_at', 'delivery_date', 'shipment_date'])
  return {
    requestedPeriodFrom: normalizeSalesPeriod(args.requestedPeriod).from,
    requestedPeriodTo: normalizeSalesPeriod(args.requestedPeriod).to,
    snapshotScopeKey: args.scopeKey,
    snapshotSourceKind: args.sourceKind,
    readRowsCount: Array.isArray(args.rows) ? args.rows.length : 0,
    readRowsSpan: formatDateSpanLabel(span.from, span.to),
  }
}

function getSalesSnapshotMap(storeClientId: string | null | undefined) {
  const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_CACHE_SNAPSHOT_KEYS as unknown as string[])
  const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_CACHE_SNAPSHOT_KEYS as unknown as string[])
  const out = new Map<string, any>()
  for (const row of rows) {
    if (out.has(row.endpoint)) continue
    const parsed = parseJsonTextSafe(row?.response_body)
    if (parsed) out.set(row.endpoint, parsed)
  }
  return out
}

function getLegacySalesPayloadMap(storeClientId: string | null | undefined) {
  const scoped = dbGetLatestApiRawResponses(storeClientId ?? null, SALES_LEGACY_ENDPOINTS as unknown as string[])
  const rows = scoped.length > 0 ? scoped : dbGetLatestApiRawResponses(null, SALES_LEGACY_ENDPOINTS as unknown as string[])
  const out = new Map<string, any>()
  for (const row of rows) {
    if (out.has(row.endpoint)) continue
    const parsed = parseJsonTextSafe(row?.response_body)
    if (parsed) out.set(row.endpoint, parsed)
  }
  return out
}

const SALES_POSTING_DETAIL_ENDPOINTS = ['/v3/posting/fbs/get', '/v2/posting/fbo/get'] as const

function extractSalesPostingDetailResult(payload: any): any {
  if (payload?.result && typeof payload.result === 'object') return payload.result
  return payload && typeof payload === 'object' ? payload : null
}

function extractSalesPostingNumberFromRawRequestBody(raw: any): string {
  return normalizeTextValue(raw?.posting_number ?? raw?.postingNumber)
}

function getSalesPostingDetailsFromRawCache(storeClientId: string | null | undefined) {
  const scoped = dbGetApiRawResponses(storeClientId ?? null, SALES_POSTING_DETAIL_ENDPOINTS as unknown as string[])
  const rows = scoped.length > 0 ? scoped : dbGetApiRawResponses(null, SALES_POSTING_DETAIL_ENDPOINTS as unknown as string[])
  const out = new Map<string, any>()

  for (const row of rows) {
    const endpoint = String(row?.endpoint ?? '').trim()
    const endpointKind = endpoint.includes('/posting/fbs/') ? 'FBS' : (endpoint.includes('/posting/fbo/') ? 'FBO' : '')
    if (endpointKind !== 'FBS' && endpointKind !== 'FBO') continue

    const requestBody = parseJsonTextSafe(row?.request_body)
    const responseBody = parseJsonTextSafe(row?.response_body)
    const detail = extractSalesPostingDetailResult(responseBody)
    if (!detail) continue

    const postingNumber = extractSalesPostingNumberFromRawRequestBody(requestBody)
      || normalizeTextValue(detail?.posting_number ?? detail?.postingNumber)
    if (!postingNumber) continue

    const key = getSalesPostingDetailsKey(endpointKind, postingNumber)
    if (out.has(key)) continue
    out.set(key, detail)
  }

  return out
}

function buildSalesPayloadsFromSnapshotMap(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const out: Array<{ endpoint: string; payload: any }> = []

  const pushSnapshot = (snapshot: any, fallbackEndpoint: string, allowAnyPeriod = false) => {
    if (!snapshot || typeof snapshot !== 'object') return false
    const snapshotPeriod = normalizeSalesPeriod(snapshot?.period ?? null)
    if (!allowAnyPeriod && !sameSalesPeriod(snapshotPeriod, normalizedRequestedPeriod)) return false
    const payloads = Array.isArray(snapshot?.payloads) ? snapshot.payloads : []
    const sourceEndpoint = String(snapshot?.sourceEndpoint ?? '').trim() || fallbackEndpoint
    for (const payload of payloads) out.push({ endpoint: sourceEndpoint, payload })
    return payloads.length > 0
  }

  const fbsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs)
  const fboSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo)
  const hasExact = [
    pushSnapshot(fbsSnapshot, '/v3/posting/fbs/list'),
    pushSnapshot(fboSnapshot, '/v2/posting/fbo/list'),
  ].some(Boolean)

  if (!hasExact) {
    pushSnapshot(fbsSnapshot, '/v3/posting/fbs/list', true)
    pushSnapshot(fboSnapshot, '/v2/posting/fbo/list', true)
  }

  return out
}

function getSalesRawCoverageFromSnapshotMap(cacheByEndpoint: Map<string, any>) {
  let hasPayloads = false
  let from: string | null = null
  let to: string | null = null

  for (const endpoint of [SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs, SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo]) {
    const snapshot = cacheByEndpoint.get(endpoint)
    if (!snapshot || typeof snapshot !== 'object') continue

    const payloads = Array.isArray(snapshot?.payloads) ? snapshot.payloads : []
    if (payloads.length > 0) hasPayloads = true

    const period = normalizeSalesPeriod(snapshot?.period ?? null)
    const periodFrom = period.from
    const periodTo = period.to
    if (periodFrom && (!from || periodFrom < from)) from = periodFrom
    if (periodTo && (!to || periodTo > to)) to = periodTo
  }

  return { hasPayloads, from, to }
}

function isRequestedSalesPeriodCoveredByRawCache(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const requested = normalizeSalesPeriod(requestedPeriod)
  const coverage = getSalesRawCoverageFromSnapshotMap(cacheByEndpoint)

  if (!requested.from && !requested.to) return coverage.hasPayloads
  if (!coverage.hasPayloads) return false

  let from = requested.from
  let to = requested.to
  if (!from && to) from = to
  if (from && !to) to = from
  if (!from || !to) return false
  if (from > to) [from, to] = [to, from]

  if (!coverage.from || !coverage.to) return false
  return from >= coverage.from && to <= coverage.to
}

function buildSalesPostingDetailsFromSnapshotMap(
  cacheByEndpoint: Map<string, any>,
  requestedPeriod: SalesPeriod | null | undefined,
): Map<string, any> {
  const detailsSnapshot = cacheByEndpoint.get(SALES_CACHE_SNAPSHOT_ENDPOINTS.details)
  if (!detailsSnapshot || typeof detailsSnapshot !== 'object') return new Map<string, any>()
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)
  const detailsPeriod = normalizeSalesPeriod(detailsSnapshot?.period ?? null)
  const useSnapshot = sameSalesPeriod(detailsPeriod, normalizedRequestedPeriod) || Boolean(detailsSnapshot?.items)
  if (!useSnapshot) return new Map<string, any>()

  const out = new Map<string, any>()
  const items = Array.isArray(detailsSnapshot?.items) ? detailsSnapshot.items : []
  for (const item of items) {
    const key = String(item?.key ?? '').trim()
    if (!key) continue
    out.set(key, item?.payload ?? null)
  }
  return out
}

function salesRelatedPostingPrefix(value: unknown): string {
  const postingNumber = String(value ?? '').trim()
  if (!postingNumber) return ''
  const firstDash = postingNumber.indexOf('-')
  if (firstDash < 0) return ''
  const secondDash = postingNumber.indexOf('-', firstDash + 1)
  if (secondDash < 0) return ''
  return postingNumber.slice(0, secondDash).trim()
}

function applySalesRelatedPostingPrefix(rows: any[]): any[] {
  if (!Array.isArray(rows) || rows.length === 0) return Array.isArray(rows) ? rows : []

  const prefixCounts = new Map<string, number>()
  for (const row of rows) {
    const prefix = salesRelatedPostingPrefix((row as any)?.posting_number)
    if (!prefix) continue
    prefixCounts.set(prefix, (prefixCounts.get(prefix) ?? 0) + 1)
  }

  return rows.map((row) => {
    const prefix = salesRelatedPostingPrefix((row as any)?.posting_number)
    if (!prefix) {
      return { ...row, related_postings: '' }
    }
    return {
      ...row,
      related_postings: (prefixCounts.get(prefix) ?? 0) > 1 ? prefix : '',
    }
  })
}

type SalesDeliveryDateTrace = Record<string, any>

type BuildSalesRowsResult = {
  rows: any[]
  sourceEndpoints: string[]
  deliveryDateTrace: SalesDeliveryDateTrace
  statusTrace: SalesDeliveryDateTrace
}

function buildSalesRowsFromPayloads(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
  payloads: Array<{ endpoint: string; payload: any }>,
  postingDetailsByKey: Map<string, any>,
  reportRows: SalesShipmentReportRow[] = [],
): BuildSalesRowsResult {
  const products = dbGetProducts(storeClientId ?? null)
  const sourceEndpoints = new Set<string>()

  for (const payload of payloads) {
    const endpoint = String(payload?.endpoint ?? '').trim()
    if (endpoint) sourceEndpoints.add(endpoint)
  }

  const rows = normalizeSalesRows(payloads, products, postingDetailsByKey, reportRows as any)
  const mergedRows = mergeSalesRowsWithFboLocalDb({
    rows,
    storeClientId: storeClientId ?? null,
    periodKey: buildDatasetScopeKey(requestedPeriod),
  })
  const reportApplied = applySalesShipmentReportDates(mergedRows, reportRows)
  const strictRows = filterSalesRowsStrictByPeriod(reportApplied.rows, requestedPeriod)
  const normalizedRows = applySalesRelatedPostingPrefix(strictRows)
  const preFilterSpan = buildRowsDateSpan(reportApplied.rows, ['in_process_at', 'accepted_at', 'delivery_date', 'shipment_date'])
  const postFilterSpan = buildRowsDateSpan(normalizedRows, ['in_process_at', 'accepted_at', 'delivery_date', 'shipment_date'])
  const traceBase = {
    ...((reportApplied.trace ?? {}) as SalesDeliveryDateTrace),
    requestedPeriodFrom: normalizeSalesPeriod(requestedPeriod).from,
    requestedPeriodTo: normalizeSalesPeriod(requestedPeriod).to,
    salesRowsBeforeStrictFilter: Array.isArray(reportApplied.rows) ? reportApplied.rows.length : 0,
    salesRowsBeforeStrictFilterSpan: formatDateSpanLabel(preFilterSpan.from, preFilterSpan.to),
    salesRowsAfterStrictFilter: normalizedRows.length,
    salesRowsAfterStrictFilterSpan: formatDateSpanLabel(postFilterSpan.from, postFilterSpan.to),
  }
  return {
    rows: normalizedRows,
    sourceEndpoints: Array.from(sourceEndpoints),
    deliveryDateTrace: {
      ...traceBase,
      finalRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'fbo') + countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'fbs') + countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'rfbs'),
      finalRowsWithoutDeliveryDate: normalizedRows.length - (countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'fbo') + countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'fbs') + countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'rfbs')),
      fboRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'fbo'),
      fbsRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'fbs'),
      rfbsRowsWithDeliveryDate: countRowsByDeliveryModelWithDeliveryDate(normalizedRows, 'rfbs'),
    },
    statusTrace: {
      ...traceBase,
      finalRowsWithStatus: countRowsByDeliveryModelWithStatus(normalizedRows, 'fbo') + countRowsByDeliveryModelWithStatus(normalizedRows, 'fbs') + countRowsByDeliveryModelWithStatus(normalizedRows, 'rfbs'),
      finalRowsWithoutStatus: normalizedRows.length - (countRowsByDeliveryModelWithStatus(normalizedRows, 'fbo') + countRowsByDeliveryModelWithStatus(normalizedRows, 'fbs') + countRowsByDeliveryModelWithStatus(normalizedRows, 'rfbs')),
      finalDeliveredRows: countRowsWithDeliveredStatus(normalizedRows),
      fboRowsWithStatus: countRowsByDeliveryModelWithStatus(normalizedRows, 'fbo'),
      fbsRowsWithStatus: countRowsByDeliveryModelWithStatus(normalizedRows, 'fbs'),
      rfbsRowsWithStatus: countRowsByDeliveryModelWithStatus(normalizedRows, 'rfbs'),
      fboRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(normalizedRows, 'fbo'),
      fbsRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(normalizedRows, 'fbs'),
      rfbsRowsWithDeliveredStatus: countRowsByDeliveryModelWithDeliveredStatus(normalizedRows, 'rfbs'),
    },
  }
}

function persistFboLocalSnapshotFromRawCache(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
  payloads: Array<{ endpoint: string; payload: any }>,
  postingDetailsByKey: Map<string, any>,
  reportRows: SalesShipmentReportRow[] = [],
) {
  const normalizedStoreClientId = normalizeTextValue(storeClientId)
  if (!normalizedStoreClientId) return null

  const fboPayloads = payloads.filter((payload) => String(payload?.endpoint ?? '').includes('/posting/fbo/'))
  if (fboPayloads.length === 0) return null

  if (reportRows.length > 0) {
    persistFboPostingsReport({
      storeClientId: normalizedStoreClientId,
      periodKey: buildDatasetScopeKey(requestedPeriod),
      rows: reportRows,
      fetchedAt: new Date().toISOString(),
    })
  }

  return buildAndPersistFboSalesSnapshot({
    storeClientId: normalizedStoreClientId,
    periodKey: buildDatasetScopeKey(requestedPeriod),
    fboPayloads,
    postingDetailsByKey,
    reportRows,
    fetchedAt: new Date().toISOString(),
  })
}

function buildSalesRowsFromLocalRawCache(storeClientId: string | null | undefined, requestedPeriod: SalesPeriod | null | undefined) {
  try {
    const cacheByEndpoint = getSalesSnapshotMap(storeClientId)
    const payloads = buildSalesPayloadsFromSnapshotMap(cacheByEndpoint, requestedPeriod)
    const snapshotPostingDetailsByKey = buildSalesPostingDetailsFromSnapshotMap(cacheByEndpoint, requestedPeriod)
    const rawCachePostingDetailsByKey = getSalesPostingDetailsFromRawCache(storeClientId)
    const postingDetailsByKey = new Map<string, any>(rawCachePostingDetailsByKey)
    for (const [key, payload] of snapshotPostingDetailsByKey.entries()) {
      if (postingDetailsByKey.has(key)) continue
      postingDetailsByKey.set(key, payload)
    }
    const reportRows = buildSalesShipmentReportRowsFromSnapshotMap(cacheByEndpoint, requestedPeriod)

    if (payloads.length === 0 && cacheByEndpoint.size === 0) {
      const legacyPayloads = getLegacySalesPayloadMap(storeClientId)
      for (const [endpoint, payload] of legacyPayloads.entries()) {
        payloads.push({ endpoint, payload })
      }
    }

    const fboPostingNumbers = getFboPostingNumbersFromPayloads(payloads)
    logFboShipmentTrace('raw-cache.rebuild.begin', {
      storeClientId,
      period: requestedPeriod,
      itemsCount: fboPostingNumbers.length,
      meta: {
        snapshotEndpointCount: cacheByEndpoint.size,
        payloadCount: payloads.length,
        fboPostingCount: fboPostingNumbers.length,
        rawCacheFboDetailCount: countPostingDetailsByKind(rawCachePostingDetailsByKey, 'FBO'),
        snapshotFboDetailCount: countPostingDetailsByKind(snapshotPostingDetailsByKey, 'FBO'),
        mergedFboDetailCount: countPostingDetailsByKind(postingDetailsByKey, 'FBO'),
        reportRowsCount: reportRows.length,
        reportFboShipmentDateImportedToLocalDb: reportRows.length > 0,
        samplePostingNumbers: uniqueSample(fboPostingNumbers, 10),
      },
    })

    const persistResult = persistFboLocalSnapshotFromRawCache(storeClientId, requestedPeriod, payloads, postingDetailsByKey, reportRows)
    if (persistResult) {
      logFboShipmentTrace('raw-cache.rebuild.snapshot.persisted', {
        storeClientId,
        period: requestedPeriod,
        itemsCount: Number(persistResult?.persisted?.shipmentDateCount ?? persistResult?.trace?.postingsWithResolvedShipmentDate ?? 0),
        meta: {
          ...persistResult,
        },
      })
    }

    const result = buildSalesRowsFromPayloads(storeClientId, requestedPeriod, payloads, postingDetailsByKey, reportRows)
    const deliveryDateTrace = (result as any)?.deliveryDateTrace ?? null
    const statusTrace = (result as any)?.statusTrace ?? null
    logFboShipmentTrace('raw-cache.rebuild.rows.built', {
      storeClientId,
      period: requestedPeriod,
      itemsCount: countRowsByDeliveryModelWithShipmentDate(result.rows, 'FBO'),
      meta: {
        salesRowsCount: result.rows.length,
        fboRowsCount: countRowsByDeliveryModel(result.rows, 'FBO'),
        fboRowsWithShipmentDate: countRowsByDeliveryModelWithShipmentDate(result.rows, 'FBO'),
        salesRowsWithDeliveryDate: Number(deliveryDateTrace?.finalRowsWithDeliveryDate ?? 0),
        salesRowsWithoutDeliveryDate: Number(deliveryDateTrace?.finalRowsWithoutDeliveryDate ?? 0),
        fboRowsWithDeliveryDate: Number(deliveryDateTrace?.fboRowsWithDeliveryDate ?? 0),
        fbsRowsWithDeliveryDate: Number(deliveryDateTrace?.fbsRowsWithDeliveryDate ?? 0),
        rfbsRowsWithDeliveryDate: Number(deliveryDateTrace?.rfbsRowsWithDeliveryDate ?? 0),
        deliveryDateMatchedRows: Number(deliveryDateTrace?.deliveryDateMatchedRows ?? 0),
        deliveryDateResolvedRows: Number(deliveryDateTrace?.deliveryDateResolvedRows ?? 0),
        deliveryDateClearedRows: Number(deliveryDateTrace?.deliveryDateClearedRows ?? 0),
        missingDeliveryDatePostingNumbers: Array.isArray(deliveryDateTrace?.missingDeliveryDatePostingNumbers) ? deliveryDateTrace.missingDeliveryDatePostingNumbers : [],
        reportDeliveryDateKeyCount: Number(deliveryDateTrace?.reportDeliveryDateKeyCount ?? 0),
        reportRowsWithDeliveryDate: Number(deliveryDateTrace?.reportRowsWithDeliveryDate ?? 0),
        reportRowsFboWithDeliveryDate: Number(deliveryDateTrace?.reportRowsFboWithDeliveryDate ?? 0),
        reportRowsFbsWithDeliveryDate: Number(deliveryDateTrace?.reportRowsFbsWithDeliveryDate ?? 0),
        reportDeliveryDateSample: Array.isArray(deliveryDateTrace?.reportDeliveryDateSample) ? deliveryDateTrace.reportDeliveryDateSample : [],
        sourceEndpoints: result.sourceEndpoints,
      },
    })
    logFboShipmentTrace('raw-cache.rebuild.origin.rows.built', {
      storeClientId,
      period: requestedPeriod,
      itemsCount: Number(deliveryDateTrace?.finalRowsWithShipmentOrigin ?? 0),
      meta: {
        salesRowsCount: result.rows.length,
        salesRowsWithShipmentOrigin: Number(deliveryDateTrace?.finalRowsWithShipmentOrigin ?? 0),
        salesRowsWithoutShipmentOrigin: Number(deliveryDateTrace?.finalRowsWithoutShipmentOrigin ?? 0),
        fboRowsWithShipmentOrigin: Number(deliveryDateTrace?.fboRowsWithShipmentOrigin ?? 0),
        fbsRowsWithShipmentOrigin: Number(deliveryDateTrace?.fbsRowsWithShipmentOrigin ?? 0),
        rfbsRowsWithShipmentOrigin: Number(deliveryDateTrace?.rfbsRowsWithShipmentOrigin ?? 0),
        shipmentOriginMatchedRows: Number(deliveryDateTrace?.shipmentOriginMatchedRows ?? 0),
        shipmentOriginResolvedRows: Number(deliveryDateTrace?.shipmentOriginResolvedRows ?? 0),
        shipmentOriginClearedRows: Number(deliveryDateTrace?.shipmentOriginClearedRows ?? 0),
        missingShipmentOriginPostingNumbers: Array.isArray(deliveryDateTrace?.missingShipmentOriginPostingNumbers) ? deliveryDateTrace.missingShipmentOriginPostingNumbers : [],
        reportShipmentOriginKeyCount: Number(deliveryDateTrace?.reportShipmentOriginKeyCount ?? 0),
        reportRowsWithShipmentOrigin: Number(deliveryDateTrace?.reportRowsWithShipmentOrigin ?? 0),
        reportRowsFboWithShipmentOrigin: Number(deliveryDateTrace?.reportRowsFboWithShipmentOrigin ?? 0),
        reportRowsFbsWithShipmentOrigin: Number(deliveryDateTrace?.reportRowsFbsWithShipmentOrigin ?? 0),
        reportShipmentOriginSample: Array.isArray(deliveryDateTrace?.reportShipmentOriginSample) ? deliveryDateTrace.reportShipmentOriginSample : [],
        sourceEndpoints: result.sourceEndpoints,
      },
    })
    logFboShipmentTrace('raw-cache.rebuild.status.rows.built', {
      storeClientId,
      period: requestedPeriod,
      itemsCount: Number(statusTrace?.finalRowsWithStatus ?? 0),
      meta: {
        salesRowsCount: result.rows.length,
        salesRowsWithStatus: Number(statusTrace?.finalRowsWithStatus ?? 0),
        salesRowsWithoutStatus: Number(statusTrace?.finalRowsWithoutStatus ?? 0),
        finalDeliveredRows: Number(statusTrace?.finalDeliveredRows ?? 0),
        fboRowsWithStatus: Number(statusTrace?.fboRowsWithStatus ?? 0),
        fbsRowsWithStatus: Number(statusTrace?.fbsRowsWithStatus ?? 0),
        rfbsRowsWithStatus: Number(statusTrace?.rfbsRowsWithStatus ?? 0),
        fboRowsWithDeliveredStatus: Number(statusTrace?.fboRowsWithDeliveredStatus ?? 0),
        fbsRowsWithDeliveredStatus: Number(statusTrace?.fbsRowsWithDeliveredStatus ?? 0),
        rfbsRowsWithDeliveredStatus: Number(statusTrace?.rfbsRowsWithDeliveredStatus ?? 0),
        statusMatchedRows: Number(statusTrace?.statusMatchedRows ?? 0),
        statusResolvedRows: Number(statusTrace?.statusResolvedRows ?? 0),
        statusClearedRows: Number(statusTrace?.statusClearedRows ?? 0),
        deliveredRowsWithClearedDetails: Number(statusTrace?.deliveredRowsWithClearedDetails ?? 0),
        missingStatusPostingNumbers: Array.isArray(statusTrace?.missingStatusPostingNumbers) ? statusTrace.missingStatusPostingNumbers : [],
        reportStatusKeyCount: Number(statusTrace?.reportStatusKeyCount ?? 0),
        reportRowsWithStatus: Number(statusTrace?.reportRowsWithStatus ?? 0),
        reportRowsFboWithStatus: Number(statusTrace?.reportRowsFboWithStatus ?? 0),
        reportRowsFbsWithStatus: Number(statusTrace?.reportRowsFbsWithStatus ?? 0),
        reportStatusSample: Array.isArray(statusTrace?.reportStatusSample) ? statusTrace.reportStatusSample : [],
        sourceEndpoints: result.sourceEndpoints,
      },
    })
    logPaidByCustomerTrace('raw-cache.rebuild.paid_by_customer.trace', {
      storeClientId,
      period: requestedPeriod,
      rows: result.rows,
      payloads,
      postingDetailsByKey,
      reportRows,
    })

    return result
  } catch (e: any) {
    logFboShipmentTrace('raw-cache.rebuild.error', {
      storeClientId,
      period: requestedPeriod,
      status: 'error',
      errorMessage: e?.message ?? String(e),
      meta: {
        stack: e?.stack ?? null,
      },
    })
    throw e
  }
}

function readScopedSalesSnapshotRows(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
): any[] | null {
  const rows = dbGetDatasetSnapshotRows({
    storeClientId: storeClientId ?? null,
    dataset: 'sales',
    scopeKey: buildDatasetScopeKey(requestedPeriod),
  })

  if (!Array.isArray(rows)) return null
  const mergedRows = mergeSalesRowsWithFboLocalDb({
    rows,
    storeClientId: storeClientId ?? null,
    periodKey: buildDatasetScopeKey(requestedPeriod),
  })
  const filteredRows = filterSalesRowsStrictByPeriod(mergedRows, requestedPeriod)
  logFboShipmentTrace('sales.read.snapshot', {
    storeClientId: storeClientId ?? null,
    period: requestedPeriod,
    itemsCount: filteredRows.length,
    meta: buildSalesReadTraceMeta({
      rows: filteredRows,
      scopeKey: buildDatasetScopeKey(requestedPeriod),
      sourceKind: 'dataset-snapshot-exact-scope',
      requestedPeriod,
    }),
  })
  return filteredRows
}

function readRollingSalesSnapshotRows(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
): any[] | null {
  const rows = dbGetDatasetSnapshotRows({
    storeClientId: storeClientId ?? null,
    dataset: 'sales',
    scopeKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
  })

  if (!Array.isArray(rows)) return null
  const mergedRows = mergeSalesRowsWithFboLocalDb({
    rows,
    storeClientId: storeClientId ?? null,
    periodKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
  })
  const filteredRows = filterSalesRowsStrictByPeriod(mergedRows, requestedPeriod)
  logFboShipmentTrace('sales.read.default_snapshot', {
    storeClientId: storeClientId ?? null,
    period: requestedPeriod,
    itemsCount: filteredRows.length,
    meta: buildSalesReadTraceMeta({
      rows: filteredRows,
      scopeKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
      sourceKind: 'dataset-snapshot-default-window',
      requestedPeriod,
    }),
  })
  return filteredRows
}

function buildDatasetScopeKey(requestedPeriod: SalesPeriod | null | undefined): string {

  const normalized = normalizeSalesPeriod(requestedPeriod)
  if (!normalized.from && !normalized.to) return ''
  return `${normalized.from ?? ''}|${normalized.to ?? ''}`
}

function translateSalesDatasetErrorMessage(messageRaw: unknown): string {
  const message = String(messageRaw ?? '').trim()
  if (/HTTP\s*400/.test(message)) return 'Ozon не принял часть дополнительной догрузки продаж за выбранный период.'
  if (/HTTP\s*429/.test(message)) return 'Ozon временно ограничил частоту запросов при обновлении продаж.'
  if (/timeout/i.test(message)) return 'Ozon не успел ответить при дополнительной догрузке продаж.'
  return 'Во время дополнительной догрузки продаж возникла неполадка.'
}

function persistDatasetSnapshot(args: {
  storeClientId?: string | null
  dataset: string
  rows: any[]
  scopeKey?: string | null
  period?: SalesPeriod | null
  sourceKind?: string
  sourceEndpoints?: string[]
}) {
  const period = normalizeSalesPeriod(args.period ?? null)
  const dataset = String(args.dataset ?? '').trim()
  const sourceKind = args.sourceKind ?? 'projection'

  return dbSaveDatasetSnapshot({
    storeClientId: args.storeClientId ?? null,
    dataset: args.dataset,
    scopeKey: args.scopeKey ?? '',
    periodFrom: period.from,
    periodTo: period.to,
    schemaVersion: getDatasetSnapshotSchemaVersion(dataset),
    mergeStrategy: getDatasetSnapshotDefaultMergeStrategy(dataset, sourceKind) as any,
    sourceKind,
    sourceEndpoints: args.sourceEndpoints ?? [],
    rows: Array.isArray(args.rows) ? args.rows : [],
  })
}


function trimSalesListPayloadForSessionSnapshot(payload: any) {
  const postings = extractPostingsFromPayload(payload)
  if (Array.isArray(postings) && postings.length > 0) return { result: { postings } }
  return payload
}

function shouldUseFastSalesListFirstRefresh(): boolean {
  return true
}

function persistFastSalesSessionSnapshot(args: {
  storeClientId: string
  requestedPeriod: SalesPeriod | null | undefined
  fbsPayloads: Array<{ endpoint: string; payload: any }>
  fboPayloads: Array<{ endpoint: string; payload: any }>
}) {
  const normalizedRequestedPeriod = normalizeSalesPeriod(args.requestedPeriod)
  const payloads = [...args.fbsPayloads, ...args.fboPayloads]
  const fetchedAt = new Date().toISOString()
  const { rows, sourceEndpoints, deliveryDateTrace, statusTrace } = buildSalesRowsFromPayloads(
    args.storeClientId,
    args.requestedPeriod,
    payloads,
    new Map<string, any>(),
    [],
  )

  const persistRawSnapshot = (endpoint: string, responseBody: any) => {
    dbRecordApiRawResponse({
      storeClientId: args.storeClientId,
      method: 'LOCAL',
      endpoint,
      requestBody: {
        mode: 'sales-cache-snapshot-fast-list',
        period: normalizedRequestedPeriod,
      },
      responseBody,
      httpStatus: 200,
      isSuccess: true,
      fetchedAt,
    })
  }

  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs, {
    sourceEndpoint: '/v3/posting/fbs/list',
    period: normalizedRequestedPeriod,
    payloads: args.fbsPayloads.map((item) => trimSalesListPayloadForSessionSnapshot(item.payload)),
  })
  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo, {
    sourceEndpoint: '/v2/posting/fbo/list',
    period: normalizedRequestedPeriod,
    payloads: args.fboPayloads.map((item) => trimSalesListPayloadForSessionSnapshot(item.payload)),
  })
  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.details, {
    period: normalizedRequestedPeriod,
    items: [],
    skippedReason: 'fast-list-first-refresh',
  })
  persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.postingsReport, {
    period: normalizedRequestedPeriod,
    rows: [],
    skippedReason: 'fast-list-first-refresh',
  })

  const persistedSalesSnapshot = persistDatasetSnapshot({
    storeClientId: args.storeClientId,
    dataset: 'sales',
    scopeKey: buildDatasetScopeKey(args.requestedPeriod),
    period: args.requestedPeriod,
    rows,
    sourceKind: 'api-live-list-fast',
    sourceEndpoints,
  })

  if (isDefaultRollingSalesPeriod(args.requestedPeriod)) {
    persistDatasetSnapshot({
      storeClientId: args.storeClientId,
      dataset: 'sales',
      scopeKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
      period: args.requestedPeriod,
      rows,
      sourceKind: 'api-live-list-fast-default-window',
      sourceEndpoints,
    })
  }

  logFboShipmentTrace('sales.snapshot.saved', {
    storeClientId: args.storeClientId,
    period: args.requestedPeriod,
    itemsCount: Number(persistedSalesSnapshot?.storedRowsCount ?? rows.length),
    meta: buildSalesSnapshotTraceMeta({
      rows,
      scopeKey: buildDatasetScopeKey(args.requestedPeriod),
      sourceKind: 'api-live-list-fast',
      saveResult: persistedSalesSnapshot,
      requestedPeriod: args.requestedPeriod,
      salesRowsWithDeliveryDate: Number(deliveryDateTrace?.finalRowsWithDeliveryDate ?? 0),
      salesRowsWithStatus: Number(statusTrace?.finalRowsWithStatus ?? 0),
    }),
  })

  return {
    rowsCount: rows.length,
    rows,
    sourceEndpoints,
  }
}


export function refreshCoreLocalDatasetSnapshots(storeClientId: string | null | undefined) {
  const productsRows = dbGetProducts(storeClientId ?? null)
  const stocksRows = dbGetStockViewRows(storeClientId ?? null)

  persistDatasetSnapshot({ storeClientId, dataset: 'products', rows: productsRows, sourceKind: 'db-table' })
  persistDatasetSnapshot({ storeClientId, dataset: 'returns', rows: productsRows, sourceKind: 'derived-products' })
  persistDatasetSnapshot({ storeClientId, dataset: 'forecast-demand', rows: productsRows, sourceKind: 'derived-products' })
  persistDatasetSnapshot({ storeClientId, dataset: 'stocks', rows: stocksRows, sourceKind: 'db-view' })

  return {
    productsRowsCount: productsRows.length,
    stocksRowsCount: stocksRows.length,
  }
}


export async function ingestOzonFboPushPayload(args: {
  storeClientId: string
  payload: any
  pathname?: string | null
  remoteAddress?: string | null
}) {
  const storeClientId = normalizeTextValue(args.storeClientId)
  try {
    const fetchedAt = new Date().toISOString()
    const pushEvents = collectFboShipmentPushEvents(args.payload)
    const samplePostingNumbers = uniqueSample(pushEvents.map((event) => event.posting_number), 10)

    dbRecordApiRawResponse({
      storeClientId: storeClientId || null,
      method: 'PUSH',
      endpoint: '/__incoming__/ozon/fbo-state',
      requestBody: {
        pathname: normalizeTextValue(args.pathname),
        remoteAddress: normalizeTextValue(args.remoteAddress),
        acceptedEventsCount: pushEvents.length,
        samplePostingNumbers,
      },
      responseBody: args.payload,
      httpStatus: 202,
      isSuccess: true,
      fetchedAt,
    })

    logFboShipmentTrace('push.ingest.received', {
      storeClientId,
      itemsCount: pushEvents.length,
      meta: {
        incomingEventsCount: pushEvents.length,
        samplePostingNumbers,
        payloadTopLevelKeys: args.payload && typeof args.payload === 'object' ? Object.keys(args.payload).slice(0, 20) : [],
      },
    })

    const persisted = persistFboPushShipmentEvents({
      storeClientId,
      events: pushEvents,
      fetchedAt,
    })

    logFboShipmentTrace('push.ingest.persisted', {
      storeClientId,
      itemsCount: Number(persisted?.acceptedEventsCount ?? pushEvents.length),
      meta: {
        incomingEventsCount: pushEvents.length,
        acceptedPushEventCount: Number(persisted?.acceptedEventsCount ?? 0),
        persisted: {
          shipmentTransferEventCount: Number(persisted?.shipmentTransferEventCount ?? 0),
          shipmentDateCount: Number(persisted?.shipmentDateCount ?? 0),
        },
        samplePostingNumbers: Array.isArray(persisted?.samplePostingNumbers) ? persisted.samplePostingNumbers : samplePostingNumbers,
      },
    })

    return {
      ok: true,
      acceptedEventsCount: Number(persisted?.acceptedEventsCount ?? 0),
      shipmentTransferEventCount: Number(persisted?.shipmentTransferEventCount ?? 0),
      shipmentDateCount: Number(persisted?.shipmentDateCount ?? 0),
      samplePostingNumbers: Array.isArray(persisted?.samplePostingNumbers) ? persisted.samplePostingNumbers : samplePostingNumbers,
    }
  } catch (e: any) {
    logFboShipmentTrace('push.ingest.error', {
      storeClientId,
      status: 'error',
      errorMessage: e?.message ?? String(e),
      meta: {
        stack: e?.stack ?? null,
      },
    })
    throw e
  }
}

export async function refreshSalesRawSnapshotFromApi(
  secrets: Secrets,
  requestedPeriod: SalesPeriod | null | undefined,
) {
  const normalizedRequestedPeriod = normalizeSalesPeriod(requestedPeriod)

  try {
    logFboShipmentTrace('api.refresh.begin', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      meta: {
        requestedPeriod: normalizedRequestedPeriod,
      },
    })

    const fbsPayloads = await fetchSalesEndpointPages(
      (body) => ozonPostingFbsList(secrets, body),
      requestedPeriod,
      '/v3/posting/fbs/list',
    )
    const fboPayloads = await fetchSalesEndpointPages(
      (body) => ozonPostingFboList(secrets, body),
      requestedPeriod,
      '/v2/posting/fbo/list',
    )
    const payloads = [...fbsPayloads, ...fboPayloads]
    const fboPostingNumbers = getFboPostingNumbersFromPayloads(fboPayloads)
    const fbsAcceptedSpan = buildRowsDateSpan(fbsPayloads.flatMap((payload) => extractPostingsFromPayload(payload?.payload)), ['in_process_at', 'created_at', 'acceptance_date'])
    const fboAcceptedSpan = buildRowsDateSpan(fboPayloads.flatMap((payload) => extractPostingsFromPayload(payload?.payload)), ['in_process_at', 'created_at', 'acceptance_date'])

    logFboShipmentTrace('api.refresh.list.loaded', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: fboPostingNumbers.length,
      meta: {
        fbsPayloadCount: fbsPayloads.length,
        fboPayloadCount: fboPayloads.length,
        payloadCount: payloads.length,
        fboPostingCount: fboPostingNumbers.length,
        samplePostingNumbers: uniqueSample(fboPostingNumbers, 10),
        requestedPeriodFrom: normalizedRequestedPeriod.from,
        requestedPeriodTo: normalizedRequestedPeriod.to,
        fbsAcceptedAtSpan: formatDateSpanLabel(fbsAcceptedSpan.from, fbsAcceptedSpan.to),
        fboAcceptedAtSpan: formatDateSpanLabel(fboAcceptedSpan.from, fboAcceptedSpan.to),
      },
    })

    const fastSnapshot = persistFastSalesSessionSnapshot({
      storeClientId: secrets.clientId,
      requestedPeriod,
      fbsPayloads,
      fboPayloads,
    })

    logFboShipmentTrace('api.refresh.fast_snapshot.returned', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: fastSnapshot.rowsCount,
      meta: {
        mode: 'list-first',
        reason: 'Sales tab must stop waiting after posting list load; heavy detail/report enrichment is skipped in no-local-db build.',
        sourceEndpoints: fastSnapshot.sourceEndpoints,
        requestedPeriodFrom: normalizedRequestedPeriod.from,
        requestedPeriodTo: normalizedRequestedPeriod.to,
      },
    })

    if (shouldUseFastSalesListFirstRefresh()) {
      return { rowsCount: fastSnapshot.rowsCount }
    }

    const cachedPostingDetailsByKey = getSalesPostingDetailsFromRawCache(secrets.clientId)
    const postingDetailsByKey = payloads.length > 0
      ? await fetchSalesPostingDetails(secrets, payloads, cachedPostingDetailsByKey)
      : new Map<string, any>(cachedPostingDetailsByKey)

    logFboShipmentTrace('api.refresh.details.loaded', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: countPostingDetailsByKind(postingDetailsByKey, 'FBO'),
      meta: {
        cachedFboDetailCount: countPostingDetailsByKind(cachedPostingDetailsByKey, 'FBO'),
        mergedFboDetailCount: countPostingDetailsByKind(postingDetailsByKey, 'FBO'),
      },
    })

    const compatFboPostingNumbers = collectFboPostingNumbersNeedingCompat(fboPayloads, postingDetailsByKey)
    let compatLoadedCount = 0
    if (compatFboPostingNumbers.length > 0) {
      const compatDetails = await fetchFboPostingDetailsCompat(secrets, compatFboPostingNumbers)
      compatLoadedCount = compatDetails.size
      for (const [postingNumber, payload] of compatDetails.entries()) {
        postingDetailsByKey.set(getSalesPostingDetailsKey('FBO', postingNumber), payload)
      }
    }

    logFboShipmentTrace('api.refresh.compat.loaded', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: compatLoadedCount,
      meta: {
        compatRequestedCount: compatFboPostingNumbers.length,
        compatLoadedCount,
        compatSamplePostingNumbers: uniqueSample(compatFboPostingNumbers, 10),
        mergedFboDetailCountAfterCompat: countPostingDetailsByKind(postingDetailsByKey, 'FBO'),
      },
    })

    let reportRows: SalesShipmentReportRow[] = []
    let reportLoaded = false
    let reportTrace: any = null
    let reportSavedCsvFiles: Array<{ path: string; schema: string; reportCode: string; headers: string[] }> = []
    let reportSavedCsvCleanupCount = 0

    logFboShipmentTrace('api.refresh.report.begin', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      meta: {
        requestedPeriod,
      },
    })

    try {
      const report = await fetchSalesPostingsReportRows(secrets, requestedPeriod)
      reportLoaded = true
      reportTrace = report.trace
      reportRows = report.rows
        .map((row) => ({
          posting_number: normalizeTextValue(row?.posting_number),
          order_number: normalizeTextValue(row?.order_number),
          delivery_schema: normalizeTextValue(row?.delivery_schema),
          shipment_date: normalizeTextValue(row?.shipment_date),
          shipment_origin: normalizeTextValue(row?.shipment_origin),
          delivery_date: normalizeTextValue(row?.delivery_date),
          status: normalizeTextValue(row?.status),
          sku: normalizeTextValue(row?.sku),
          offer_id: normalizeTextValue(row?.offer_id),
          product_name: normalizeTextValue(row?.product_name),
          in_process_at: normalizeTextValue((row as any)?.in_process_at) || normalizeTextValue((row as any)?.raw_row?.['Принят в обработку']) || normalizeTextValue((row as any)?.raw_row?.['Принят в обработку (МСК)']),
          price: normalizeSalesShipmentReportNumber(row?.price),
          quantity: normalizeSalesShipmentReportNumber(row?.quantity),
          paid_by_customer: normalizeSalesShipmentReportNumber(row?.paid_by_customer),
          raw_row: normalizeSalesShipmentReportRawRow(row?.raw_row),
        }))
        .filter((row) => row.posting_number)

      const reportSavedCsv = persistSalesPostingsCsvArtifacts(Array.isArray(report.downloads) ? report.downloads : [])
      reportSavedCsvFiles = reportSavedCsv.files
      reportSavedCsvCleanupCount = Number(reportSavedCsv.cleanedLegacyFilesCount || 0)

      const reportSegments = Array.isArray(reportTrace?.segments) ? reportTrace.segments : []
      const failedSegments = reportSegments.filter((segment: any) => normalizeTextValue(segment?.error))
      const successfulSegments = reportSegments.filter((segment: any) => !normalizeTextValue(segment?.error))
      const failedSegmentSample = failedSegments.slice(0, 5).map((segment: any) => ({
        label: normalizeTextValue(segment?.label) || `${normalizeTextValue(segment?.from)}..${normalizeTextValue(segment?.to)}`,
        error: normalizeTextValue(segment?.error),
      }))

      logFboShipmentTrace('api.refresh.report.created', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: Number(reportRows.length),
        meta: {
          reportCode: report.reportCode,
          fileUrl: report.fileUrl,
          createBody: reportTrace?.createBody ?? null,
          reportStrategy: reportTrace?.strategy ?? 'single',
          reportPartial: Boolean(reportTrace?.partial),
          reportSegmentsTotal: reportSegments.length,
          reportSegmentsSucceeded: successfulSegments.length,
          reportSegmentsFailed: failedSegments.length,
          failedSegmentSample,
        },
      })

      logFboShipmentTrace('api.refresh.report.strategy', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: successfulSegments.length,
        meta: {
          reportStrategy: reportTrace?.strategy ?? 'single',
          reportPartial: Boolean(reportTrace?.partial),
          reportSegmentsTotal: reportSegments.length,
          reportSegmentsSucceeded: successfulSegments.length,
          reportSegmentsFailed: failedSegments.length,
          failedSegmentSample,
        },
      })

      logFboShipmentTrace('api.refresh.report.polled', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: Array.isArray(reportTrace?.pollAttempts) ? reportTrace.pollAttempts.length : 0,
        meta: {
          pollAttempts: reportTrace?.pollAttempts ?? [],
          reportStrategy: reportTrace?.strategy ?? 'single',
          reportSegmentsTotal: reportSegments.length,
          reportSegmentsSucceeded: successfulSegments.length,
          reportSegmentsFailed: failedSegments.length,
          failedSegmentSample,
        },
      })

      logFboShipmentTrace('api.refresh.report.downloaded', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: Number(reportTrace?.download?.bytes ?? 0),
        meta: {
          download: reportTrace?.download ?? null,
          reportSavedCsvCount: reportSavedCsvFiles.length,
          reportSavedCsvPaths: reportSavedCsvFiles.map((item) => item.path),
          reportSavedCsvCleanupCount,
          reportCsvHeaderCount: Number(reportTrace?.csv?.headerCount ?? 0),
          reportCsvHeaderNames: Array.isArray(reportTrace?.csv?.headerNames) ? reportTrace.csv.headerNames : [],
          reportStrategy: reportTrace?.strategy ?? 'single',
          reportSegmentsTotal: reportSegments.length,
          reportSegmentsSucceeded: successfulSegments.length,
          reportSegmentsFailed: failedSegments.length,
        },
      })

      logFboShipmentTrace('api.refresh.report.parsed', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: countRowsWithShipmentDate(reportRows),
        meta: {
          csv: reportTrace?.csv ?? null,
          reportRowsCount: reportRows.length,
          reportRowsWithShipmentDate: countRowsWithShipmentDate(reportRows),
          reportRowsWithDeliveryDate: countRowsWithDeliveryDate(reportRows),
          reportRowsWithShipmentOrigin: countRowsWithShipmentOrigin(reportRows),
          reportRowsWithStatus: countRowsWithStatus(reportRows),
          reportRowsFboCount: countRowsByDeliverySchema(reportRows, 'fbo'),
          reportRowsFboWithShipmentDate: (Array.isArray(reportRows) ? reportRows : []).filter((row) => normalizeDeliveryModelKey(row?.delivery_schema) === 'fbo' && Boolean(normalizeTextValue(row?.shipment_date))).length,
          reportRowsFboWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(reportRows, 'fbo'),
          reportRowsFboWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbo'),
          reportRowsFboWithStatus: countRowsByDeliverySchemaWithStatus(reportRows, 'fbo'),
          reportRowsFbsWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(reportRows, 'fbs'),
          reportRowsFbsWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbs'),
          reportRowsFbsWithStatus: countRowsByDeliverySchemaWithStatus(reportRows, 'fbs'),
          reportDeliveryDateSample: uniqueSample(reportRows.filter((row) => normalizeTextValue(row?.delivery_date)).map((row) => row.delivery_date), 10),
          reportShipmentOriginSample: uniqueSample(reportRows.filter((row) => normalizeTextValue(row?.shipment_origin)).map((row) => row.shipment_origin), 10),
          reportStatusSample: uniqueSample(reportRows.filter((row) => normalizeTextValue(row?.status)).map((row) => normalizeSalesReportStatusValue(row?.status)), 10),
          reportAcceptedAtSpan: formatDateSpanLabel(
            buildRowsDateSpan(reportRows, ['in_process_at', 'raw_row.Принят в обработку', 'raw_row.Принят в обработку (МСК)']).from,
            buildRowsDateSpan(reportRows, ['in_process_at', 'raw_row.Принят в обработку', 'raw_row.Принят в обработку (МСК)']).to,
          ),
          reportDeliveryDateSpan: formatDateSpanLabel(
            buildRowsDateSpan(reportRows, ['delivery_date']).from,
            buildRowsDateSpan(reportRows, ['delivery_date']).to,
          ),
          reportSavedCsvCount: reportSavedCsvFiles.length,
          reportSavedCsvPaths: reportSavedCsvFiles.map((item) => item.path),
          reportSavedCsvCleanupCount,
          reportCsvHeaderCount: Number(reportTrace?.csv?.headerCount ?? 0),
          reportCsvHeaderNames: Array.isArray(reportTrace?.csv?.headerNames) ? reportTrace.csv.headerNames : [],
          reportStrategy: reportTrace?.strategy ?? 'single',
          reportPartial: Boolean(reportTrace?.partial),
          reportSegmentsTotal: reportSegments.length,
          reportSegmentsSucceeded: successfulSegments.length,
          reportSegmentsFailed: failedSegments.length,
          failedSegmentSample,
        },
      })

      if (Boolean(reportTrace?.partial)) {
        logFboShipmentTrace('api.refresh.report.partial', {
          storeClientId: secrets.clientId,
          period: requestedPeriod,
          status: failedSegments.length > 0 ? 'error' : 'success',
          itemsCount: countRowsWithShipmentDate(reportRows),
          errorMessage: failedSegments.length > 0 ? failedSegments[0]?.error ?? null : null,
          meta: {
            reportStrategy: reportTrace?.strategy ?? 'single',
            reportPartial: true,
            reportSegmentsTotal: reportSegments.length,
            reportSegmentsSucceeded: successfulSegments.length,
            reportSegmentsFailed: failedSegments.length,
            failedSegmentSample,
          },
        })
      }
    } catch (error: any) {
      reportRows = []
      reportTrace = null
      logFboShipmentTrace('api.refresh.report.error', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        status: 'error',
        errorMessage: error?.message ?? String(error),
        meta: {
          requestedPeriod,
          reportStrategy: 'single',
          reportBuildError: error?.message ?? String(error),
        },
      })
    }

    const fetchedAt = new Date().toISOString()

    const reportPersistResult = reportLoaded
      ? persistFboPostingsReport({
        storeClientId: secrets.clientId,
        periodKey: buildDatasetScopeKey(requestedPeriod),
        rows: reportRows,
        fetchedAt,
      })
      : null

    if (reportLoaded) {
      logFboShipmentTrace('api.refresh.report.persisted', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: Number(reportPersistResult?.shipmentDateCount ?? 0),
        meta: {
          reportPersistResult,
          reportRowsCount: reportRows.length,
          reportRowsWithShipmentDate: countRowsWithShipmentDate(reportRows),
          reportRowsWithDeliveryDate: countRowsWithDeliveryDate(reportRows),
          reportRowsWithShipmentOrigin: countRowsWithShipmentOrigin(reportRows),
          reportRowsWithStatus: countRowsWithStatus(reportRows),
          reportRowsFboWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbo'),
          reportRowsFboWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(reportRows, 'fbo'),
          reportRowsFboWithStatus: countRowsByDeliverySchemaWithStatus(reportRows, 'fbo'),
          reportRowsFbsWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbs'),
          reportRowsFbsWithShipmentOrigin: countRowsByDeliverySchemaWithShipmentOrigin(reportRows, 'fbs'),
          reportRowsFbsWithStatus: countRowsByDeliverySchemaWithStatus(reportRows, 'fbs'),
          reportDeliveryDateSample: uniqueSample(reportRows.filter((row) => normalizeTextValue(row?.delivery_date)).map((row) => row.delivery_date), 10),
          reportStatusSample: uniqueSample(reportRows.filter((row) => normalizeTextValue(row?.status)).map((row) => normalizeSalesReportStatusValue(row?.status)), 10),
        },
      })

      if (countRowsWithDeliveryDate(reportRows) === 0) {
        logFboShipmentTrace('api.refresh.report.empty', {
          storeClientId: secrets.clientId,
          period: requestedPeriod,
          itemsCount: 0,
          meta: {
            reportTrace,
            reportRowsCount: reportRows.length,
            reportRowsFboCount: countRowsByDeliverySchema(reportRows, 'fbo'),
            reportRowsWithDeliveryDate: countRowsWithDeliveryDate(reportRows),
            reportRowsWithStatus: countRowsWithStatus(reportRows),
            reportRowsFboWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbo'),
            reportRowsFboWithStatus: countRowsByDeliverySchemaWithStatus(reportRows, 'fbo'),
            reportRowsFbsWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbs'),
            reportRowsFbsWithStatus: countRowsByDeliverySchemaWithStatus(reportRows, 'fbs'),
          },
        })
      }
    }

    const persistResult = buildAndPersistFboSalesSnapshot({
      storeClientId: secrets.clientId,
      periodKey: buildDatasetScopeKey(requestedPeriod),
      fboPayloads,
      postingDetailsByKey,
      reportRows,
      fetchedAt,
    })

    logFboShipmentTrace('api.refresh.snapshot.persisted', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: Number(persistResult?.persisted?.shipmentDateCount ?? persistResult?.trace?.postingsWithResolvedShipmentDate ?? 0),
      meta: {
        reportRowsCount: reportRows.length,
        reportLoaded,
        reportPersistResult,
        reportFboShipmentDateImportedToLocalDb: reportRows.length > 0,
        reportRowsWithDeliveryDate: countRowsWithDeliveryDate(reportRows),
        reportRowsFboWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbo'),
        reportRowsFbsWithDeliveryDate: countRowsByDeliverySchemaWithDeliveryDate(reportRows, 'fbs'),
        ...persistResult,
      },
    })

    const trimListPayload = (payload: any) => {
      const postings = extractPostingsFromPayload(payload)
      if (Array.isArray(postings) && postings.length > 0) return { result: { postings } }
      return payload
    }

    const MAX_DETAILS_ITEMS = 2000
    const detailsItems: Array<{ key: string; payload: any }> = []
    const seenDetailKeys = new Set<string>()
    for (const env of payloads) {
      const endpointKind = String(env.endpoint).includes('/fbs/') ? 'FBS' : 'FBO'
      for (const posting of extractPostingsFromPayload(env.payload)) {
        const postingNumber = String((posting as any)?.posting_number ?? (posting as any)?.postingNumber ?? '').trim()
        if (!postingNumber) continue
        const key = getSalesPostingDetailsKey(endpointKind, postingNumber)
        if (seenDetailKeys.has(key)) continue
        const payload = postingDetailsByKey.get(key)
        if (!payload) continue
        detailsItems.push({ key, payload })
        seenDetailKeys.add(key)
        if (detailsItems.length >= MAX_DETAILS_ITEMS) break
      }
      if (detailsItems.length >= MAX_DETAILS_ITEMS) break
    }

    const persistRawSnapshot = (endpoint: string, responseBody: any) => {
      dbRecordApiRawResponse({
        storeClientId: secrets.clientId,
        method: 'LOCAL',
        endpoint,
        requestBody: {
          mode: 'sales-cache-snapshot',
          period: normalizedRequestedPeriod,
        },
        responseBody,
        httpStatus: 200,
        isSuccess: true,
        fetchedAt,
      })
    }

    persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbs, {
      sourceEndpoint: '/v3/posting/fbs/list',
      period: normalizedRequestedPeriod,
      payloads: fbsPayloads.map((item) => trimListPayload(item.payload)),
    })
    persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.fbo, {
      sourceEndpoint: '/v2/posting/fbo/list',
      period: normalizedRequestedPeriod,
      payloads: fboPayloads.map((item) => trimListPayload(item.payload)),
    })
    persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.details, {
      period: normalizedRequestedPeriod,
      items: detailsItems,
    })
    persistRawSnapshot(SALES_CACHE_SNAPSHOT_ENDPOINTS.postingsReport, {
      period: normalizedRequestedPeriod,
      rows: reportRows,
      csvHeaderCount: Number(reportTrace?.csv?.headerCount ?? 0),
      csvHeaderNames: Array.isArray(reportTrace?.csv?.headerNames) ? reportTrace.csv.headerNames : [],
      savedCsvFiles: reportSavedCsvFiles,
    })

    const persistedReportSnapshot = inspectPersistedPostingsReportSnapshot(secrets.clientId, requestedPeriod)
    logFboShipmentTrace('api.refresh.report.snapshot.persisted', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      status: persistedReportSnapshot.found ? 'success' : 'error',
      itemsCount: Number(persistedReportSnapshot.rowsCount ?? 0),
      errorMessage: persistedReportSnapshot.found ? null : 'Snapshot отчёта postings не найден в api_raw_cache',
      meta: {
        reportSnapshotPersistedToApiRawCache: persistedReportSnapshot.found,
        reportSnapshotRowsCount: persistedReportSnapshot.rowsCount,
        reportSnapshotFetchedAt: persistedReportSnapshot.fetchedAt,
        reportSnapshotPeriodMatches: persistedReportSnapshot.periodMatches,
        reportSnapshotRowsWithDeliveryDate: persistedReportSnapshot.rowsWithDeliveryDate,
        reportSnapshotRowsWithShipmentOrigin: persistedReportSnapshot.rowsWithShipmentOrigin,
        reportSnapshotRowsWithStatus: persistedReportSnapshot.rowsWithStatus,
        reportSnapshotPeriodMatchesRequested: persistedReportSnapshot.periodMatches,
        reportSnapshotRowsFboWithShipmentOrigin: persistedReportSnapshot.rowsFboWithShipmentOrigin,
        reportSnapshotRowsFbsWithShipmentOrigin: persistedReportSnapshot.rowsFbsWithShipmentOrigin,
        reportSnapshotResponseTruncated: persistedReportSnapshot.responseTruncated,
        reportSnapshotResponseBodyLen: persistedReportSnapshot.responseBodyLen,
        reportSnapshotCsvHeaderCount: persistedReportSnapshot.csvHeaderCount,
        reportSnapshotCsvHeaderNames: persistedReportSnapshot.csvHeaderNames,
        reportSnapshotSavedCsvFilesCount: persistedReportSnapshot.savedCsvFilesCount,
        reportSnapshotSavedCsvPaths: persistedReportSnapshot.savedCsvPaths,
      },
    })

    const { rows: builtRows, sourceEndpoints, deliveryDateTrace, statusTrace } = buildSalesRowsFromPayloads(secrets.clientId, requestedPeriod, payloads, postingDetailsByKey, reportRows)
    const rows = await applyCbrConversionsToSalesRows(builtRows)
    logFboShipmentTrace('api.refresh.rows.built', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: countRowsByDeliveryModelWithShipmentDate(rows, 'FBO'),
      meta: {
        salesRowsCount: rows.length,
        fboRowsCount: countRowsByDeliveryModel(rows, 'FBO'),
        fboRowsWithShipmentDate: countRowsByDeliveryModelWithShipmentDate(rows, 'FBO'),
        salesRowsWithDeliveryDate: Number(deliveryDateTrace?.finalRowsWithDeliveryDate ?? 0),
        salesRowsWithoutDeliveryDate: Number(deliveryDateTrace?.finalRowsWithoutDeliveryDate ?? 0),
        fboRowsWithDeliveryDate: Number(deliveryDateTrace?.fboRowsWithDeliveryDate ?? 0),
        fbsRowsWithDeliveryDate: Number(deliveryDateTrace?.fbsRowsWithDeliveryDate ?? 0),
        rfbsRowsWithDeliveryDate: Number(deliveryDateTrace?.rfbsRowsWithDeliveryDate ?? 0),
        deliveryDateMatchedRows: Number(deliveryDateTrace?.deliveryDateMatchedRows ?? 0),
        deliveryDateResolvedRows: Number(deliveryDateTrace?.deliveryDateResolvedRows ?? 0),
        deliveryDateClearedRows: Number(deliveryDateTrace?.deliveryDateClearedRows ?? 0),
        missingDeliveryDatePostingNumbers: Array.isArray(deliveryDateTrace?.missingDeliveryDatePostingNumbers) ? deliveryDateTrace.missingDeliveryDatePostingNumbers : [],
        reportDeliveryDateKeyCount: Number(deliveryDateTrace?.reportDeliveryDateKeyCount ?? 0),
        reportRowsWithDeliveryDate: Number(deliveryDateTrace?.reportRowsWithDeliveryDate ?? 0),
        reportRowsFboWithDeliveryDate: Number(deliveryDateTrace?.reportRowsFboWithDeliveryDate ?? 0),
        reportRowsFbsWithDeliveryDate: Number(deliveryDateTrace?.reportRowsFbsWithDeliveryDate ?? 0),
        reportDeliveryDateSample: Array.isArray(deliveryDateTrace?.reportDeliveryDateSample) ? deliveryDateTrace.reportDeliveryDateSample : [],
        sourceEndpoints,
      },
    })
    logFboShipmentTrace('api.refresh.origin.rows.built', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: Number(deliveryDateTrace?.finalRowsWithShipmentOrigin ?? 0),
      meta: {
        salesRowsCount: rows.length,
        salesRowsWithShipmentOrigin: Number(deliveryDateTrace?.finalRowsWithShipmentOrigin ?? 0),
        salesRowsWithoutShipmentOrigin: Number(deliveryDateTrace?.finalRowsWithoutShipmentOrigin ?? 0),
        fboRowsWithShipmentOrigin: Number(deliveryDateTrace?.fboRowsWithShipmentOrigin ?? 0),
        fbsRowsWithShipmentOrigin: Number(deliveryDateTrace?.fbsRowsWithShipmentOrigin ?? 0),
        rfbsRowsWithShipmentOrigin: Number(deliveryDateTrace?.rfbsRowsWithShipmentOrigin ?? 0),
        shipmentOriginMatchedRows: Number(deliveryDateTrace?.shipmentOriginMatchedRows ?? 0),
        shipmentOriginResolvedRows: Number(deliveryDateTrace?.shipmentOriginResolvedRows ?? 0),
        shipmentOriginClearedRows: Number(deliveryDateTrace?.shipmentOriginClearedRows ?? 0),
        missingShipmentOriginPostingNumbers: Array.isArray(deliveryDateTrace?.missingShipmentOriginPostingNumbers) ? deliveryDateTrace.missingShipmentOriginPostingNumbers : [],
        reportShipmentOriginKeyCount: Number(deliveryDateTrace?.reportShipmentOriginKeyCount ?? 0),
        reportRowsWithShipmentOrigin: Number(deliveryDateTrace?.reportRowsWithShipmentOrigin ?? 0),
        reportRowsFboWithShipmentOrigin: Number(deliveryDateTrace?.reportRowsFboWithShipmentOrigin ?? 0),
        reportRowsFbsWithShipmentOrigin: Number(deliveryDateTrace?.reportRowsFbsWithShipmentOrigin ?? 0),
        reportShipmentOriginSample: Array.isArray(deliveryDateTrace?.reportShipmentOriginSample) ? deliveryDateTrace.reportShipmentOriginSample : [],
        sourceEndpoints,
      },
    })

    logFboShipmentTrace('api.refresh.status.rows.built', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: Number(statusTrace?.finalRowsWithStatus ?? 0),
      meta: {
        salesRowsCount: rows.length,
        salesRowsWithStatus: Number(statusTrace?.finalRowsWithStatus ?? 0),
        salesRowsWithoutStatus: Number(statusTrace?.finalRowsWithoutStatus ?? 0),
        finalDeliveredRows: Number(statusTrace?.finalDeliveredRows ?? 0),
        fboRowsWithStatus: Number(statusTrace?.fboRowsWithStatus ?? 0),
        fbsRowsWithStatus: Number(statusTrace?.fbsRowsWithStatus ?? 0),
        rfbsRowsWithStatus: Number(statusTrace?.rfbsRowsWithStatus ?? 0),
        fboRowsWithDeliveredStatus: Number(statusTrace?.fboRowsWithDeliveredStatus ?? 0),
        fbsRowsWithDeliveredStatus: Number(statusTrace?.fbsRowsWithDeliveredStatus ?? 0),
        rfbsRowsWithDeliveredStatus: Number(statusTrace?.rfbsRowsWithDeliveredStatus ?? 0),
        statusMatchedRows: Number(statusTrace?.statusMatchedRows ?? 0),
        statusResolvedRows: Number(statusTrace?.statusResolvedRows ?? 0),
        statusClearedRows: Number(statusTrace?.statusClearedRows ?? 0),
        deliveredRowsWithClearedDetails: Number(statusTrace?.deliveredRowsWithClearedDetails ?? 0),
        missingStatusPostingNumbers: Array.isArray(statusTrace?.missingStatusPostingNumbers) ? statusTrace.missingStatusPostingNumbers : [],
        reportStatusKeyCount: Number(statusTrace?.reportStatusKeyCount ?? 0),
        reportRowsWithStatus: Number(statusTrace?.reportRowsWithStatus ?? 0),
        reportRowsFboWithStatus: Number(statusTrace?.reportRowsFboWithStatus ?? 0),
        reportRowsFbsWithStatus: Number(statusTrace?.reportRowsFbsWithStatus ?? 0),
        reportStatusSample: Array.isArray(statusTrace?.reportStatusSample) ? statusTrace.reportStatusSample : [],
        sourceEndpoints,
      },
    })
    logPaidByCustomerTrace('api.refresh.paid_by_customer.trace', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      rows,
      payloads,
      postingDetailsByKey,
      reportRows,
    })

    const persistedSalesSnapshot = persistDatasetSnapshot({
      storeClientId: secrets.clientId,
      dataset: 'sales',
      scopeKey: buildDatasetScopeKey(requestedPeriod),
      period: requestedPeriod,
      rows,
      sourceKind: 'api-live',
      sourceEndpoints,
    })

    logFboShipmentTrace('sales.snapshot.saved', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      itemsCount: Number(persistedSalesSnapshot?.storedRowsCount ?? rows.length),
      meta: buildSalesSnapshotTraceMeta({
        rows,
        scopeKey: buildDatasetScopeKey(requestedPeriod),
        sourceKind: 'api-live',
        saveResult: persistedSalesSnapshot,
        requestedPeriod,
      }),
    })

    if (isDefaultRollingSalesPeriod(requestedPeriod)) {
      const persistedRollingSnapshot = persistDatasetSnapshot({
        storeClientId: secrets.clientId,
        dataset: 'sales',
        scopeKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
        period: requestedPeriod,
        rows,
        sourceKind: 'api-live-default-window',
        sourceEndpoints,
      })

      logFboShipmentTrace('sales.snapshot.saved', {
        storeClientId: secrets.clientId,
        period: requestedPeriod,
        itemsCount: Number(persistedRollingSnapshot?.storedRowsCount ?? rows.length),
        meta: buildSalesSnapshotTraceMeta({
          rows,
          scopeKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
          sourceKind: 'api-live-default-window',
          saveResult: persistedRollingSnapshot,
          requestedPeriod,
        }),
      })
    }

    return { rowsCount: rows.length }
  } catch (e: any) {
    const technicalMessage = e?.message ?? String(e)
    logFboShipmentTrace('api.refresh.error', {
      storeClientId: secrets.clientId,
      period: requestedPeriod,
      status: 'error',
      errorMessage: translateSalesDatasetErrorMessage(technicalMessage),
      meta: {
        stack: e?.stack ?? null,
        technicalError: technicalMessage,
      },
    })
    throw e
  }
}

function salesRowNeedsFboBackfill(row: any): boolean {
  if (!row || typeof row !== 'object') return false
  const model = String(row?.delivery_model ?? '').trim().toUpperCase()
  if (model !== 'FBO') return false

  const status = String(row?.status ?? '').trim()
  const shipmentDate = String(row?.shipment_date ?? '').trim()
  const deliveryCluster = String(row?.delivery_cluster ?? '').trim()
  const deliveryDate = String(row?.delivery_date ?? '').trim()

  if (!shipmentDate) return true
  if (!deliveryCluster) return true
  if (status === 'Доставлен' && !deliveryDate) return true
  return false
}

function salesRowsNeedFboBackfill(rows: any[]): boolean {
  return rows.some((row) => salesRowNeedsFboBackfill(row))
}

export async function ensureLocalSalesSnapshotFromApiIfMissing(
  secrets: Secrets | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
): Promise<{ refreshed: boolean; rowsCount: number }> {
  const storeClientId = secrets?.clientId ?? null
  const cacheByEndpoint = getSalesSnapshotMap(storeClientId)
  const hasLocalCoverage = isRequestedSalesPeriodCoveredByRawCache(cacheByEndpoint, requestedPeriod)

  if (!hasLocalCoverage && secrets) {
    const refreshed = await refreshSalesRawSnapshotFromApi(secrets, requestedPeriod)
    return {
      refreshed: true,
      rowsCount: Number(refreshed?.rowsCount ?? 0),
    }
  }

  const snapshotRows = readScopedSalesSnapshotRows(storeClientId, requestedPeriod)
  if (snapshotRows) {
    return {
      refreshed: false,
      rowsCount: snapshotRows.length,
    }
  }

  if (hasLocalCoverage) {
    const { rows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(storeClientId, requestedPeriod)
    const persistedSnapshot = persistDatasetSnapshot({
      storeClientId,
      dataset: 'sales',
      scopeKey: buildDatasetScopeKey(requestedPeriod),
      period: requestedPeriod,
      rows,
      sourceKind: 'api-raw-cache',
      sourceEndpoints,
    })
    logFboShipmentTrace('sales.snapshot.saved', {
      storeClientId,
      period: requestedPeriod,
      itemsCount: Number(persistedSnapshot?.storedRowsCount ?? rows.length),
      meta: buildSalesSnapshotTraceMeta({
        rows,
        scopeKey: buildDatasetScopeKey(requestedPeriod),
        sourceKind: 'api-raw-cache',
        saveResult: persistedSnapshot,
        requestedPeriod,
      }),
    })
    return {
      refreshed: false,
      rowsCount: rows.length,
    }
  }

  return {
    refreshed: false,
    rowsCount: 0,
  }
}

export function hasExactLocalSalesSnapshot(
  storeClientId: string | null | undefined,
  requestedPeriod: SalesPeriod | null | undefined,
): boolean {
  const rows = dbGetDatasetSnapshotRows({
    storeClientId: storeClientId ?? null,
    dataset: 'sales',
    scopeKey: buildDatasetScopeKey(requestedPeriod),
  })
  return Array.isArray(rows)
}

export function getLocalDatasetRows(
  storeClientId: string | null | undefined,
  datasetRaw: LocalDatasetName,
  options?: { period?: SalesPeriod | null },
): any[] {
  const dataset = String(datasetRaw ?? '').trim() || 'products'
  const requestedPeriod = options?.period ?? null
  const scopeKey = buildDatasetScopeKey(requestedPeriod)

  if (dataset === 'sales') {
    const exactSnapshotRows = readScopedSalesSnapshotRows(storeClientId ?? null, requestedPeriod)
    if (exactSnapshotRows) return exactSnapshotRows

    const rollingRows = readRollingSalesSnapshotRows(storeClientId ?? null, requestedPeriod)
    if (rollingRows && rollingRows.length > 0) {
      if (scopeKey && isDefaultRollingSalesPeriod(requestedPeriod)) {
        const persistedRollingSnapshot = persistDatasetSnapshot({
          storeClientId,
          dataset,
          scopeKey,
          period: requestedPeriod,
          rows: rollingRows,
          sourceKind: 'dataset-snapshot-default-window',
          sourceEndpoints: [],
        })
        logFboShipmentTrace('sales.snapshot.saved', {
          storeClientId,
          period: requestedPeriod,
          itemsCount: Number(persistedRollingSnapshot?.storedRowsCount ?? rollingRows.length),
          meta: buildSalesSnapshotTraceMeta({
            rows: rollingRows,
            scopeKey,
            sourceKind: 'dataset-snapshot-default-window',
            saveResult: persistedRollingSnapshot,
            requestedPeriod,
          }),
        })
      } else {
        logFboShipmentTrace('sales.read.default_snapshot_fallback', {
          storeClientId: storeClientId ?? null,
          period: requestedPeriod,
          itemsCount: rollingRows.length,
          meta: buildSalesReadTraceMeta({
            rows: rollingRows,
            scopeKey: SALES_DEFAULT_ROLLING_SCOPE_KEY,
            sourceKind: 'dataset-snapshot-default-window-fallback',
            requestedPeriod,
          }),
        })
      }
      return rollingRows
    }

    const cacheByEndpoint = getSalesSnapshotMap(storeClientId ?? null)
    const hasLocalCoverage = isRequestedSalesPeriodCoveredByRawCache(cacheByEndpoint, requestedPeriod)
    if (cacheByEndpoint.size > 0 && hasLocalCoverage) {
      const { rows, sourceEndpoints } = buildSalesRowsFromLocalRawCache(storeClientId ?? null, requestedPeriod)
      const persistedSnapshot = persistDatasetSnapshot({
        storeClientId,
        dataset,
        scopeKey,
        period: requestedPeriod,
        rows,
        sourceKind: 'api-raw-cache',
        sourceEndpoints,
      })
      logFboShipmentTrace('sales.snapshot.saved', {
        storeClientId,
        period: requestedPeriod,
        itemsCount: Number(persistedSnapshot?.storedRowsCount ?? rows.length),
        meta: buildSalesSnapshotTraceMeta({
          rows,
          scopeKey,
          sourceKind: 'api-raw-cache',
          saveResult: persistedSnapshot,
          requestedPeriod,
        }),
      })
      return rows
    }

    if (scopeKey) {
      return []
    }
  }

  const fromSnapshot = dbGetDatasetSnapshotRows({ storeClientId: storeClientId ?? null, dataset, scopeKey })
  if (Array.isArray(fromSnapshot)) {
    if (dataset === 'sales') {
      return filterSalesRowsStrictByPeriod(fromSnapshot, options?.period ?? null)
    }
    return fromSnapshot
  }

  if (dataset === 'products') {
    const rows = dbGetProducts(storeClientId ?? null)
    persistDatasetSnapshot({ storeClientId, dataset, rows, sourceKind: 'db-table' })
    return rows
  }

  if (dataset === 'stocks') {
    const rows = dbGetStockViewRows(storeClientId ?? null)
    persistDatasetSnapshot({ storeClientId, dataset, rows, sourceKind: 'db-view' })
    return rows
  }

  if (dataset === 'returns' || dataset === 'forecast-demand') {
    const rows = dbGetProducts(storeClientId ?? null)
    persistDatasetSnapshot({ storeClientId, dataset, rows, sourceKind: 'derived-products' })
    return rows
  }

  if (dataset === 'sales') {
    return []
  }

  return []
}

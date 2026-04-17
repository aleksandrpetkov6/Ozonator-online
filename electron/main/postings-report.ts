import { gunzipSync, inflateRawSync } from 'node:zlib'
import type { Secrets } from './types'

const OZON_API_BASE_URL = 'https://api-seller.ozon.ru'
const REPORT_INFO_POLL_ATTEMPTS = 25
const REPORT_INFO_POLL_DELAY_MS = 1500
const CSV_MOSCOW_OFFSET_MS = 3 * 60 * 60 * 1000
const DEFAULT_REPORT_LOOKBACK_DAYS = 30
const REPORT_PRIMARY_CHUNK_DAYS = 7
const REPORT_FALLBACK_CHUNK_DAYS = 1

type JsonRecord = Record<string, unknown>

export type ReportPeriodInput = {
  from?: string | null
  to?: string | null
}

export type SalesPostingsReportRow = {
  posting_number: string
  order_number: string
  delivery_schema: string
  shipment_date: string
  shipment_origin: string
  delivery_date: string
  status: string
  sku: string
  offer_id: string
  product_name: string
  in_process_at: string
  price: number | ''
  quantity: number | ''
  paid_by_customer: number | ''
  raw_row: Record<string, string>
}

type ReportPollAttempt = { attempt: number; status: string; hasFile: boolean; error: string }

type ReportDownloadTrace = {
  contentType: string
  contentEncoding: string
  bytes: number
  archiveKind: 'plain' | 'gzip' | 'zip' | 'unknown'
  extractedEntryName: string
  extractedBytes: number
}

export type SalesPostingsReportDownloadArtifact = {
  schema: 'fbo' | 'fbs'
  label: string
  from: string
  to: string
  reportCode: string
  fileUrl: string
  archiveKind: 'plain' | 'gzip' | 'zip' | 'unknown'
  extractedEntryName: string
  csvText: string
  headerNames: string[]
}

type ReportCsvTrace = {
  rowsRaw: number
  rowsMapped: number
  rowsWithPostingNumber: number
  rowsWithShipmentDate: number
  rowsWithShipmentOrigin: number
  rowsWithShipmentWarehouse: number
  rowsWithShipmentCluster: number
  rowsWithDeliveryDate: number
  rowsWithStatus: number
  rowsFbo: number
  rowsFboWithShipmentDate: number
  rowsFboWithShipmentOrigin: number
  rowsFboWithShipmentCluster: number
  rowsFboWithDeliveryDate: number
  rowsFboWithStatus: number
  rowsFbs: number
  rowsFbsWithShipmentOrigin: number
  rowsFbsWithShipmentWarehouse: number
  rowsFbsWithDeliveryDate: number
  rowsFbsWithStatus: number
  headerCount: number
  headerNames: string[]
  headerSample: string[]
  samplePostingNumbers: string[]
  sampleShipmentDates: string[]
  sampleShipmentOrigins: string[]
  sampleShipmentWarehouses: string[]
  sampleShipmentClusters: string[]
  sampleDeliveryDates: string[]
  sampleStatuses: string[]
}

export type SalesPostingsReportTraceSegment = {
  label: string
  from: string
  to: string
  reportCode: string
  fileUrl: string
  createBody: Record<string, unknown>
  pollAttempts: ReportPollAttempt[]
  download: ReportDownloadTrace | null
  csv: ReportCsvTrace | null
  rows: number
  rowsWithShipmentDate: number
  error: string
}

export type SalesPostingsReportTrace = {
  reportCode: string
  fileUrl: string
  createBody: Record<string, unknown>
  pollAttempts: ReportPollAttempt[]
  download: ReportDownloadTrace
  csv: ReportCsvTrace
  strategy: 'single' | 'chunked-7d' | 'chunked-1d'
  partial: boolean
  segments: SalesPostingsReportTraceSegment[]
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function text(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function normalizePeriodDate(value: unknown): string | null {
  const raw = text(value)
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null
}

function normalizePeriod(period: ReportPeriodInput | null | undefined): { from: string | null; to: string | null } {
  let from = normalizePeriodDate(period?.from)
  let to = normalizePeriodDate(period?.to)
  if (!from && to) from = to
  if (from && !to) to = from
  if (from && to && from > to) [from, to] = [to, from]
  return { from, to }
}

function resolvePeriodBounds(period: ReportPeriodInput | null | undefined): { from: string; to: string } {
  const normalized = normalizePeriod(period)
  if (normalized.from && normalized.to) {
    return { from: normalized.from, to: normalized.to }
  }

  const now = new Date()
  const toDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()))
  const fromDate = new Date(toDate.getTime() - ((DEFAULT_REPORT_LOOKBACK_DAYS - 1) * 24 * 60 * 60 * 1000))
  const formatDate = (value: Date) => value.toISOString().slice(0, 10)
  return { from: formatDate(fromDate), to: formatDate(toDate) }
}

function buildPostingsReportCreateBody(period: ReportPeriodInput | null | undefined, deliverySchema: 'fbo' | 'fbs' = 'fbo') {
  const bounds = resolvePeriodBounds(period)
  const fromDate = new Date(`${bounds.from}T00:00:00.000Z`)
  const toDate = new Date(`${bounds.to}T23:59:59.999Z`)

  return {
    filter: {
      processed_at_from: fromDate.toISOString(),
      processed_at_to: toDate.toISOString(),
      delivery_schema: [deliverySchema],
    },
    language: 'DEFAULT',
  }
}

function addDays(dateTextRaw: string, days: number): string {
  const dt = new Date(`${dateTextRaw}T00:00:00.000Z`)
  dt.setUTCDate(dt.getUTCDate() + days)
  return dt.toISOString().slice(0, 10)
}

function diffDaysInclusive(from: string, to: string): number {
  const fromMs = Date.parse(`${from}T00:00:00.000Z`)
  const toMs = Date.parse(`${to}T00:00:00.000Z`)
  return Math.max(1, Math.floor((toMs - fromMs) / (24 * 60 * 60 * 1000)) + 1)
}

function splitPeriodIntoSegments(period: { from: string; to: string }, chunkDays: number): Array<{ from: string; to: string; label: string }> {
  const totalDays = diffDaysInclusive(period.from, period.to)
  if (chunkDays <= 0 || totalDays <= chunkDays) {
    return [{ from: period.from, to: period.to, label: `${period.from}..${period.to}` }]
  }

  const out: Array<{ from: string; to: string; label: string }> = []
  let cursor = period.from
  while (cursor <= period.to) {
    const tentativeTo = addDays(cursor, chunkDays - 1)
    const segmentTo = tentativeTo < period.to ? tentativeTo : period.to
    out.push({ from: cursor, to: segmentTo, label: `${cursor}..${segmentTo}` })
    cursor = addDays(segmentTo, 1)
  }
  return out
}

function shouldFallbackToChunkedReports(error: unknown): boolean {
  const message = text((error as any)?.message ?? error).toLowerCase()
  return message.includes('failed to build report')
    || message.includes('report file is not ready')
    || message.includes('report error: failed')
    || message.includes('failed (')
}

async function ozonPost(secrets: Secrets, endpoint: string, body: unknown): Promise<JsonRecord> {
  const res = await fetch(`${OZON_API_BASE_URL}${endpoint}`, {
    method: 'POST',
    headers: {
      'Client-Id': secrets.clientId,
      'Api-Key': secrets.apiKey,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body ?? {}),
  })

  const rawText = await res.text()
  let parsed: JsonRecord = {}
  if (rawText.trim()) {
    try {
      parsed = JSON.parse(rawText) as JsonRecord
    } catch {
      throw new Error(`Ozon API error: invalid JSON for ${endpoint} (HTTP ${res.status})`)
    }
  }

  if (!res.ok) {
    const message = text((parsed as any)?.message) || text((parsed as any)?.error) || `HTTP ${res.status}`
    throw new Error(`Ozon API error ${endpoint}: ${message}`)
  }

  return parsed
}

async function ozonReportPostingsCreate(secrets: Secrets, body: unknown) {
  return ozonPost(secrets, '/v1/report/postings/create', body ?? {})
}

async function ozonReportInfo(secrets: Secrets, code: string) {
  const normalizedCode = text(code)
  if (!normalizedCode) throw new Error('Не указан code отчёта')
  return ozonPost(secrets, '/v1/report/info', { code: normalizedCode })
}

async function ozonDownloadReportFile(fileUrl: string): Promise<{ bytes: Uint8Array; contentType: string; contentEncoding: string }> {
  const normalizedUrl = text(fileUrl)
  if (!normalizedUrl) throw new Error('Не указан URL отчёта')

  const res = await fetch(normalizedUrl, {
    method: 'GET',
    headers: {
      Accept: 'text/csv,application/octet-stream;q=0.9,*/*;q=0.8',
    },
  })

  if (!res.ok) {
    throw new Error(`Ozon report download error: HTTP ${res.status}`)
  }

  const buffer = await res.arrayBuffer()
  return {
    bytes: new Uint8Array(buffer),
    contentType: text(res.headers.get('content-type')),
    contentEncoding: text(res.headers.get('content-encoding')),
  }
}

function normalizeReportStatus(value: unknown): string {
  return text(value).toLowerCase().replace(/\s+/g, '_')
}

function looksLikeReadyReportStatus(value: unknown): boolean {
  const status = normalizeReportStatus(value)
  return status === 'success' || status === 'completed' || status === 'done' || status === 'ready'
}

function looksLikeFailedReportStatus(value: unknown): boolean {
  const status = normalizeReportStatus(value)
  return status === 'error' || status === 'failed' || status === 'fail' || status === 'cancelled' || status === 'canceled'
}

function decodeCsvBytes(bytes: Uint8Array): string {
  if (bytes.length === 0) return ''

  const tryDecode = (encoding: string) => {
    try {
      return new TextDecoder(encoding).decode(bytes)
    } catch {
      return ''
    }
  }

  const utf8 = tryDecode('utf-8')
  if (utf8 && !utf8.includes('�')) return utf8

  const win1251 = tryDecode('windows-1251')
  if (win1251) return win1251

  return utf8 || tryDecode('utf-8')
}

function bytesStartWith(bytes: Uint8Array, signature: number[]): boolean {
  if (bytes.length < signature.length) return false
  for (let index = 0; index < signature.length; index += 1) {
    if (bytes[index] !== signature[index]) return false
  }
  return true
}

function readUInt16LE(bytes: Uint8Array, offset: number): number {
  return bytes[offset] | (bytes[offset + 1] << 8)
}

function readUInt32LE(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset]
    | (bytes[offset + 1] << 8)
    | (bytes[offset + 2] << 16)
    | (bytes[offset + 3] << 24)
  ) >>> 0
}

function extractFirstZipEntry(bytes: Uint8Array): { name: string; bytes: Uint8Array } | null {
  let offset = 0
  while (offset + 30 <= bytes.length) {
    const signature = readUInt32LE(bytes, offset)
    if (signature !== 0x04034b50) break
    const compressionMethod = readUInt16LE(bytes, offset + 8)
    const compressedSize = readUInt32LE(bytes, offset + 18)
    const fileNameLength = readUInt16LE(bytes, offset + 26)
    const extraLength = readUInt16LE(bytes, offset + 28)
    const nameStart = offset + 30
    const nameEnd = nameStart + fileNameLength
    const dataStart = nameEnd + extraLength
    const dataEnd = dataStart + compressedSize
    if (dataEnd > bytes.length) return null

    const name = new TextDecoder('utf-8').decode(bytes.slice(nameStart, nameEnd))
    const payload = bytes.slice(dataStart, dataEnd)
    if (!name.endsWith('/')) {
      if (compressionMethod === 0) return { name, bytes: payload }
      if (compressionMethod === 8) return { name, bytes: new Uint8Array(inflateRawSync(payload)) }
      return null
    }

    offset = dataEnd
  }
  return null
}

function extractReportText(bytes: Uint8Array): {
  text: string
  archiveKind: 'plain' | 'gzip' | 'zip' | 'unknown'
  extractedEntryName: string
  extractedBytes: number
} {
  if (bytes.length === 0) {
    return { text: '', archiveKind: 'plain', extractedEntryName: '', extractedBytes: 0 }
  }

  if (bytesStartWith(bytes, [0x1f, 0x8b])) {
    const unpacked = new Uint8Array(gunzipSync(bytes))
    return {
      text: decodeCsvBytes(unpacked),
      archiveKind: 'gzip',
      extractedEntryName: '',
      extractedBytes: unpacked.length,
    }
  }

  if (bytesStartWith(bytes, [0x50, 0x4b, 0x03, 0x04])) {
    const firstEntry = extractFirstZipEntry(bytes)
    if (firstEntry) {
      return {
        text: decodeCsvBytes(firstEntry.bytes),
        archiveKind: 'zip',
        extractedEntryName: firstEntry.name,
        extractedBytes: firstEntry.bytes.length,
      }
    }
    return {
      text: '',
      archiveKind: 'zip',
      extractedEntryName: '',
      extractedBytes: 0,
    }
  }

  return {
    text: decodeCsvBytes(bytes),
    archiveKind: 'plain',
    extractedEntryName: '',
    extractedBytes: bytes.length,
  }
}

function detectCsvDelimiter(line: string): string {
  const candidates = [';', ',', '\t']
  let best = ';'
  let bestScore = -1
  for (const delimiter of candidates) {
    let score = 0
    let inQuotes = false
    for (const ch of line) {
      if (ch === '"') inQuotes = !inQuotes
      else if (ch === delimiter && !inQuotes) score += 1
    }
    if (score > bestScore) {
      bestScore = score
      best = delimiter
    }
  }
  return best
}

function parseCsv(textRaw: string): { headers: string[]; rows: Array<Record<string, string>> } {
  const text = textRaw.replace(/^\uFEFF/, '')
  const firstLine = text.split(/\r?\n/, 1)[0] ?? ''
  const delimiter = detectCsvDelimiter(firstLine)

  const rows: string[][] = []
  let field = ''
  let row: string[] = []
  let inQuotes = false

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i]
    const next = text[i + 1]

    if (ch === '"') {
      if (inQuotes && next === '"') {
        field += '"'
        i += 1
      } else {
        inQuotes = !inQuotes
      }
      continue
    }

    if (!inQuotes && ch === delimiter) {
      row.push(field)
      field = ''
      continue
    }

    if (!inQuotes && (ch === '\n' || ch === '\r')) {
      if (ch === '\r' && next === '\n') i += 1
      row.push(field)
      rows.push(row)
      row = []
      field = ''
      continue
    }

    field += ch
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field)
    rows.push(row)
  }

  if (rows.length === 0) return { headers: [], rows: [] }

  const headers = rows[0].map((value, index) => value.trim() || `col_${index}`)
  const out: Array<Record<string, string>> = []
  for (const rawRow of rows.slice(1)) {
    const obj: Record<string, string> = {}
    let hasValue = false
    for (let index = 0; index < headers.length; index += 1) {
      const header = headers[index] ?? `col_${index}`
      const value = String(rawRow[index] ?? '').trim()
      obj[header] = value
      if (value) hasValue = true
    }
    if (hasValue) out.push(obj)
  }
  return { headers, rows: out }
}

function normalizeHeaderKey(value: unknown): string {
  return text(value)
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[()]/g, ' ')
    .replace(/[^a-zа-я0-9]+/gi, '_')
    .replace(/^_+|_+$/g, '')
}

function pickRowValue(row: Record<string, string>, aliases: string[]): string {
  const normalizedRow = new Map<string, string>()
  for (const [key, value] of Object.entries(row)) {
    normalizedRow.set(normalizeHeaderKey(key), text(value))
  }

  for (const alias of aliases) {
    const direct = normalizedRow.get(normalizeHeaderKey(alias))
    if (direct) return direct
  }
  return ''
}

function toUtcIsoFromMoscowParts(year: number, month: number, day: number, hour: number, minute: number, second: number) {
  const localMillis = Date.UTC(year, month - 1, day, hour, minute, second, 0)
  return new Date(localMillis - CSV_MOSCOW_OFFSET_MS).toISOString()
}

function parseOzonLocalDateToIso(value: unknown): string {
  const raw = text(value)
  if (!raw) return ''

  const isoParsed = new Date(raw)
  if (!Number.isNaN(isoParsed.getTime()) && /(z|[+-]\d{2}:?\d{2})$/i.test(raw)) {
    return isoParsed.toISOString()
  }

  const dotMatch = raw.match(/^(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/)
  if (dotMatch) {
    const day = Number(dotMatch[1])
    const month = Number(dotMatch[2])
    const year = Number(dotMatch[3])
    const hour = Number(dotMatch[4] ?? '0')
    const minute = Number(dotMatch[5] ?? '0')
    const second = Number(dotMatch[6] ?? '0')
    return toUtcIsoFromMoscowParts(year, month, day, hour, minute, second)
  }

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/)
  if (isoMatch) {
    const year = Number(isoMatch[1])
    const month = Number(isoMatch[2])
    const day = Number(isoMatch[3])
    const hour = Number(isoMatch[4] ?? '0')
    const minute = Number(isoMatch[5] ?? '0')
    const second = Number(isoMatch[6] ?? '0')
    return toUtcIsoFromMoscowParts(year, month, day, hour, minute, second)
  }

  if (!Number.isNaN(isoParsed.getTime())) return isoParsed.toISOString()
  return ''
}


function parseCsvNumber(value: unknown): number | '' {
  const raw = text(value).replace(/\s+/g, '').replace(',', '.')
  if (!raw) return ''
  const parsed = Number(raw)
  return Number.isFinite(parsed) ? parsed : ''
}

function normalizeDeliverySchema(value: unknown): string {
  const compact = text(value).toLowerCase().replace(/[^a-z]/g, '')
  if (!compact) return ''
  if (compact.includes('rfbs')) return 'rFBS'
  if (compact.includes('fbo')) return 'FBO'
  if (compact.includes('fbs')) return 'FBS'
  return text(value)
}

function buildCsvHeaderSample(rows: Array<Record<string, string>>, limit = 12): string[] {
  const out: string[] = []
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      const normalized = text(key)
      if (!normalized || out.includes(normalized)) continue
      out.push(normalized)
      if (out.length >= limit) return out
    }
  }
  return out
}

function uniqueSample(values: unknown[], limit = 10): string[] {
  const out: string[] = []
  for (const value of values) {
    const normalized = text(value)
    if (!normalized || out.includes(normalized)) continue
    out.push(normalized)
    if (out.length >= limit) break
  }
  return out
}

function mapCsvRowToSalesReportRow(row: Record<string, string>): SalesPostingsReportRow | null {
  const postingNumber = pickRowValue(row, ['Номер отправления', 'Отправление', 'posting_number', 'posting number'])
  if (!postingNumber) return null

  const orderNumber = pickRowValue(row, ['Номер заказа', 'order_number', 'order number'])
  const deliverySchema = normalizeDeliverySchema(pickRowValue(row, ['Метод доставки', 'Схема доставки', 'Тип доставки', 'delivery_schema', 'delivery schema']))
  const shipmentDate = parseOzonLocalDateToIso(pickRowValue(row, [
    'Фактическая дата передачи в доставку',
    'Фактическая дата передачи в доставку (МСК)',
    'Дата и время фактической передачи в доставку',
    'Фактическая дата отгрузки',
    'Фактическая дата передачи отправления в доставку',
    'shipment_date_actual',
    'shipment_date_fact',
    'shipment_date',
    'shipment date actual',
    'shipment date',
    'Дата отгрузки',
  ]))
  const shipmentWarehouse = pickRowValue(row, [
    'Склад отгрузки',
    'Склад отправления',
    'warehouse_from',
    'shipment_warehouse',
    'shipment warehouse',
  ])
  const shipmentCluster = pickRowValue(row, [
    'Кластер отгрузки',
    'Кластер отправления',
    'cluster_from',
    'shipment_cluster',
    'shipment cluster',
  ])
  const shipmentOrigin = deliverySchema === 'FBO'
    ? shipmentCluster
    : shipmentWarehouse
  const deliveryDate = parseOzonLocalDateToIso(pickRowValue(row, ['Дата доставки', 'delivery_date', 'delivery date']))
  const status = pickRowValue(row, [
    'Статус',
    'Статус отправления',
    'Статус заказа',
    'Статус доставки',
    'posting_status',
    'order_status',
    'status',
  ])
  const sku = pickRowValue(row, ['SKU', 'sku'])
  const offerId = pickRowValue(row, ['Артикул', 'offer_id', 'offer id'])
  const productName = pickRowValue(row, ['Название товара', 'Наименование товара', 'product_name', 'product name'])
  const inProcessAt = parseOzonLocalDateToIso(pickRowValue(row, [
    'Принят в обработку',
    'Принят в обработку (МСК)',
    'Дата принятия в обработку',
    'Дата и время принятия в обработку',
    'in_process_at',
    'accepted_at',
    'accepted at',
  ]))
  const price = parseCsvNumber(pickRowValue(row, ['Ваша цена', 'price', 'seller_price']))
  const quantity = parseCsvNumber(pickRowValue(row, ['Количество', 'quantity', 'qty']))
  const paidByCustomer = parseCsvNumber(pickRowValue(row, ['Оплачено покупателем', 'client_price', 'paid_by_customer']))

  return {
    posting_number: postingNumber,
    order_number: orderNumber,
    delivery_schema: deliverySchema,
    shipment_date: shipmentDate,
    shipment_origin: shipmentOrigin,
    delivery_date: deliveryDate,
    status,
    sku,
    offer_id: offerId,
    product_name: productName,
    in_process_at: inProcessAt,
    price,
    quantity,
    paid_by_customer: paidByCustomer,
    raw_row: row,
  }
}

function parseSalesPostingsReportCsv(csvText: string): {
  rows: SalesPostingsReportRow[]
  trace: SalesPostingsReportTrace['csv']
} {
  const parsedCsv = parseCsv(csvText)
  const rawRows = parsedCsv.rows
  const headerNames = parsedCsv.headers
  const out = new Map<string, SalesPostingsReportRow>()

  for (const rawRow of rawRows) {
    const mapped = mapCsvRowToSalesReportRow(rawRow)
    if (!mapped) continue
    const key = buildSalesPostingsReportRowKey(mapped)
    const prev = out.get(key)
    if (!prev || (!prev.shipment_date && mapped.shipment_date) || (!prev.shipment_origin && mapped.shipment_origin) || (!prev.delivery_date && mapped.delivery_date) || (!prev.status && mapped.status)) {
      out.set(key, mapped)
    }
  }

  const rows = Array.from(out.values())
  return {
    rows,
    trace: {
      rowsRaw: rawRows.length,
      rowsMapped: rows.length,
      rowsWithPostingNumber: rows.filter((row) => Boolean(row.posting_number)).length,
      rowsWithShipmentDate: rows.filter((row) => Boolean(row.shipment_date)).length,
      rowsWithShipmentOrigin: rows.filter((row) => Boolean(text(row.shipment_origin))).length,
      rowsWithShipmentWarehouse: rawRows.filter((row) => Boolean(text(pickRowValue(row, [
        'Склад отгрузки',
        'Склад отправления',
        'warehouse_from',
        'shipment_warehouse',
        'shipment warehouse',
      ])))).length,
      rowsWithShipmentCluster: rawRows.filter((row) => Boolean(text(pickRowValue(row, [
        'Кластер отгрузки',
        'Кластер отправления',
        'cluster_from',
        'shipment_cluster',
        'shipment cluster',
      ])))).length,
      rowsWithDeliveryDate: rows.filter((row) => Boolean(row.delivery_date)).length,
      rowsWithStatus: rows.filter((row) => Boolean(text(row.status))).length,
      rowsFbo: rows.filter((row) => normalizeDeliverySchema(row.delivery_schema) === 'FBO').length,
      rowsFboWithShipmentDate: rows.filter((row) => normalizeDeliverySchema(row.delivery_schema) === 'FBO' && Boolean(row.shipment_date)).length,
      rowsFboWithShipmentOrigin: rows.filter((row) => normalizeDeliverySchema(row.delivery_schema) === 'FBO' && Boolean(text(row.shipment_origin))).length,
      rowsFboWithShipmentCluster: rawRows.filter((row) => normalizeDeliverySchema(pickRowValue(row, ['Метод доставки', 'Схема доставки', 'Тип доставки', 'delivery_schema', 'delivery schema'])) === 'FBO' && Boolean(text(pickRowValue(row, [
        'Кластер отгрузки',
        'Кластер отправления',
        'cluster_from',
        'shipment_cluster',
        'shipment cluster',
      ])))).length,
      rowsFboWithDeliveryDate: rows.filter((row) => normalizeDeliverySchema(row.delivery_schema) === 'FBO' && Boolean(row.delivery_date)).length,
      rowsFboWithStatus: rows.filter((row) => normalizeDeliverySchema(row.delivery_schema) === 'FBO' && Boolean(text(row.status))).length,
      rowsFbs: rows.filter((row) => ['FBS', 'rFBS'].includes(normalizeDeliverySchema(row.delivery_schema))).length,
      rowsFbsWithShipmentOrigin: rows.filter((row) => ['FBS', 'rFBS'].includes(normalizeDeliverySchema(row.delivery_schema)) && Boolean(text(row.shipment_origin))).length,
      rowsFbsWithShipmentWarehouse: rawRows.filter((row) => ['FBS', 'rFBS'].includes(normalizeDeliverySchema(pickRowValue(row, ['Метод доставки', 'Схема доставки', 'Тип доставки', 'delivery_schema', 'delivery schema']))) && Boolean(text(pickRowValue(row, [
        'Склад отгрузки',
        'Склад отправления',
        'warehouse_from',
        'shipment_warehouse',
        'shipment warehouse',
      ])))).length,
      rowsFbsWithDeliveryDate: rows.filter((row) => ['FBS', 'rFBS'].includes(normalizeDeliverySchema(row.delivery_schema)) && Boolean(row.delivery_date)).length,
      rowsFbsWithStatus: rows.filter((row) => ['FBS', 'rFBS'].includes(normalizeDeliverySchema(row.delivery_schema)) && Boolean(text(row.status))).length,
      headerCount: headerNames.length,
      headerNames: headerNames.slice(0, 200),
      headerSample: headerNames.slice(0, 20),
      samplePostingNumbers: uniqueSample(rows.map((row) => row.posting_number), 10),
      sampleShipmentDates: uniqueSample(rows.filter((row) => row.shipment_date).map((row) => row.shipment_date), 10),
      sampleShipmentOrigins: uniqueSample(rows.filter((row) => text(row.shipment_origin)).map((row) => row.shipment_origin), 10),
      sampleShipmentWarehouses: uniqueSample(rawRows.map((row) => pickRowValue(row, [
        'Склад отгрузки',
        'Склад отправления',
        'warehouse_from',
        'shipment_warehouse',
        'shipment warehouse',
      ])), 10),
      sampleShipmentClusters: uniqueSample(rawRows.map((row) => pickRowValue(row, [
        'Кластер отгрузки',
        'Кластер отправления',
        'cluster_from',
        'shipment_cluster',
        'shipment cluster',
      ])), 10),
      sampleDeliveryDates: uniqueSample(rows.filter((row) => row.delivery_date).map((row) => row.delivery_date), 10),
      sampleStatuses: uniqueSample(rows.filter((row) => text(row.status)).map((row) => row.status), 10),
    },
  }
}

async function fetchSingleSalesPostingsReportRows(
  secrets: Secrets,
  period: ReportPeriodInput | null | undefined,
  deliverySchema: 'fbo' | 'fbs',
): Promise<{ reportCode: string; fileUrl: string; rows: SalesPostingsReportRow[]; trace: { createBody: Record<string, unknown>; pollAttempts: ReportPollAttempt[]; download: ReportDownloadTrace; csv: ReportCsvTrace }; artifact: SalesPostingsReportDownloadArtifact }> {
  const createBody = buildPostingsReportCreateBody(period, deliverySchema)
  const createResponse = await ozonReportPostingsCreate(secrets, createBody)
  const createResult = (createResponse && typeof createResponse === 'object' && 'result' in createResponse)
    ? (createResponse as any).result
    : createResponse
  const reportCode = text((createResult as any)?.code)
  if (!reportCode) throw new Error('Ozon report error: empty report code for /v1/report/postings/create')

  let fileUrl = ''
  let lastStatus = ''
  let lastError = ''
  const pollAttempts: ReportPollAttempt[] = []

  for (let attempt = 0; attempt < REPORT_INFO_POLL_ATTEMPTS; attempt += 1) {
    const infoResponse = await ozonReportInfo(secrets, reportCode)
    const infoResult = (infoResponse && typeof infoResponse === 'object' && 'result' in infoResponse)
      ? (infoResponse as any).result
      : infoResponse

    lastStatus = normalizeReportStatus((infoResult as any)?.status)
    lastError = text((infoResult as any)?.error)
    fileUrl = text((infoResult as any)?.file)
    pollAttempts.push({ attempt: attempt + 1, status: lastStatus || 'unknown', hasFile: Boolean(fileUrl), error: lastError })

    if (fileUrl && (looksLikeReadyReportStatus(lastStatus) || !lastStatus)) break
    if (looksLikeFailedReportStatus(lastStatus)) {
      throw new Error(`Ozon report error: ${lastStatus}${lastError ? ` (${lastError})` : ''}`)
    }
    await sleep(REPORT_INFO_POLL_DELAY_MS)
  }

  if (!fileUrl) {
    throw new Error(`Ozon report error: report file is not ready for code ${reportCode}${lastStatus ? ` (${lastStatus})` : ''}`)
  }

  const downloaded = await ozonDownloadReportFile(fileUrl)
  const extracted = extractReportText(downloaded.bytes)
  const parsed = parseSalesPostingsReportCsv(extracted.text)
  const artifactBounds = resolvePeriodBounds(period)

  return {
    reportCode,
    fileUrl,
    rows: parsed.rows,
    trace: {
      createBody: createBody as Record<string, unknown>,
      pollAttempts,
      download: {
        contentType: downloaded.contentType,
        contentEncoding: downloaded.contentEncoding,
        bytes: downloaded.bytes.length,
        archiveKind: extracted.archiveKind,
        extractedEntryName: extracted.extractedEntryName,
        extractedBytes: extracted.extractedBytes,
      },
      csv: parsed.trace,
    },
    artifact: {
      schema: deliverySchema,
      label: `${deliverySchema}:${artifactBounds.from}..${artifactBounds.to}`,
      from: artifactBounds.from,
      to: artifactBounds.to,
      reportCode,
      fileUrl,
      archiveKind: extracted.archiveKind,
      extractedEntryName: extracted.extractedEntryName,
      csvText: extracted.text,
      headerNames: parsed.trace.headerNames,
    },
  }
}

function buildSalesPostingsReportRowKey(row: SalesPostingsReportRow): string {
  return [
    text(row?.delivery_schema).toUpperCase(),
    text(row?.posting_number),
    text(row?.sku),
    text(row?.offer_id),
    text(row?.product_name),
  ].join('|')
}

function mergeSalesPostingsReportRows(rows: SalesPostingsReportRow[]): SalesPostingsReportRow[] {
  const out = new Map<string, SalesPostingsReportRow>()
  for (const row of rows) {
    const postingNumber = text(row?.posting_number)
    if (!postingNumber) continue
    const key = buildSalesPostingsReportRowKey(row)
    const prev = out.get(key)
    if (!prev) {
      out.set(key, row)
      continue
    }

    const prevShipmentDate = text(prev.shipment_date)
    const nextShipmentDate = text(row.shipment_date)
    if (!prevShipmentDate && nextShipmentDate) {
      out.set(key, row)
      continue
    }
    if (prevShipmentDate && nextShipmentDate && nextShipmentDate > prevShipmentDate) {
      out.set(key, row)
    }
  }
  return Array.from(out.values())
}

function buildAggregateCsvTrace(rows: SalesPostingsReportRow[], segments: SalesPostingsReportTraceSegment[]): ReportCsvTrace {
  const headerNames: string[] = []
  const headerSample: string[] = []
  const samplePostingNumbers: string[] = []
  const sampleShipmentDates: string[] = []
  const sampleShipmentOrigins: string[] = []
  const sampleShipmentWarehouses: string[] = []
  const sampleShipmentClusters: string[] = []
  const sampleDeliveryDates: string[] = []
  const sampleStatuses: string[] = []
  let rowsRaw = 0
  let rowsMapped = 0
  let rowsWithPostingNumber = 0
  let rowsWithShipmentDate = 0
  let rowsWithShipmentOrigin = 0
  let rowsWithShipmentWarehouse = 0
  let rowsWithShipmentCluster = 0
  let rowsWithDeliveryDate = 0
  let rowsWithStatus = 0
  let rowsFbo = 0
  let rowsFboWithShipmentDate = 0
  let rowsFboWithShipmentOrigin = 0
  let rowsFboWithShipmentCluster = 0
  let rowsFboWithDeliveryDate = 0
  let rowsFboWithStatus = 0
  let rowsFbs = 0
  let rowsFbsWithShipmentOrigin = 0
  let rowsFbsWithShipmentWarehouse = 0
  let rowsFbsWithDeliveryDate = 0
  let rowsFbsWithStatus = 0

  for (const segment of segments) {
    if (segment.csv) {
      rowsRaw += Number(segment.csv.rowsRaw ?? 0)
      rowsMapped += Number(segment.csv.rowsMapped ?? 0)
      rowsWithPostingNumber += Number(segment.csv.rowsWithPostingNumber ?? 0)
      rowsWithShipmentDate += Number(segment.csv.rowsWithShipmentDate ?? 0)
      rowsWithShipmentOrigin += Number(segment.csv.rowsWithShipmentOrigin ?? 0)
      rowsWithShipmentWarehouse += Number(segment.csv.rowsWithShipmentWarehouse ?? 0)
      rowsWithShipmentCluster += Number(segment.csv.rowsWithShipmentCluster ?? 0)
      rowsWithDeliveryDate += Number(segment.csv.rowsWithDeliveryDate ?? 0)
      rowsWithStatus += Number(segment.csv.rowsWithStatus ?? 0)
      rowsFbo += Number(segment.csv.rowsFbo ?? 0)
      rowsFboWithShipmentDate += Number(segment.csv.rowsFboWithShipmentDate ?? 0)
      rowsFboWithShipmentOrigin += Number(segment.csv.rowsFboWithShipmentOrigin ?? 0)
      rowsFboWithShipmentCluster += Number(segment.csv.rowsFboWithShipmentCluster ?? 0)
      rowsFboWithDeliveryDate += Number(segment.csv.rowsFboWithDeliveryDate ?? 0)
      rowsFboWithStatus += Number(segment.csv.rowsFboWithStatus ?? 0)
      rowsFbs += Number(segment.csv.rowsFbs ?? 0)
      rowsFbsWithShipmentOrigin += Number(segment.csv.rowsFbsWithShipmentOrigin ?? 0)
      rowsFbsWithShipmentWarehouse += Number(segment.csv.rowsFbsWithShipmentWarehouse ?? 0)
      rowsFbsWithDeliveryDate += Number(segment.csv.rowsFbsWithDeliveryDate ?? 0)
      rowsFbsWithStatus += Number(segment.csv.rowsFbsWithStatus ?? 0)
      for (const key of segment.csv.headerNames ?? []) {
        if (key && !headerNames.includes(key) && headerNames.length < 200) headerNames.push(key)
      }
      for (const key of segment.csv.headerSample ?? []) {
        if (key && !headerSample.includes(key) && headerSample.length < 20) headerSample.push(key)
      }
    }
    if (!samplePostingNumbers.length || samplePostingNumbers.length < 10) {
      for (const item of rows) {
        const value = text(item.posting_number)
        if (value && !samplePostingNumbers.includes(value)) samplePostingNumbers.push(value)
        if (samplePostingNumbers.length >= 10) break
      }
    }
    if (!sampleShipmentDates.length || sampleShipmentDates.length < 10) {
      for (const item of rows) {
        const value = text(item.shipment_date)
        if (value && !sampleShipmentDates.includes(value)) sampleShipmentDates.push(value)
        if (sampleShipmentDates.length >= 10) break
      }
    }
    if (!sampleShipmentOrigins.length || sampleShipmentOrigins.length < 10) {
      for (const item of rows) {
        const value = text(item.shipment_origin)
        if (value && !sampleShipmentOrigins.includes(value)) sampleShipmentOrigins.push(value)
        if (sampleShipmentOrigins.length >= 10) break
      }
    }
    if (segment.csv && (!sampleShipmentWarehouses.length || sampleShipmentWarehouses.length < 10)) {
      for (const value of segment.csv.sampleShipmentWarehouses ?? []) {
        const normalized = text(value)
        if (normalized && !sampleShipmentWarehouses.includes(normalized)) sampleShipmentWarehouses.push(normalized)
        if (sampleShipmentWarehouses.length >= 10) break
      }
    }
    if (segment.csv && (!sampleShipmentClusters.length || sampleShipmentClusters.length < 10)) {
      for (const value of segment.csv.sampleShipmentClusters ?? []) {
        const normalized = text(value)
        if (normalized && !sampleShipmentClusters.includes(normalized)) sampleShipmentClusters.push(normalized)
        if (sampleShipmentClusters.length >= 10) break
      }
    }
    if (!sampleDeliveryDates.length || sampleDeliveryDates.length < 10) {
      for (const item of rows) {
        const value = text(item.delivery_date)
        if (value && !sampleDeliveryDates.includes(value)) sampleDeliveryDates.push(value)
        if (sampleDeliveryDates.length >= 10) break
      }
    }
    if (!sampleStatuses.length || sampleStatuses.length < 10) {
      for (const item of rows) {
        const value = text(item.status)
        if (value && !sampleStatuses.includes(value)) sampleStatuses.push(value)
        if (sampleStatuses.length >= 10) break
      }
    }
  }

  return {
    rowsRaw,
    rowsMapped,
    rowsWithPostingNumber,
    rowsWithShipmentDate,
    rowsWithShipmentOrigin,
    rowsWithShipmentWarehouse,
    rowsWithShipmentCluster,
    rowsWithDeliveryDate,
    rowsWithStatus,
    rowsFbo,
    rowsFboWithShipmentDate,
    rowsFboWithShipmentOrigin,
    rowsFboWithShipmentCluster,
    rowsFboWithDeliveryDate,
    rowsFboWithStatus,
    rowsFbs,
    rowsFbsWithShipmentOrigin,
    rowsFbsWithShipmentWarehouse,
    rowsFbsWithDeliveryDate,
    rowsFbsWithStatus,
    headerCount: headerNames.length,
    headerNames,
    headerSample,
    samplePostingNumbers,
    sampleShipmentDates,
    sampleShipmentOrigins,
    sampleShipmentWarehouses,
    sampleShipmentClusters,
    sampleDeliveryDates,
    sampleStatuses,
  }
}

function buildAggregateDownloadTrace(segments: SalesPostingsReportTraceSegment[]): ReportDownloadTrace {
  const successful = segments.filter((segment) => !segment.error && segment.download)
  if (successful.length === 0) {
    return {
      contentType: '',
      contentEncoding: '',
      bytes: 0,
      archiveKind: 'unknown',
      extractedEntryName: '',
      extractedBytes: 0,
    }
  }

  if (successful.length === 1 && successful[0].download) return successful[0].download

  return {
    contentType: 'multiple',
    contentEncoding: 'multiple',
    bytes: successful.reduce((sum, segment) => sum + Number(segment.download?.bytes ?? 0), 0),
    archiveKind: 'unknown',
    extractedEntryName: successful.map((segment) => segment.download?.extractedEntryName).filter(Boolean).join(', '),
    extractedBytes: successful.reduce((sum, segment) => sum + Number(segment.download?.extractedBytes ?? 0), 0),
  }
}

async function fetchSalesPostingsReportRowsForSchema(
  secrets: Secrets,
  period: ReportPeriodInput | null | undefined,
  deliverySchema: 'fbo' | 'fbs',
): Promise<{ reportCode: string; fileUrl: string; rows: SalesPostingsReportRow[]; trace: SalesPostingsReportTrace; downloads: SalesPostingsReportDownloadArtifact[] }> {
  const bounds = resolvePeriodBounds(period)
  const totalDays = diffDaysInclusive(bounds.from, bounds.to)
  const preferChunked = totalDays > DEFAULT_REPORT_LOOKBACK_DAYS
  const strategies: Array<{ name: 'single' | 'chunked-7d' | 'chunked-1d'; segments: Array<{ from: string; to: string; label: string }> }> = preferChunked
    ? []
    : [{ name: 'single', segments: [{ from: bounds.from, to: bounds.to, label: `${bounds.from}..${bounds.to}` }] }]
  if (totalDays > 1) {
    strategies.push({ name: 'chunked-7d', segments: splitPeriodIntoSegments(bounds, REPORT_PRIMARY_CHUNK_DAYS) })
    strategies.push({ name: 'chunked-1d', segments: splitPeriodIntoSegments(bounds, REPORT_FALLBACK_CHUNK_DAYS) })
  }
  if (preferChunked) {
    strategies.push({ name: 'single', segments: [{ from: bounds.from, to: bounds.to, label: `${bounds.from}..${bounds.to}` }] })
  }

  let lastError: unknown = null

  for (const strategy of strategies) {
    const segmentTraces: SalesPostingsReportTraceSegment[] = []
    const collectedRows: SalesPostingsReportRow[] = []
    const downloads: SalesPostingsReportDownloadArtifact[] = []

    for (const segment of strategy.segments) {
      try {
        const single = await fetchSingleSalesPostingsReportRows(secrets, segment, deliverySchema)
        collectedRows.push(...single.rows)
        downloads.push(single.artifact)
        segmentTraces.push({
          label: `${deliverySchema}:${segment.label}`,
          from: segment.from,
          to: segment.to,
          reportCode: single.reportCode,
          fileUrl: single.fileUrl,
          createBody: single.trace.createBody,
          pollAttempts: single.trace.pollAttempts,
          download: single.trace.download,
          csv: single.trace.csv,
          rows: single.rows.length,
          rowsWithShipmentDate: Number(single.trace.csv.rowsWithShipmentDate ?? 0),
          error: '',
        })
      } catch (error) {
        lastError = error
        segmentTraces.push({
          label: `${deliverySchema}:${segment.label}`,
          from: segment.from,
          to: segment.to,
          reportCode: '',
          fileUrl: '',
          createBody: buildPostingsReportCreateBody(segment, deliverySchema) as Record<string, unknown>,
          pollAttempts: [],
          download: null,
          csv: null,
          rows: 0,
          rowsWithShipmentDate: 0,
          error: text((error as any)?.message ?? error),
        })

        if (strategy.name === 'single' && shouldFallbackToChunkedReports(error)) {
          break
        }
        if (strategy.name === 'single') {
          break
        }
      }
    }

    const mergedRows = mergeSalesPostingsReportRows(collectedRows)
    if (mergedRows.length > 0) {
      const firstSuccess = segmentTraces.find((segment) => !segment.error)
      const partial = segmentTraces.some((segment) => Boolean(segment.error))
      return {
        reportCode: firstSuccess?.reportCode ?? '',
        fileUrl: firstSuccess?.fileUrl ?? '',
        rows: mergedRows,
        trace: {
          reportCode: firstSuccess?.reportCode ?? '',
          fileUrl: firstSuccess?.fileUrl ?? '',
          createBody: (firstSuccess?.createBody ?? {}) as Record<string, unknown>,
          pollAttempts: firstSuccess?.pollAttempts ?? [],
          download: buildAggregateDownloadTrace(segmentTraces),
          csv: buildAggregateCsvTrace(mergedRows, segmentTraces),
          strategy: strategy.name,
          partial,
          segments: segmentTraces,
        },
        downloads,
      }
    }
  }

  if (lastError instanceof Error) throw lastError
  throw new Error(`Ozon report error: postings report did not return any rows for ${deliverySchema}`)
}

export async function fetchSalesPostingsReportRows(
  secrets: Secrets,
  period: ReportPeriodInput | null | undefined,
): Promise<{ reportCode: string; fileUrl: string; rows: SalesPostingsReportRow[]; trace: SalesPostingsReportTrace; downloads: SalesPostingsReportDownloadArtifact[] }> {
  const schemas: Array<'fbo' | 'fbs'> = ['fbo', 'fbs']
  const allRows: SalesPostingsReportRow[] = []
  const allSegments: SalesPostingsReportTraceSegment[] = []
  const allDownloads: SalesPostingsReportDownloadArtifact[] = []
  let firstSuccess: { reportCode: string; fileUrl: string; trace: SalesPostingsReportTrace } | null = null
  let hasPartial = false
  let lastError: unknown = null

  for (const schema of schemas) {
    try {
      const result = await fetchSalesPostingsReportRowsForSchema(secrets, period, schema)
      allRows.push(...result.rows)
      allSegments.push(...(Array.isArray(result.trace?.segments) ? result.trace.segments : []))
      allDownloads.push(...(Array.isArray(result.downloads) ? result.downloads : []))
      if (!firstSuccess) firstSuccess = result
      if (result.trace?.partial) hasPartial = true
    } catch (error) {
      lastError = error
      hasPartial = true
      allSegments.push({
        label: `${schema}:failed`,
        from: resolvePeriodBounds(period).from,
        to: resolvePeriodBounds(period).to,
        reportCode: '',
        fileUrl: '',
        createBody: buildPostingsReportCreateBody(period, schema) as Record<string, unknown>,
        pollAttempts: [],
        download: null,
        csv: null,
        rows: 0,
        rowsWithShipmentDate: 0,
        error: text((error as any)?.message ?? error),
      })
    }
  }

  const mergedRows = mergeSalesPostingsReportRows(allRows)
  if (mergedRows.length > 0 && firstSuccess) {
    return {
      reportCode: firstSuccess.reportCode,
      fileUrl: firstSuccess.fileUrl,
      rows: mergedRows,
      trace: {
        reportCode: firstSuccess.reportCode,
        fileUrl: firstSuccess.fileUrl,
        createBody: firstSuccess.trace.createBody,
        pollAttempts: firstSuccess.trace.pollAttempts,
        download: buildAggregateDownloadTrace(allSegments),
        csv: buildAggregateCsvTrace(mergedRows, allSegments),
        strategy: firstSuccess.trace.strategy,
        partial: hasPartial || allSegments.some((segment) => Boolean(segment.error)),
        segments: allSegments,
      },
      downloads: allDownloads,
    }
  }

  if (lastError instanceof Error) throw lastError
  throw new Error('Ozon report error: postings report did not return any rows')
}

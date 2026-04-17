import type { SalesPayloadEnvelope } from '../sales-sync'
import { collectSalesStateEvents, extractPostingsFromPayload, getSalesPostingDetailsKey, resolveFboShipmentDateFromSources } from '../sales-sync'

export type FboPostingsReportRow = {
  posting_number: string
  order_number?: string | null
  delivery_schema?: string | null
  shipment_date?: string | null
  delivery_date?: string | null
  raw_row?: Record<string, string> | null
}

type FboPostingMemoryRow = {
  store_client_id: string
  period_key: string
  posting_number: string
  order_id?: string | null
  related_postings?: string | null
  shipment_date?: string | null
  delivery_date?: string | null
  delivery_cluster?: string | null
  updated_at: string
}

type FboItemMemoryRow = {
  store_client_id: string
  period_key: string
  posting_number: string
  line_no: number
  sku?: string | null
  offer_id?: string | null
  updated_at: string
}

type FboEventMemoryRow = {
  store_client_id: string
  period_key: string
  posting_number: string
  event_key: string
  event_type: string
  new_state: string
  state: string
  changed_state_date: string
  updated_at: string
}

type FboReportMemoryRow = {
  store_client_id: string
  period_key: string
  posting_number: string
  order_number?: string | null
  delivery_schema?: string | null
  shipment_date?: string | null
  delivery_date?: string | null
  raw_row_json?: string | null
  updated_at: string
}

const postingsByKey = new Map<string, FboPostingMemoryRow>()
const itemsByKey = new Map<string, FboItemMemoryRow>()
const eventsByKey = new Map<string, FboEventMemoryRow>()
const reportRowsByKey = new Map<string, FboReportMemoryRow>()

function text(value: any): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function nowIso() {
  return new Date().toISOString()
}

function key(parts: Array<string | number | null | undefined>): string {
  return parts.map((part) => String(part ?? '').trim()).join('::')
}

function getByPath(source: any, path: string) {
  if (!source || typeof source !== 'object') return undefined
  let cur = source
  for (const part of String(path).split('.').filter(Boolean)) {
    if (cur == null || typeof cur !== 'object' || !(part in cur)) return undefined
    cur = cur[part]
  }
  return cur
}

function pick(source: any, paths: string[]): any {
  for (const path of paths) {
    const value = getByPath(source, path)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function pickFromSources(paths: string[], ...sources: any[]): any {
  for (const source of sources) {
    const value = pick(source, paths)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function dateText(value: any): string {
  const raw = text(value)
  if (!raw) return ''
  const dt = new Date(raw)
  return Number.isNaN(dt.getTime()) ? raw : dt.toISOString()
}

function postingNumberOf(source: any): string {
  return text(pick(source, ['posting_number', 'postingNumber']))
}

function orderIdOf(source: any): string {
  return text(pick(source, ['order_id', 'order_number']))
}

const FBO_STATE_CHANGED_EVENT_TYPE = 'type_state_changed'
const FBO_SHIPMENT_PUSH_PERIOD_KEY = '__push__'

const FBO_DELIVERED_STATES = new Set<string>([
  'delivered',
  'delivered_to_customer',
  'customer_received',
  'posting_delivered',
  'posting_delivered_to_customer',
])

function itemsOf(source: any): any[] {
  if (Array.isArray(source?.products)) return source.products
  if (Array.isArray(source?.items)) return source.items
  return []
}

function mergeItem(detailItem: any, postingItem: any) {
  const detailObj = detailItem && typeof detailItem === 'object' ? detailItem : {}
  const postingObj = postingItem && typeof postingItem === 'object' ? postingItem : {}
  return {
    ...postingObj,
    ...detailObj,
    sku: pickFromSources(['sku', 'sku_id', 'id'], detailObj, postingObj),
    offer_id: pickFromSources(['offer_id', 'offerId', 'article'], detailObj, postingObj),
  }
}

function mergedItems(detail: any, posting: any): any[] {
  const detailItems = itemsOf(detail)
  const postingItems = itemsOf(posting)
  if (detailItems.length === 0 && postingItems.length === 0) return []
  const maxLen = Math.max(detailItems.length, postingItems.length)
  const out: any[] = []
  for (let i = 0; i < maxLen; i += 1) out.push(mergeItem(detailItems[i], postingItems[i]))
  return out
}

function relatedOf(detail: any, posting: any, fallback: string[]): string {
  const direct = pickFromSources([
    'related_postings.related_posting_numbers',
    'result.related_postings.related_posting_numbers',
    'related_postings.related_postings',
    'related_postings',
  ], detail, posting)
  if (Array.isArray(direct)) {
    const vals = direct.map((v) => text(v)).filter(Boolean)
    if (vals.length > 0) return vals.join(', ')
  }
  const raw = text(direct)
  if (raw) return raw
  return fallback.join(', ')
}

function pushTraceSample(target: string[], value: string, limit = 10) {
  if (!value || target.includes(value) || target.length >= limit) return
  target.push(value)
}

function safeJsonStringify(value: unknown): string | null {
  if (value == null) return null
  try { return JSON.stringify(value) } catch { return null }
}

function mergePreferIncoming<T>(incoming: T, existing: T): T {
  const incomingText = typeof incoming === 'string' ? incoming.trim() : incoming
  if (incomingText !== null && incomingText !== undefined && incomingText !== '' && (!(Array.isArray(incomingText)) || incomingText.length > 0)) return incoming as T
  return existing as T
}

function buildFboReportShipmentDateMap(rows: FboPostingsReportRow[] | null | undefined): Map<string, string> {
  const out = new Map<string, string>()
  for (const row of Array.isArray(rows) ? rows : []) {
    const deliverySchema = text(row?.delivery_schema).toLowerCase().replace(/[^a-z]/g, '')
    if (deliverySchema && deliverySchema !== 'fbo') continue
    const postingNumber = text(row?.posting_number)
    const shipmentDate = dateText(row?.shipment_date)
    if (!postingNumber || !shipmentDate) continue
    const prev = text(out.get(postingNumber))
    if (!prev || shipmentDate > prev) out.set(postingNumber, shipmentDate)
  }
  return out
}

function buildFboReportDeliveryDateMap(rows: FboPostingsReportRow[] | null | undefined): Map<string, string> {
  const out = new Map<string, string>()
  for (const row of Array.isArray(rows) ? rows : []) {
    const deliverySchema = text(row?.delivery_schema).toLowerCase().replace(/[^a-z]/g, '')
    if (deliverySchema && deliverySchema !== 'fbo') continue
    const postingNumber = text(row?.posting_number)
    const deliveryDate = dateText(row?.delivery_date)
    if (!postingNumber || !deliveryDate) continue
    const prev = text(out.get(postingNumber))
    if (!prev || deliveryDate > prev) out.set(postingNumber, deliveryDate)
  }
  return out
}

function getPersistedShipmentDateForPosting(storeClientId: string, postingNumber: string, periodKey: string): string {
  let best = ''
  for (const event of eventsByKey.values()) {
    if (event.store_client_id !== storeClientId) continue
    if (event.posting_number !== postingNumber) continue
    if (![periodKey, FBO_SHIPMENT_PUSH_PERIOD_KEY].includes(event.period_key)) continue
    if (text(event.event_type) !== FBO_STATE_CHANGED_EVENT_TYPE) continue
    if (text(event.new_state || event.state) !== 'posting_transferring_to_delivery') continue
    if (!best || event.changed_state_date > best) best = event.changed_state_date
  }
  return best
}

function shipmentDateOf(detail: any, posting: any): string {
  return resolveFboShipmentDateFromSources(detail, posting)
}

function hasDeliveredStateOf(...sources: any[]): boolean {
  for (const source of sources) {
    const state = text(pick(source, [
      'status',
      'result.status',
      'provider_status',
      'result.provider_status',
      'new_state',
      'result.new_state',
      'state',
      'result.state',
    ])).toLowerCase()
    if (state && FBO_DELIVERED_STATES.has(state)) return true
  }
  return false
}

function deliveryDateOf(detail: any, posting: any): string {
  if (!hasDeliveredStateOf(detail, posting)) return ''
  const exact = dateText(pickFromSources(['fact_delivery_date', 'result.fact_delivery_date'], detail, posting))
  if (exact) return exact
  return dateText(pickFromSources([
    'result.customer_deliver_date',
    'customer_deliver_date',
    'result.delivered_at',
    'delivered_at',
    'delivered_date',
    'result.delivered_date',
  ], detail, posting))
}

function deliveryClusterOf(detail: any, posting: any): string {
  return text(pickFromSources([
    'financial_data.cluster_to',
    'result.financial_data.cluster_to',
    'cluster_to',
    'result.cluster_to',
  ], detail, posting))
}

function countPostings(storeClientId: string, periodKey: string): number {
  return Array.from(postingsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === periodKey).length
}

function countItems(storeClientId: string, periodKey: string): number {
  return Array.from(itemsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === periodKey).length
}

function countEvents(storeClientId: string, periodKey: string): number {
  return Array.from(eventsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === periodKey).length
}

export function buildAndPersistFboSalesSnapshot(args: {
  storeClientId: string
  periodKey?: string | null
  fboPayloads: SalesPayloadEnvelope[]
  postingDetailsByKey: Map<string, any>
  reportRows?: FboPostingsReportRow[] | null
  fetchedAt?: string
}) {
  const storeClientId = text(args.storeClientId)
  if (!storeClientId) return {
    postingsCount: 0,
    itemsCount: 0,
    eventsCount: 0,
    trace: {
      postingsSeen: 0,
      postingsWithDetail: 0,
      postingsWithoutDetail: 0,
      postingsWithAnyStateEvents: 0,
      postingsWithShipmentTransferEvent: 0,
      postingsWithResolvedShipmentDate: 0,
      missingDetailPostingNumbers: [] as string[],
      missingShipmentDatePostingNumbers: [] as string[],
      shipmentTransferPostingNumbers: [] as string[],
    },
    persisted: {
      postingsCount: 0,
      itemsCount: 0,
      eventsCount: 0,
      shipmentDateCount: 0,
      deliveryDateCount: 0,
      shipmentTransferEventCount: 0,
    },
  }

  const periodKey = text(args.periodKey)
  const fetchedAt = text(args.fetchedAt) || nowIso()
  const reportShipmentDateByPosting = buildFboReportShipmentDateMap(args.reportRows ?? [])
  const reportDeliveryDateByPosting = buildFboReportDeliveryDateMap(args.reportRows ?? [])
  const orderPostings = new Map<string, Set<string>>()
  const postingRows = new Map<string, FboPostingMemoryRow>()
  const itemRows = new Map<string, FboItemMemoryRow>()
  const eventRows = new Map<string, FboEventMemoryRow>()
  const postingsSeen = new Set<string>()
  const postingsWithDetail = new Set<string>()
  const postingsWithAnyStateEvents = new Set<string>()
  const postingsWithShipmentTransferEvent = new Set<string>()
  const postingsWithResolvedShipmentDate = new Set<string>()
  const missingDetailPostingNumbers: string[] = []
  const missingShipmentDatePostingNumbers: string[] = []
  const shipmentTransferPostingNumbers: string[] = []

  for (const envelope of args.fboPayloads) {
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const orderId = orderIdOf(posting)
      const postingNumber = postingNumberOf(posting)
      if (!orderId || !postingNumber) continue
      let bucket = orderPostings.get(orderId)
      if (!bucket) {
        bucket = new Set<string>()
        orderPostings.set(orderId, bucket)
      }
      bucket.add(postingNumber)
    }
  }

  for (const envelope of args.fboPayloads) {
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const basePostingNumber = postingNumberOf(posting)
      if (!basePostingNumber) continue
      const detail = args.postingDetailsByKey.get(getSalesPostingDetailsKey('FBO', basePostingNumber)) ?? null
      const postingNumber = postingNumberOf(detail) || basePostingNumber
      const orderId = orderIdOf(detail) || orderIdOf(posting)
      const fallback = orderId ? Array.from(orderPostings.get(orderId) ?? []).filter((v) => v !== postingNumber) : []
      const reportShipmentDate = text(reportShipmentDateByPosting.get(postingNumber) ?? reportShipmentDateByPosting.get(basePostingNumber) ?? '')
      const reportDeliveryDate = text(reportDeliveryDateByPosting.get(postingNumber) ?? reportDeliveryDateByPosting.get(basePostingNumber) ?? '')
      const shipmentDate = reportShipmentDate || shipmentDateOf(detail, posting) || getPersistedShipmentDateForPosting(storeClientId, postingNumber, periodKey)
      const stateEvents = collectSalesStateEvents(detail, posting)
      const shipmentTransferEvents = stateEvents.filter((event) => (
        text(event?.event_type) === FBO_STATE_CHANGED_EVENT_TYPE
        && text(event?.new_state || event?.state) === 'posting_transferring_to_delivery'
      ))

      postingsSeen.add(postingNumber)
      if (detail) postingsWithDetail.add(postingNumber)
      else pushTraceSample(missingDetailPostingNumbers, postingNumber)
      if (stateEvents.length > 0) postingsWithAnyStateEvents.add(postingNumber)
      if (shipmentTransferEvents.length > 0) {
        postingsWithShipmentTransferEvent.add(postingNumber)
        pushTraceSample(shipmentTransferPostingNumbers, postingNumber)
      }
      if (shipmentDate) postingsWithResolvedShipmentDate.add(postingNumber)
      else pushTraceSample(missingShipmentDatePostingNumbers, postingNumber)

      postingRows.set(postingNumber, {
        store_client_id: storeClientId,
        period_key: periodKey,
        posting_number: postingNumber,
        order_id: orderId || null,
        related_postings: relatedOf(detail, posting, fallback) || null,
        shipment_date: shipmentDate || null,
        delivery_date: reportDeliveryDate || deliveryDateOf(detail, posting) || null,
        delivery_cluster: deliveryClusterOf(detail, posting) || null,
        updated_at: fetchedAt,
      })

      for (const event of stateEvents) {
        const eventType = text(event.event_type) || FBO_STATE_CHANGED_EVENT_TYPE
        const newState = text(event.new_state) || text(event.state)
        const changedStateDate = text(event.changed_state_date)
        if (!changedStateDate) continue
        const eventKey = `${eventType}|${newState}|${changedStateDate}`
        eventRows.set(`${postingNumber}|${eventKey}`, {
          store_client_id: storeClientId,
          period_key: periodKey,
          posting_number: postingNumber,
          event_key: eventKey,
          event_type: eventType,
          new_state: newState,
          state: newState,
          changed_state_date: changedStateDate,
          updated_at: fetchedAt,
        })
      }

      const items = mergedItems(detail, posting)
      let lineNo = 0
      for (const item of items) {
        const sku = text((item as any)?.sku)
        const offerId = text((item as any)?.offer_id)
        if (!sku && !offerId) continue
        lineNo += 1
        itemRows.set(`${postingNumber}|${lineNo}|${sku}|${offerId}`, {
          store_client_id: storeClientId,
          period_key: periodKey,
          posting_number: postingNumber,
          line_no: lineNo,
          sku: sku || null,
          offer_id: offerId || null,
          updated_at: fetchedAt,
        })
      }
    }
  }

  const incomingPostingKeys = new Set<string>()
  const incomingItemKeys = new Set<string>()
  const incomingEventKeys = new Set<string>()

  for (const row of postingRows.values()) {
    const k = key([row.store_client_id, row.period_key, row.posting_number])
    const existing = postingsByKey.get(k)
    postingsByKey.set(k, {
      ...row,
      shipment_date: mergePreferIncoming(row.shipment_date ?? null, existing?.shipment_date ?? null),
      delivery_date: mergePreferIncoming(row.delivery_date ?? null, existing?.delivery_date ?? null),
      delivery_cluster: mergePreferIncoming(row.delivery_cluster ?? null, existing?.delivery_cluster ?? null),
      related_postings: mergePreferIncoming(row.related_postings ?? null, existing?.related_postings ?? null),
    })
    incomingPostingKeys.add(k)
  }
  for (const row of itemRows.values()) {
    const k = key([row.store_client_id, row.period_key, row.posting_number, row.line_no])
    itemsByKey.set(k, row)
    incomingItemKeys.add(k)
  }
  for (const row of eventRows.values()) {
    const k = key([row.store_client_id, row.period_key, row.posting_number, row.event_key])
    eventsByKey.set(k, row)
    incomingEventKeys.add(k)
  }

  for (const [k, row] of postingsByKey.entries()) {
    if (row.store_client_id === storeClientId && row.period_key === periodKey && !incomingPostingKeys.has(k)) postingsByKey.delete(k)
  }
  for (const [k, row] of itemsByKey.entries()) {
    if (row.store_client_id === storeClientId && row.period_key === periodKey && !incomingItemKeys.has(k)) itemsByKey.delete(k)
  }
  for (const [k, row] of eventsByKey.entries()) {
    if (row.store_client_id === storeClientId && row.period_key === periodKey && !incomingEventKeys.has(k)) eventsByKey.delete(k)
  }

  const persistedRows = Array.from(postingsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === periodKey)
  const persistedEvents = Array.from(eventsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === periodKey)

  return {
    postingsCount: postingRows.size,
    itemsCount: itemRows.size,
    eventsCount: eventRows.size,
    trace: {
      postingsSeen: postingsSeen.size,
      postingsWithDetail: postingsWithDetail.size,
      postingsWithoutDetail: Math.max(0, postingsSeen.size - postingsWithDetail.size),
      postingsWithAnyStateEvents: postingsWithAnyStateEvents.size,
      postingsWithShipmentTransferEvent: postingsWithShipmentTransferEvent.size,
      postingsWithResolvedShipmentDate: postingsWithResolvedShipmentDate.size,
      missingDetailPostingNumbers,
      missingShipmentDatePostingNumbers,
      shipmentTransferPostingNumbers,
    },
    persisted: {
      postingsCount: countPostings(storeClientId, periodKey),
      itemsCount: countItems(storeClientId, periodKey),
      eventsCount: countEvents(storeClientId, periodKey),
      shipmentDateCount: persistedRows.filter((row) => text(row.shipment_date)).length,
      deliveryDateCount: persistedRows.filter((row) => text(row.delivery_date)).length,
      shipmentTransferEventCount: persistedEvents.filter((row) => text(row.event_type) === FBO_STATE_CHANGED_EVENT_TYPE && text(row.new_state || row.state) === 'posting_transferring_to_delivery').length,
    },
  }
}

export function persistFboPostingsReport(args: {
  storeClientId: string
  periodKey?: string | null
  rows: FboPostingsReportRow[]
  fetchedAt?: string
}) {
  const storeClientId = text(args.storeClientId)
  if (!storeClientId) return { rowsCount: 0, shipmentDateCount: 0, deliveryDateCount: 0 }
  const periodKey = text(args.periodKey)
  const fetchedAt = text(args.fetchedAt) || nowIso()
  const rows = Array.isArray(args.rows) ? args.rows : []
  const normalizedRows = new Map<string, FboReportMemoryRow>()

  for (const rawRow of rows) {
    const postingNumber = text(rawRow?.posting_number)
    if (!postingNumber) continue
    const deliverySchemaRaw = text(rawRow?.delivery_schema)
    const deliverySchema = deliverySchemaRaw.toLowerCase().replace(/[^a-z]/g, '')
    if (deliverySchema && deliverySchema !== 'fbo') continue
    const shipmentDate = dateText(rawRow?.shipment_date)
    const deliveryDate = dateText(rawRow?.delivery_date)
    const prev = normalizedRows.get(postingNumber)
    normalizedRows.set(postingNumber, {
      store_client_id: storeClientId,
      period_key: periodKey,
      posting_number: postingNumber,
      order_number: mergePreferIncoming(text(rawRow?.order_number) || null, prev?.order_number ?? null),
      delivery_schema: mergePreferIncoming(deliverySchemaRaw || null, prev?.delivery_schema ?? null),
      shipment_date: mergePreferIncoming(shipmentDate || null, prev?.shipment_date ?? null),
      delivery_date: mergePreferIncoming(deliveryDate || null, prev?.delivery_date ?? null),
      raw_row_json: mergePreferIncoming(safeJsonStringify(rawRow?.raw_row) ?? null, prev?.raw_row_json ?? null),
      updated_at: fetchedAt,
    })
  }

  const incomingKeys = new Set<string>()
  for (const row of normalizedRows.values()) {
    const k = key([row.store_client_id, row.period_key, row.posting_number])
    const existing = reportRowsByKey.get(k)
    reportRowsByKey.set(k, {
      ...row,
      order_number: mergePreferIncoming(row.order_number ?? null, existing?.order_number ?? null),
      delivery_schema: mergePreferIncoming(row.delivery_schema ?? null, existing?.delivery_schema ?? null),
      shipment_date: mergePreferIncoming(row.shipment_date ?? null, existing?.shipment_date ?? null),
      delivery_date: mergePreferIncoming(row.delivery_date ?? null, existing?.delivery_date ?? null),
      raw_row_json: mergePreferIncoming(row.raw_row_json ?? null, existing?.raw_row_json ?? null),
    })
    incomingKeys.add(k)
  }
  for (const [k, row] of reportRowsByKey.entries()) {
    if (row.store_client_id === storeClientId && row.period_key === periodKey && !incomingKeys.has(k)) reportRowsByKey.delete(k)
  }

  const scopedRows = Array.from(reportRowsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === periodKey)
  return {
    rowsCount: scopedRows.length,
    shipmentDateCount: scopedRows.filter((row) => text(row.shipment_date)).length,
    deliveryDateCount: scopedRows.filter((row) => text(row.delivery_date)).length,
  }
}

export function persistFboPushShipmentEvents(args: {
  storeClientId: string
  events: Array<{
    posting_number: string
    event_type?: string | null
    new_state?: string | null
    state?: string | null
    changed_state_date: string
  }>
  fetchedAt?: string
}) {
  const storeClientId = text(args.storeClientId)
  const fetchedAt = text(args.fetchedAt) || nowIso()
  if (!storeClientId) return { acceptedEventsCount: 0, shipmentTransferEventCount: 0, shipmentDateCount: 0, samplePostingNumbers: [] as string[] }

  const normalizedEvents = (Array.isArray(args.events) ? args.events : [])
    .map((event) => {
      const postingNumber = text((event as any)?.posting_number)
      const changedStateDate = dateText((event as any)?.changed_state_date)
      const eventType = text((event as any)?.event_type) || FBO_STATE_CHANGED_EVENT_TYPE
      const newState = text((event as any)?.new_state || (event as any)?.state) || 'posting_transferring_to_delivery'
      if (!postingNumber || !changedStateDate) return null
      return {
        store_client_id: storeClientId,
        period_key: FBO_SHIPMENT_PUSH_PERIOD_KEY,
        posting_number: postingNumber,
        event_key: `${eventType}|${newState}|${changedStateDate}`,
        event_type: eventType,
        new_state: newState,
        state: newState,
        changed_state_date: changedStateDate,
        updated_at: fetchedAt,
      }
    })
    .filter((event): event is FboEventMemoryRow => Boolean(event))

  const samplePostingNumbers: string[] = []
  for (const row of normalizedEvents) {
    eventsByKey.set(key([row.store_client_id, row.period_key, row.posting_number, row.event_key]), row)
    pushTraceSample(samplePostingNumbers, row.posting_number, 10)
    if (text(row.new_state) === 'posting_transferring_to_delivery') {
      for (const [postingKey, posting] of postingsByKey.entries()) {
        if (posting.store_client_id !== row.store_client_id || posting.posting_number !== row.posting_number) continue
        const current = text(posting.shipment_date)
        if (!current || current < row.changed_state_date) postingsByKey.set(postingKey, { ...posting, shipment_date: row.changed_state_date, updated_at: fetchedAt })
      }
    }
  }

  const shipmentTransferEventCount = Array.from(eventsByKey.values()).filter((row) => row.store_client_id === storeClientId && row.period_key === FBO_SHIPMENT_PUSH_PERIOD_KEY && text(row.event_type) === FBO_STATE_CHANGED_EVENT_TYPE && text(row.new_state || row.state) === 'posting_transferring_to_delivery').length
  const shipmentDateCount = Array.from(postingsByKey.values()).filter((row) => row.store_client_id === storeClientId && text(row.shipment_date)).length

  return {
    acceptedEventsCount: normalizedEvents.length,
    shipmentTransferEventCount,
    shipmentDateCount,
    samplePostingNumbers,
  }
}

export function mergeSalesRowsWithFboLocalDb(args: {
  rows: any[]
  storeClientId?: string | null
  periodKey?: string | null
}) {
  const periodKey = text(args.periodKey)
  const rows = Array.isArray(args.rows) ? args.rows : []
  if (rows.length === 0) return rows
  const storeClientId = text(args.storeClientId)
  const byPosting = new Map<string, FboPostingMemoryRow>()

  for (const posting of postingsByKey.values()) {
    if (posting.period_key !== periodKey) continue
    if (storeClientId && posting.store_client_id !== storeClientId) continue
    const postingNumber = text(posting.posting_number)
    if (!postingNumber || byPosting.has(postingNumber)) continue
    let shipmentDate = text(posting.shipment_date)
    if (!shipmentDate) shipmentDate = getPersistedShipmentDateForPosting(posting.store_client_id, posting.posting_number, posting.period_key)
    byPosting.set(postingNumber, { ...posting, shipment_date: shipmentDate || posting.shipment_date || null })
  }

  if (byPosting.size === 0) return rows

  return rows.map((row) => {
    const postingNumber = text(row?.posting_number)
    const extra = byPosting.get(postingNumber)
    if (!extra) return row

    const shipmentDateFromExtra = text(extra?.shipment_date)
    const shipmentDateFromRow = text(row?.shipment_date)
    const acceptedAt = text(row?.in_process_at)
    const normalizedExtraShipment = shipmentDateFromExtra && shipmentDateFromExtra !== acceptedAt ? shipmentDateFromExtra : ''
    const normalizedRowShipment = shipmentDateFromRow && shipmentDateFromRow !== acceptedAt ? shipmentDateFromRow : ''
    const shipmentDate = normalizedExtraShipment || normalizedRowShipment

    const deliveryDateFromExtra = text(extra?.delivery_date)
    const rowStatus = text(row?.status).toLowerCase()
    const isDelivered = Boolean(rowStatus && (rowStatus.includes('доставлен') || rowStatus.includes('получен покупателем') || FBO_DELIVERED_STATES.has(rowStatus)))
    const deliveryDate = isDelivered ? deliveryDateFromExtra : ''

    return {
      ...row,
      related_postings: text(row?.related_postings) || text(extra?.related_postings),
      shipment_date: shipmentDate,
      shipment_date_source: normalizedExtraShipment ? 'online_session' : text((row as any)?.shipment_date_source),
      delivery_date: deliveryDate,
      delivery_cluster: text(row?.delivery_cluster) || text(extra?.delivery_cluster),
      delivery_model: text(row?.delivery_model) || 'FBO',
    }
  })
}

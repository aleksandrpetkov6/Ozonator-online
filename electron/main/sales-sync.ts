import { ozonPostingFboGet, ozonPostingFbsGet } from './ozon'
import type { SalesPostingsReportRow } from './postings-report'

export type SalesPeriod = {
  from?: string | null
  to?: string | null
}

export type SalesRow = GridApiRow & {
  in_process_at?: string | null
  posting_number?: string | null
  related_postings?: string | null
  shipment_date?: string | null
  shipment_origin?: string | null
  status?: string | null
  status_details?: string | null
  carrier_status_details?: string | null
  delivery_date?: string | null
  delivery_cluster?: string | null
  delivery_model?: string | null
  currency?: string | null
  item_currency?: string | null
  customer_currency_in_item_currency?: number | string | null
  price?: number | ''
  quantity?: number | ''
  paid_by_customer?: number | ''
}

export type SalesPayloadEnvelope = {
  endpoint: string
  payload: any
}

export type SalesPaidByCustomerTrace = {
  totalPostingCount: number
  totalItemCount: number
  postingsWithDetailCount: number
  listItemDirectValueCount: number
  listFinancialValueCount: number
  detailItemDirectValueCount: number
  detailFinancialValueCount: number
  detailWithFinancialDataObjectCount: number
  detailWithFinancialProductsArrayCount: number
  detailWithNonEmptyFinancialProductsCount: number
  reportRowsCount: number
  reportRowsWithPaidByCustomerCount: number
  reportMatchedRowsCount: number
  reportResolvedRowsCount: number
  finalRowsCount: number
  finalRowsWithPaidByCustomer: number
  finalRowsWithoutPaidByCustomer: number
  fbsRowsWithPaidByCustomer: number
  fboRowsWithPaidByCustomer: number
  rfbsRowsWithPaidByCustomer: number
  missingPostingNumbers: string[]
  detailShapeSamples: string[]
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

function sleep(ms: number): Promise<void> {
  const delay = Number(ms)
  if (!Number.isFinite(delay) || delay <= 0) return Promise.resolve()
  return new Promise((resolve) => setTimeout(resolve, delay))
}

function safeGetByPath(source: any, path: string, fallback: any = undefined) {
  if (!source || typeof source !== 'object') return fallback
  const parts = String(path ?? '').split('.').map((x) => x.trim()).filter(Boolean)
  if (parts.length === 0) return fallback
  let cur = source
  for (const part of parts) {
    if (cur == null || typeof cur !== 'object' || !(part in cur)) return fallback
    cur = cur[part]
  }
  return cur == null ? fallback : cur
}

function pickFirstPresent(source: any, paths: string[]) {
  for (const path of paths) {
    const value = safeGetByPath(source, path, undefined)
    if (value === undefined || value === null || value === '') continue
    return value
  }
  return undefined
}

function normalizeTextValue(value: any): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

const SALES_STATUS_LABELS_RU: Record<string, string> = {
  awaiting_packaging: 'Ожидает упаковки',
  awaiting_deliver: 'Ожидает доставки',
  awaiting_approve: 'Ожидает подтверждения',
  awaiting_registration: 'Ожидает регистрации',
  awaiting_customer: 'Ожидает покупателя',
  acceptance_in_progress: 'Идёт приёмка',
  created: 'Создан',
  processing: 'В обработке',
  in_process: 'В обработке',
  ready_to_ship: 'Готов к отгрузке',
  ready_for_pickup: 'Готов к выдаче',
  shipped: 'Отгружен',
  handed_over_to_delivery: 'Передан в доставку',
  sent_to_delivery: 'Передан в доставку',
  sent_by_seller: 'Отправлен продавцом',
  driver_pickup: 'Забирает курьер',
  in_transit: 'В пути',
  transit: 'В пути',
  on_the_way: 'В пути',
  on_route: 'В пути',
  delivering: 'Доставляется',
  delivery_failed: 'Доставка не удалась',
  delivered: 'Доставлен',
  delivered_to_customer: 'Доставлен покупателю',
  customer_received: 'Получен покупателем',
  returned: 'Возвращён',
  returning: 'Возвращается',
  return_in_progress: 'Возврат в обработке',
  return_preparing: 'Готовится возврат',
  return_arrived_to_seller: 'Возврат прибыл продавцу',
  return_ready_for_seller_pickup: 'Готов к выдаче продавцу',
  return_not_possible: 'Возврат невозможен',
  cancelled: 'Отменён',
  not_accepted: 'Не принят',
  not_in_time: 'Не доставлен вовремя',
  not_found: 'Не найден',
  lost: 'Утерян',
  damaged: 'Повреждён',
  arbitration: 'Арбитраж',
  client_arbitration: 'Арбитраж с клиентом',
  posting_created: 'Создан',
  posting_created_from_split: 'Создан после разделения',
  posting_registered: 'Зарегистрирован',
  posting_accepted: 'Принят',
  posting_reception_transfer: 'Передан на приёмку',
  posting_ready_to_ship: 'Готов к отгрузке',
  posting_sent_by_seller: 'Отправлен продавцом',
  posting_transferring_to_delivery: 'Передаётся в доставку',
  posting_transfered_to_courier_service: 'Передан в службу доставки',
  posting_transferred_to_courier_service: 'Передан в службу доставки',
  posting_driver_pick_up: 'Забирает курьер',
  posting_in_carriage: 'В пути',
  posting_sent_to_city: 'Отправлен в город получения',
  posting_on_way_to_city: 'В пути в город получения',
  posting_on_way_to_pickup_point: 'В пути в пункт выдачи',
  posting_arrived_at_pickup_point: 'Прибыл в пункт выдачи',
  posting_in_pickup_point: 'В пункте выдачи',
  posting_on_pickup_point: 'В пункте выдачи',
  posting_waiting_buyer: 'Ожидает покупателя',
  posting_waiting_passport_data: 'Ожидает паспортные данные',
  posting_conditionally_delivered: 'Условно доставлен',
  posting_delivering: 'Доставляется',
  posting_delivered: 'Доставлен',
  posting_delivered_to_customer: 'Доставлен покупателю',
  posting_not_in_sort_center: 'Не найден в сортировочном центре',
  posting_not_in_pickup_point: 'Не найден в пункте выдачи',
  posting_lost: 'Утерян',
  posting_damaged: 'Повреждён',
  posting_timeout: 'Истёк срок хранения',
  posting_return_in_progress: 'Возврат в обработке',
  posting_returning: 'Возвращается',
  posting_returned: 'Возвращён',
  posting_returned_to_seller: 'Возвращён продавцу',
  posting_partial_return: 'Частичный возврат',
  returned_to_seller: 'Возвращён продавцу',
}

const SALES_PROVIDER_STATUS_LABELS_RU: Record<string, string> = {
  created: 'Создан',
  accepted: 'Принят',
  awaiting_registration: 'Ожидает регистрации',
  ready_for_pickup: 'Готов к выдаче',
  ready_to_ship: 'Готов к отгрузке',
  handed_over_to_delivery: 'Передан в доставку',
  sent_to_delivery: 'Передан в доставку',
  in_transit: 'В пути',
  transit: 'В пути',
  on_the_way: 'В пути',
  on_route: 'В пути',
  delivering: 'Доставляется',
  delivery_failed: 'Доставка не удалась',
  delivered: 'Доставлен',
  delivered_to_customer: 'Доставлен покупателю',
  returned: 'Возвращён',
  returning: 'Возвращается',
  cancelled: 'Отменён',
  lost: 'Утерян',
  damaged: 'Повреждён',
  not_found: 'Не найден',
  on_point: 'В пункте выдачи',
  pickup: 'В пункте выдачи',
  at_pickup_point: 'В пункте выдачи',
}

function capitalizeSalesText(value: string): string {
  const text = value.trim()
  if (!text) return ''
  return text.charAt(0).toUpperCase() + text.slice(1)
}

function getUnknownSalesText(mode: 'status' | 'detail' | 'provider'): string {
  if (mode === 'provider' || mode === 'detail') return ''
  return 'Прочий статус'
}

function normalizeSalesLookupKey(value: any): string {
  let text = normalizeTextValue(value)
  if (!text) return ''
  const prefixed = text.match(/^(substatus|previous_substatus|provider_status|status|state)\s*[:=]\s*(.+)$/i)
  if (prefixed?.[2]) text = prefixed[2].trim()
  return text
    .toLowerCase()
    .replace(/[|]+/g, ' ')
    .replace(/[./\\]+/g, ' ')
    .replace(/[:=]+/g, ' ')
    .replace(/[_\-\s]+/g, '_')
    .replace(/^_+|_+$/g, '')
}

export function translateSalesCodeValue(value: any, mode: 'status' | 'detail' | 'provider' = 'status'): string {
  const raw = normalizeTextValue(value)
  if (!raw) return ''
  if (raw.includes('|')) {
    const parts = raw
      .split(/\s*\|\s*/)
      .map((part) => translateSalesCodeValue(part, mode))
      .filter(Boolean)
    return Array.from(new Set(parts)).join(' | ')
  }
  const prefixedPrevious = raw.match(/^(previous|previous_substatus)\s*[:=]\s*(.+)$/i)
  if (prefixedPrevious?.[2]) {
    const translated = translateSalesCodeValue(prefixedPrevious[2], 'detail')
    return translated ? `Предыдущий: ${translated}` : ''
  }
  if (/[А-Яа-яЁё]/.test(raw)) return capitalizeSalesText(raw)
  const key = normalizeSalesLookupKey(raw)
  if (!key) return ''
  const providerMapped = SALES_PROVIDER_STATUS_LABELS_RU[key]
  const statusMapped = SALES_STATUS_LABELS_RU[key]
  const mapped = mode === 'provider' ? (providerMapped ?? statusMapped) : (statusMapped ?? providerMapped)
  if (mapped) return capitalizeSalesText(mapped)
  return getUnknownSalesText(mode)
}

function pushUniqueSalesPart(parts: string[], value: any) {
  const text = normalizeTextValue(value)
  if (!text) return
  if (!parts.includes(text)) parts.push(text)
}

function pushLabeledSalesPart(parts: string[], label: string, value: any) {
  const text = normalizeTextValue(value)
  if (!text) return
  const normalized = `${label}: ${text}`
  if (!parts.includes(normalized)) parts.push(normalized)
}

function normalizeDateValue(value: any): string {
  if (value == null || value === '') return ''
  const raw = typeof value === 'string' ? value.trim() : String(value).trim()
  if (!raw) return ''
  const parsed = new Date(raw)
  return Number.isNaN(parsed.getTime()) ? '' : raw
}

function normalizeNumberValue(value: any): number | '' {
  if (value == null || value === '') return ''
  const n = Number(value)
  if (!Number.isFinite(n)) return ''
  return n
}

function normalizeCurrencyValue(value: any): string {
  const raw = normalizeTextValue(value).toUpperCase()
  return raw
}

function extractPostingItems(source: any): any[] {
  const direct = pickFirstPresent(source, ['products', 'result.products', 'items', 'result.items'])
  return Array.isArray(direct) ? direct : []
}

function extractFinancialProducts(source: any): any[] {
  const direct = pickFirstPresent(source, ['financial_data.products', 'result.financial_data.products'])
  return Array.isArray(direct) ? direct : []
}

function sampleObjectKeys(value: any, limit = 8): string[] {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return []
  return Object.keys(value).slice(0, limit)
}

function summarizePaidByCustomerDetailShape(postingNumber: string, detailPosting: any): string {
  const rootKeys = sampleObjectKeys(detailPosting)
  const financialData = pickFirstPresent(detailPosting, ['financial_data', 'result.financial_data'])
  const financialKeys = sampleObjectKeys(financialData)
  const financialProducts = extractFinancialProducts(detailPosting)
  const itemSample = extractPostingItems(detailPosting)[0] ?? null
  const financialProductSample = financialProducts[0] ?? null
  const parts = [
    postingNumber,
    `root=[${rootKeys.join('|') || '-'}]`,
    `financial=${financialData && typeof financialData === 'object' ? 'yes' : 'no'}`,
    `financialKeys=[${financialKeys.join('|') || '-'}]`,
    `financialProducts=${financialProducts.length}`,
    `itemKeys=[${sampleObjectKeys(itemSample).join('|') || '-'}]`,
    `finProductKeys=[${sampleObjectKeys(financialProductSample).join('|') || '-'}]`,
  ]
  return parts.join(', ')
}

function findSalesProductIdByItemInSources(item: any, sources: any[]): string {
  const directProductId = normalizeTextValue(pickFirstPresent(item, ['product_id', 'productId']))
  if (directProductId) return directProductId

  const offerId = normalizeTextValue(pickFirstPresent(item, ['offer_id', 'offerId', 'article']))
  const sku = normalizeTextValue(pickFirstPresent(item, ['sku', 'sku_id', 'id']))

  for (const source of sources) {
    for (const candidate of extractPostingItems(source)) {
      const candidateProductId = normalizeTextValue(pickFirstPresent(candidate, ['product_id', 'productId']))
      if (!candidateProductId) continue

      const candidateOfferId = normalizeTextValue(pickFirstPresent(candidate, ['offer_id', 'offerId', 'article']))
      if (offerId && candidateOfferId && candidateOfferId === offerId) return candidateProductId

      const candidateSku = normalizeTextValue(pickFirstPresent(candidate, ['sku', 'sku_id', 'id']))
      if (sku && candidateSku && candidateSku === sku) return candidateProductId
    }
  }

  return ''
}

function findSalesProductIdByItem(item: any, detailPosting: any, posting: any): string {
  return findSalesProductIdByItemInSources(item, [detailPosting, posting])
}

function findMatchingSalesItemInSource(item: any, source: any): any {
  const directProductId = normalizeTextValue(pickFirstPresent(item, ['product_id', 'productId']))
  const offerId = normalizeTextValue(pickFirstPresent(item, ['offer_id', 'offerId', 'article']))
  const sku = normalizeTextValue(pickFirstPresent(item, ['sku', 'sku_id', 'id']))

  for (const candidate of extractPostingItems(source)) {
    const candidateProductId = normalizeTextValue(pickFirstPresent(candidate, ['product_id', 'productId']))
    if (directProductId && candidateProductId && candidateProductId === directProductId) return candidate

    const candidateOfferId = normalizeTextValue(pickFirstPresent(candidate, ['offer_id', 'offerId', 'article']))
    if (offerId && candidateOfferId && candidateOfferId === offerId) return candidate

    const candidateSku = normalizeTextValue(pickFirstPresent(candidate, ['sku', 'sku_id', 'id']))
    if (sku && candidateSku && candidateSku === sku) return candidate
  }

  return null
}

function findSalesFinancialProductInSources(item: any, sources: any[]): any {
  const productId = findSalesProductIdByItemInSources(item, sources)
  const offerId = normalizeTextValue(pickFirstPresent(item, ['offer_id', 'offerId', 'article']))
  const sku = normalizeTextValue(pickFirstPresent(item, ['sku', 'sku_id', 'id']))

  for (const source of sources) {
    const financialProducts = extractFinancialProducts(source)
    if (financialProducts.length === 0) continue

    if (productId) {
      const byProductId = financialProducts.find((candidate) => normalizeTextValue(pickFirstPresent(candidate, ['product_id', 'productId', 'id'])) === productId)
      if (byProductId) return byProductId
    }

    if (offerId) {
      const byOfferId = financialProducts.find((candidate) => normalizeTextValue(pickFirstPresent(candidate, ['offer_id', 'offerId', 'article'])) === offerId)
      if (byOfferId) return byOfferId
    }

    if (sku) {
      const bySku = financialProducts.find((candidate) => normalizeTextValue(pickFirstPresent(candidate, ['sku', 'sku_id'])) === sku)
      if (bySku) return bySku
    }

    if (financialProducts.length === 1) return financialProducts[0]
  }

  return null
}

function findSalesFinancialProduct(item: any, detailPosting: any, posting: any): any {
  return findSalesFinancialProductInSources(item, [detailPosting, posting])
}

function resolveSalesItemPriceValue(item: any): number | '' {
  return normalizeNumberValue(pickFirstPresent(item, ['price', 'your_price', 'seller_price']))
}

function resolveSalesItemCurrencyValue(item: any, detailPosting: any, posting: any): string {
  const financialProduct = findSalesFinancialProduct(item, detailPosting, posting)
  return normalizeCurrencyValue(pickFirstPresent(financialProduct, [
    'customer_currency_code',
    'customerCurrencyCode',
  ]))
}

function resolveSalesItemProductCurrencyValue(item: any, detailPosting: any): string {
  const detailItem = findMatchingSalesItemInSource(item, detailPosting)
  return normalizeCurrencyValue(pickFirstPresent(item, [
    'currency_code',
    'currencyCode',
  ])) || normalizeCurrencyValue(pickFirstPresent(detailItem, [
    'currency_code',
    'currencyCode',
  ]))
}

function resolveSalesItemPaidByCustomerValue(item: any, detailPosting: any, posting: any): number | '' {
  const financialProduct = findSalesFinancialProduct(item, detailPosting, posting)
  const financialClientPrice = normalizeNumberValue(pickFirstPresent(financialProduct, ['client_price', 'clientPrice']))
  if (financialClientPrice !== '') return financialClientPrice

  return normalizeNumberValue(pickFirstPresent(item, ['client_price', 'clientPrice', 'paid_by_customer', 'paidByCustomer']))
}

function resolveSalesItemQuantityValue(item: any): number | '' {
  return normalizeNumberValue(pickFirstPresent(item, ['quantity', 'qty']))
}


function normalizeSalesModelKey(value: any): string {
  const raw = normalizeTextValue(value).toUpperCase()
  if (!raw) return ''
  if (raw === 'RFBS') return 'rFBS'
  if (raw === 'FBS') return 'FBS'
  if (raw === 'FBO') return 'FBO'
  return raw
}

function buildSalesReportLookupCandidates(args: {
  deliveryModel?: any
  postingNumber?: any
  sku?: any
  offerId?: any
  productName?: any
}): string[] {
  const postingNumber = normalizeTextValue(args.postingNumber)
  if (!postingNumber) return []
  const sku = normalizeTextValue(args.sku)
  const offerId = normalizeTextValue(args.offerId)
  const productName = normalizeTextValue(args.productName)
  const model = normalizeSalesModelKey(args.deliveryModel)
  const modelCandidates = model === 'rFBS' ? ['rFBS', 'FBS', '*'] : (model ? [model, '*'] : ['*'])
  const out: string[] = []
  const push = (...parts: string[]) => {
    const candidate = parts.map((part) => normalizeTextValue(part)).join('|')
    if (candidate && !out.includes(candidate)) out.push(candidate)
  }

  for (const modelCandidate of modelCandidates) {
    if (sku && offerId && productName) push(modelCandidate, postingNumber, sku, offerId, productName)
    if (sku && offerId) push(modelCandidate, postingNumber, sku, offerId)
    if (sku) push(modelCandidate, postingNumber, sku)
    if (offerId) push(modelCandidate, postingNumber, offerId)
    if (productName) push(modelCandidate, postingNumber, productName)
  }
  return out
}

function buildSalesPaidByCustomerReportMap(reportRows: SalesPostingsReportRow[]): {
  valueByKey: Map<string, number>
  reportRowsCount: number
  reportRowsWithPaidByCustomerCount: number
} {
  const valueByKey = new Map<string, number>()
  let reportRowsCount = 0
  let reportRowsWithPaidByCustomerCount = 0

  for (const row of Array.isArray(reportRows) ? reportRows : []) {
    reportRowsCount += 1
    const paidValue = normalizeNumberValue(row?.paid_by_customer)
    if (paidValue === '') continue
    reportRowsWithPaidByCustomerCount += 1
    const candidates = buildSalesReportLookupCandidates({
      deliveryModel: row?.delivery_schema,
      postingNumber: row?.posting_number,
      sku: row?.sku,
      offerId: row?.offer_id,
      productName: row?.product_name,
    })
    for (const candidate of candidates) {
      if (!valueByKey.has(candidate)) valueByKey.set(candidate, paidValue)
    }
  }

  return { valueByKey, reportRowsCount, reportRowsWithPaidByCustomerCount }
}

function resolveSalesPaidByCustomerFromReportRow(
  row: Pick<SalesRow, 'delivery_model' | 'posting_number' | 'sku' | 'offer_id' | 'name'>,
  reportValueByKey: Map<string, number>,
): number | '' {
  const candidates = buildSalesReportLookupCandidates({
    deliveryModel: row?.delivery_model,
    postingNumber: row?.posting_number,
    sku: row?.sku,
    offerId: row?.offer_id,
    productName: row?.name,
  })
  for (const candidate of candidates) {
    const value = reportValueByKey.get(candidate)
    if (value !== undefined) return value
  }
  return ''
}

function applySalesPaidByCustomerFromReportRows(rows: SalesRow[], reportRows: SalesPostingsReportRow[]): {
  rows: SalesRow[]
  reportRowsCount: number
  reportRowsWithPaidByCustomerCount: number
  reportMatchedRowsCount: number
  reportResolvedRowsCount: number
} {
  const { valueByKey, reportRowsCount, reportRowsWithPaidByCustomerCount } = buildSalesPaidByCustomerReportMap(reportRows)
  if (valueByKey.size === 0) {
    return {
      rows,
      reportRowsCount,
      reportRowsWithPaidByCustomerCount,
      reportMatchedRowsCount: 0,
      reportResolvedRowsCount: 0,
    }
  }

  let reportMatchedRowsCount = 0
  let reportResolvedRowsCount = 0
  const nextRows = rows.map((row) => {
    const reportPaid = resolveSalesPaidByCustomerFromReportRow(row, valueByKey)
    if (reportPaid === '') return row
    reportMatchedRowsCount += 1
    if (normalizeNumberValue(row?.paid_by_customer) !== '') return row
    reportResolvedRowsCount += 1
    return {
      ...row,
      paid_by_customer: reportPaid,
    }
  })

  return {
    rows: nextRows,
    reportRowsCount,
    reportRowsWithPaidByCustomerCount,
    reportMatchedRowsCount,
    reportResolvedRowsCount,
  }
}

export function extractPostingsFromPayload(payload: any): any[] {
  const fromResult = safeGetByPath(payload, 'result.postings', null)
  if (Array.isArray(fromResult)) return fromResult

  const fromResultItems = safeGetByPath(payload, 'result.items', null)
  if (Array.isArray(fromResultItems)) return fromResultItems

  const fromResultRows = safeGetByPath(payload, 'result.rows', null)
  if (Array.isArray(fromResultRows)) return fromResultRows

  const direct = safeGetByPath(payload, 'postings', null)
  if (Array.isArray(direct)) return direct

  const directItems = safeGetByPath(payload, 'items', null)
  if (Array.isArray(directItems)) return directItems

  const directResult = safeGetByPath(payload, 'result', null)
  if (Array.isArray(directResult)) return directResult

  if (Array.isArray(payload)) return payload
  return []
}

function normalizeTextList(values: any[]): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  const pushOne = (value: any) => {
    if (value == null) return
    if (Array.isArray(value)) {
      for (const item of value) pushOne(item)
      return
    }
    if (typeof value === 'object') {
      const obj = value as Record<string, unknown>
      const direct = [
        obj.posting_number,
        obj.postingNumber,
        obj.related_posting_number,
        obj.related_posting_numbers,
        obj.relatedPostingNumbers,
        obj.related_postings,
        obj.relatedPostings,
        obj.parent_posting_number,
        obj.parentPostingNumber,
        obj.number,
        obj.value,
        obj.name,
        obj.id,
      ]
      for (const candidate of direct) pushOne(candidate)
      return
    }
    const raw = String(value).trim()
    if (!raw) return
    for (const part of raw.split(',')) {
      const normalized = part.trim()
      if (!normalized || seen.has(normalized)) continue
      seen.add(normalized)
      out.push(normalized)
    }
  }
  for (const value of values) pushOne(value)
  return out
}

function pickFirstPresentFromSources(paths: string[], ...sources: any[]) {
  for (const source of sources) {
    const value = pickFirstPresent(source, paths)
    if (value !== undefined && value !== null && value !== '') return value
  }
  return undefined
}

export type SalesPostingStateEvent = {
  event_type: string
  new_state: string
  state: string
  changed_state_date: string
}

const SALES_STATE_CHANGED_EVENT_TYPE = 'type_state_changed'
const FBO_SHIPMENT_STATE = 'posting_transferring_to_delivery'
const FBO_SHIPMENT_STATE_SET = new Set<string>([FBO_SHIPMENT_STATE])

function buildSalesStateEventCandidate(source: any): SalesPostingStateEvent | null {
  if (!source || typeof source !== 'object') return null

  const explicitNewState = pickFirstPresent(source, ['new_state'])
  const eventType = normalizeSalesLookupKey(pickFirstPresent(source, ['type', 'event_type']))
  const isExplicitStateEvent = eventType === SALES_STATE_CHANGED_EVENT_TYPE
  if (!isExplicitStateEvent) return null

  const newState = normalizeSalesLookupKey(
    explicitNewState ?? pickFirstPresent(source, ['state', 'status']),
  )
  const changedStateDate = normalizeDateValue(
    pickFirstPresent(source, ['changed_state_date'])
      ?? pickFirstPresent(source, ['date', 'created_at']),
  )

  if (!newState || !changedStateDate) return null
  return {
    event_type: SALES_STATE_CHANGED_EVENT_TYPE,
    new_state: newState,
    state: newState,
    changed_state_date: changedStateDate,
  }
}

export function collectSalesStateEvents(...sources: any[]): SalesPostingStateEvent[] {
  const out: SalesPostingStateEvent[] = []
  const seen = new Set<string>()
  const visited = new Set<any>()

  const walk = (value: any) => {
    if (!value || typeof value !== 'object') return
    if (visited.has(value)) return
    visited.add(value)

    const candidate = buildSalesStateEventCandidate(value)
    if (candidate) {
      const key = `${candidate.state}|${candidate.changed_state_date}`
      if (!seen.has(key)) {
        seen.add(key)
        out.push(candidate)
      }
    }

    if (Array.isArray(value)) {
      for (const item of value) walk(item)
      return
    }

    for (const nested of Object.values(value as Record<string, unknown>)) {
      if (!nested || typeof nested !== 'object') continue
      walk(nested)
    }
  }

  for (const source of sources) walk(source)

  out.sort((left, right) => left.changed_state_date.localeCompare(right.changed_state_date))
  return out
}

export function resolveFboShipmentDateFromSources(...sources: any[]): string {
  return collectSalesStateEvents(...sources)
    .filter((event) => (
      event.event_type === SALES_STATE_CHANGED_EVENT_TYPE
      && FBO_SHIPMENT_STATE_SET.has(event.new_state)
    ))
    .map((event) => event.changed_state_date)
    .sort((left, right) => right.localeCompare(left))[0] ?? ''
}

function buildRelatedPostingsText(posting: any, fallbackPostings: string[] = [], secondaryPosting: any = null): string {
  const candidates = normalizeTextList([
    safeGetByPath(posting, 'related_postings.related_posting_numbers', undefined),
    safeGetByPath(posting, 'result.related_postings.related_posting_numbers', undefined),
    safeGetByPath(posting, 'related_postings.related_postings', undefined),
    safeGetByPath(posting, 'related_postings_numbers', undefined),
    safeGetByPath(posting, 'related_posting_numbers', undefined),
    safeGetByPath(posting, 'related_postings', undefined),
    safeGetByPath(posting, 'parent_posting_number', undefined),
    safeGetByPath(secondaryPosting, 'related_postings.related_posting_numbers', undefined),
    safeGetByPath(secondaryPosting, 'result.related_postings.related_posting_numbers', undefined),
    safeGetByPath(secondaryPosting, 'related_postings.related_postings', undefined),
    safeGetByPath(secondaryPosting, 'related_postings_numbers', undefined),
    safeGetByPath(secondaryPosting, 'related_posting_numbers', undefined),
    safeGetByPath(secondaryPosting, 'related_postings', undefined),
    safeGetByPath(secondaryPosting, 'parent_posting_number', undefined),
    fallbackPostings,
  ])
  return candidates.join(', ')
}

function normalizeSalesEndpointName(endpoint: string): 'FBS' | 'FBO' | '' {
  if (endpoint.includes('/posting/fbs/')) return 'FBS'
  if (endpoint.includes('/posting/fbo/')) return 'FBO'
  return ''
}

function normalizeDeliveryModelLabel(value: any): 'FBS' | 'rFBS' | 'FBO' | '' {
  const raw = normalizeTextValue(value)
  if (!raw) return ''
  const compact = raw.replace(/[^a-z]/gi, '').toUpperCase()
  if (!compact) return ''
  if (compact.includes('RFBS')) return 'rFBS'
  if (compact.includes('FBO')) return 'FBO'
  if (compact.includes('FBS')) return 'FBS'
  return ''
}

function pickDeliverySchemaFromOperations(source: any): any {
  if (!source || typeof source !== 'object') return undefined
  const operations = Array.isArray((source as any)?.operations)
    ? (source as any).operations
    : (Array.isArray((source as any)?.result?.operations) ? (source as any).result.operations : [])
  for (const operation of operations) {
    const value = pickFirstPresent(operation, ['posting.delivery_schema', 'delivery_schema'])
    if (value !== undefined && value !== null && value !== '') return value
  }
  return undefined
}

function hasRfbsAnalyticsSignal(...sources: any[]): boolean {
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue
    const city = normalizeTextValue(pickFirstPresent(source, ['analytics_data.city', 'city']))
    const region = normalizeTextValue(pickFirstPresent(source, ['analytics_data.region', 'region']))
    if (city || region) return true
  }
  return false
}

function buildDeliveryModelValue(posting: any, detailPosting: any, endpoint: string): string {
  const explicit = normalizeDeliveryModelLabel(
    pickFirstPresent(detailPosting, [
      'delivery_schema',
      'posting.delivery_schema',
      'analytics_data.delivery_schema',
      'result.delivery_schema',
      'result.posting.delivery_schema',
    ])
    ?? pickDeliverySchemaFromOperations(detailPosting)
    ?? pickFirstPresent(posting, [
      'delivery_schema',
      'posting.delivery_schema',
      'analytics_data.delivery_schema',
      'result.delivery_schema',
      'result.posting.delivery_schema',
    ])
    ?? pickDeliverySchemaFromOperations(posting),
  )
  if (explicit) return explicit

  const normalizedEndpoint = normalizeSalesEndpointName(endpoint)
  if (normalizedEndpoint === 'FBO') return 'FBO'
  if (normalizedEndpoint === 'FBS') {
    if (hasRfbsAnalyticsSignal(detailPosting, posting)) return 'rFBS'
    return 'FBS'
  }

  return normalizeDeliveryModelLabel(
    pickFirstPresent(detailPosting, ['delivery_method.name', 'delivery_method', 'delivery_type', 'analytics_data.delivery_type'])
    ?? pickFirstPresent(posting, ['delivery_method.name', 'delivery_method', 'delivery_type', 'analytics_data.delivery_type']),
  )
}

function resolveSalesShipmentOriginValue(detailPosting: any, posting: any, endpoint: string, deliveryModelRaw: unknown): string {
  const deliveryModel = normalizeDeliveryModelLabel(deliveryModelRaw) || buildDeliveryModelValue(posting, detailPosting, endpoint)
  if (deliveryModel === 'FBO') {
    return normalizeTextValue(pickFirstPresentFromSources([
      'financial_data.cluster_from',
      'result.financial_data.cluster_from',
      'cluster_from',
      'result.cluster_from',
    ], detailPosting, posting))
  }
  if (deliveryModel === 'FBS' || deliveryModel === 'rFBS') {
    return normalizeTextValue(pickFirstPresentFromSources([
      'delivery_method.warehouse',
      'result.delivery_method.warehouse',
      'analytics_data.warehouse',
      'result.analytics_data.warehouse',
      'analytics_data.warehouse_name',
      'result.analytics_data.warehouse_name',
      'warehouse_name',
      'warehouse',
    ], detailPosting, posting))
  }
  return ''
}

function buildSalesStatusDetailsValue(posting: any, endpoint: string, secondaryPosting: any = null): string {
  const parts: string[] = []
  if (normalizeSalesEndpointName(endpoint) === 'FBO') {
    const nextStateRaw = pickFirstPresentFromSources(['new_state', 'result.new_state'], posting, secondaryPosting)
    const nextStateKey = normalizeSalesLookupKey(nextStateRaw)
    const nextState = nextStateKey === FBO_SHIPMENT_STATE
      ? 'Передан в доставку'
      : translateSalesCodeValue(nextStateRaw, 'detail')
    pushUniqueSalesPart(parts, nextState)
    pushLabeledSalesPart(parts, 'Дата изменения', pickFirstPresentFromSources(['changed_state_date', 'result.changed_state_date'], posting, secondaryPosting))
    return parts.join(' | ')
  }
  const substatus = translateSalesCodeValue(pickFirstPresentFromSources(['substatus', 'result.substatus'], posting, secondaryPosting), 'detail')
  pushUniqueSalesPart(parts, substatus)
  return parts.join(' | ')
}

function buildSalesCarrierStatusDetailsValue(posting: any, secondaryPosting: any = null): string {
  const parts: string[] = []
  pushUniqueSalesPart(parts, translateSalesCodeValue(pickFirstPresentFromSources(['provider_status', 'result.provider_status'], posting, secondaryPosting), 'provider'))
  return parts.join(' | ')
}

function normalizeSalesPeriodDate(value: any): string {
  const raw = typeof value === 'string' ? value.trim() : ''
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : ''
}

function buildUtcDayBoundaryIso(year: number, month: number, day: number, boundary: 'start' | 'end'): string {
  const iso = boundary === 'start'
    ? new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0))
    : new Date(Date.UTC(year, month - 1, day, 23, 59, 59, 999))
  return iso.toISOString()
}

export function buildSalesRequestBody(period: SalesPeriod | null | undefined, limit = 1000, offset = 0, endpoint?: string) {
  let from = normalizeSalesPeriodDate(period?.from)
  let to = normalizeSalesPeriodDate(period?.to)
  if (!from && to) from = to
  if (from && !to) to = from
  if (from && to && from > to) [from, to] = [to, from]
  if (from && to) {
    const [fromYear, fromMonth, fromDay] = from.split('-').map((x) => Number(x))
    const [toYear, toMonth, toDay] = to.split('-').map((x) => Number(x))
    const body = {
      dir: 'DESC',
      filter: {
        since: buildUtcDayBoundaryIso(fromYear, fromMonth, fromDay, 'start'),
        to: buildUtcDayBoundaryIso(toYear, toMonth, toDay, 'end'),
      },
      limit,
      offset,
    }
    if (endpoint?.includes('/posting/fbs/')) {
      ;(body as any).with = {
        analytics_data: true,
        financial_data: true,
        barcodes: true,
        related_postings: true,
      }
    }
    return body
  }
  const now = new Date()
  const since = new Date(now.getTime() - (90 * 24 * 60 * 60 * 1000))
  const body = {
    dir: 'DESC',
    filter: {
      since: since.toISOString(),
      to: now.toISOString(),
    },
    limit,
    offset,
  }
  if (endpoint?.includes('/posting/fbs/')) {
    ;(body as any).with = {
      analytics_data: true,
      financial_data: true,
      barcodes: true,
      related_postings: true,
    }
  }
  return body
}

export async function fetchSalesEndpointPages(
  loader: (body: any) => Promise<any>,
  period: SalesPeriod | null | undefined,
  endpoint: string,
): Promise<SalesPayloadEnvelope[]> {
  const hasExplicitPeriod = Boolean(normalizeSalesPeriodDate(period?.from) || normalizeSalesPeriodDate(period?.to))
  const limit = 1000
  const maxPages = hasExplicitPeriod ? 100 : 1
  const payloads: SalesPayloadEnvelope[] = []
  for (let page = 0; page < maxPages; page++) {
    const offset = page * limit
    const payload = await loader(buildSalesRequestBody(period, limit, offset, endpoint))
    payloads.push({ endpoint, payload })
    const postings = extractPostingsFromPayload(payload)
    if (postings.length < limit) break
  }
  return payloads
}

function shouldReplaceSalesRow(prev: SalesRow, next: SalesRow): boolean {
  const prevDelivered = String(prev?.delivery_date ?? '').trim()
  const nextDelivered = String(next?.delivery_date ?? '').trim()
  if (!prevDelivered && nextDelivered) return true
  if (prevDelivered && !nextDelivered) return false
  if (prevDelivered && nextDelivered) return nextDelivered > prevDelivered
  return false
}

export function getSalesPostingDetailsKey(endpointKind: 'FBS' | 'FBO' | '', postingNumber: string): string {
  return `${endpointKind}|${String(postingNumber ?? '').trim()}`
}

function extractSalesPostingResult(payload: any): any {
  const result = safeGetByPath(payload, 'result', null)
  if (result && typeof result === 'object') return result
  if (payload && typeof payload === 'object') return payload
  return null
}

function getFactDeliveryDateValue(source: any): string {
  return normalizeDateValue(pickFirstPresent(source, ['result.fact_delivery_date', 'fact_delivery_date']))
}

function getFallbackDeliveredDateValue(source: any): string {
  return normalizeDateValue(pickFirstPresent(source, [
    'result.delivered_at',
    'delivered_at',
    'result.delivered_date',
    'delivered_date',
  ]))
}

function getShipmentDateValue(detailPosting: any, posting: any, endpointKind: 'FBS' | 'FBO' | ''): string {
  if (endpointKind === 'FBO') {
    return resolveFboShipmentDateFromSources(detailPosting, posting)
  }

  if (endpointKind === 'FBS') {
    return normalizeDateValue(pickFirstPresentFromSources([
      'delivering_date',
      'shipment_date_actual',
      'shipped_at',
    ], detailPosting, posting))
  }

  return normalizeDateValue(pickFirstPresentFromSources([
    'delivering_date',
    'shipment_date_actual',
    'shipped_at',
  ], detailPosting, posting))
}

const SALES_DELIVERED_STATUS_KEYS = new Set([
  'delivered',
  'delivered_to_customer',
  'customer_received',
  'posting_delivered',
  'posting_delivered_to_customer',
  'posting_conditionally_delivered',
])

function hasDeliveredStatusSignal(posting: any): boolean {
  const statusCandidates = [
    pickFirstPresent(posting, ['status', 'result.status', 'state', 'result.state']),
    pickFirstPresent(posting, ['provider_status', 'result.provider_status']),
    pickFirstPresent(posting, ['new_state', 'result.new_state']),
  ]
  for (const candidate of statusCandidates) {
    const key = normalizeSalesLookupKey(candidate)
    if (key && SALES_DELIVERED_STATUS_KEYS.has(key)) return true
  }
  return false
}

const SALES_DELIVERY_FALLBACK_PATHS = [
  'result.customer_deliver_date',
  'customer_deliver_date',
]

function hasDeliveryDateSignal(posting: any): boolean {
  return Boolean(normalizeDateValue(pickFirstPresent(posting, SALES_DELIVERY_FALLBACK_PATHS)))
}

function getFallbackDeliveryDateValue(source: any): string {
  return normalizeDateValue(pickFirstPresent(source, SALES_DELIVERY_FALLBACK_PATHS)) || getFallbackDeliveredDateValue(source)
}

function resolvePostingDeliveryDate(_detailPosting: any, _posting: any): string {
  // По КС П «Дата доставки» для всех методов доставки берём только из postings-report CSV.
  // Из posting/detail API это поле в строки продаж не подмешиваем.
  return ''
}

function shouldFetchSalesPostingDetails(posting: any, endpointKind: 'FBS' | 'FBO' | ''): boolean {
  if (!posting || typeof posting !== 'object') return false
  if (endpointKind === 'FBO') {
    const hasRelated = Boolean(buildRelatedPostingsText(posting))
    const hasShipmentDate = Boolean(getShipmentDateValue(null, posting, 'FBO'))
    const hasDeliveryCluster = Boolean(normalizeTextValue(pickFirstPresent(posting, [
      'financial_data.cluster_to',
      'result.financial_data.cluster_to',
      'cluster_to',
      'result.cluster_to',
    ])))

    if (!hasRelated || !hasShipmentDate || !hasDeliveryCluster) return true
    return false
  }
  if (!buildRelatedPostingsText(posting)) return true
  if (!getShipmentDateValue(null, posting, endpointKind)) return true
  if (!normalizeTextValue(pickFirstPresent(posting, ['financial_data.cluster_to', 'result.financial_data.cluster_to', 'cluster_to', 'result.cluster_to']))) return true
  return false
}

async function fetchSingleSalesPostingDetailWithRetry(secrets: any, request: { endpointKind: 'FBS' | 'FBO'; postingNumber: string }): Promise<any> {
  const retryDelaysMs = [0, 250, 750, 1500]
  let lastError: unknown = null
  for (const delayMs of retryDelaysMs) {
    if (delayMs > 0) await sleep(delayMs)
    try {
      return request.endpointKind === 'FBS'
        ? await ozonPostingFbsGet(secrets, request.postingNumber)
        : await ozonPostingFboGet(secrets, request.postingNumber)
    } catch (error) {
      lastError = error
    }
  }
  throw lastError instanceof Error ? lastError : new Error('Не удалось получить детали отправления')
}

export async function fetchSalesPostingDetails(
  secrets: any,
  payloads: SalesPayloadEnvelope[],
  existingDetailsByKey?: Map<string, any>,
): Promise<Map<string, any>> {
  const requests: Array<{ endpointKind: 'FBS' | 'FBO'; postingNumber: string }> = []
  const out = new Map<string, any>(existingDetailsByKey ?? [])
  const seen = new Set<string>()
  for (const envelope of payloads) {
    const endpointKind = normalizeSalesEndpointName(envelope.endpoint)
    if (endpointKind !== 'FBS' && endpointKind !== 'FBO') continue
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
      if (!postingNumber || !shouldFetchSalesPostingDetails(posting, endpointKind)) continue
      const requestKey = getSalesPostingDetailsKey(endpointKind, postingNumber)
      if (out.has(requestKey) || seen.has(requestKey)) continue
      seen.add(requestKey)
      requests.push({ endpointKind, postingNumber })
    }
  }
  if (requests.length === 0) return out
  for (const batch of chunk(requests, 4)) {
    const settled = await Promise.allSettled(batch.map(async (request) => ({
      request,
      payload: await fetchSingleSalesPostingDetailWithRetry(secrets, request),
    })))
    for (const result of settled) {
      if (result.status !== 'fulfilled') continue
      const detailPosting = extractSalesPostingResult(result.value.payload)
      if (!detailPosting) continue
      const endpointKind = result.value.request.endpointKind
      const postingNumber = normalizeTextValue(pickFirstPresent(detailPosting, ['posting_number', 'postingNumber'])) || result.value.request.postingNumber
      if (!postingNumber) continue
      out.set(getSalesPostingDetailsKey(endpointKind, postingNumber), detailPosting)
    }
    if (batch.length === 4) await sleep(120)
  }
  return out
}


function pickReportRowValue(row: SalesPostingsReportRow | null | undefined, keys: string[]): string {
  const rawRow = row && typeof row.raw_row === 'object' && row.raw_row ? row.raw_row : {}
  for (const key of keys) {
    const value = normalizeTextValue((rawRow as any)?.[key])
    if (value) return value
  }
  return ''
}

function buildSalesRowFromReportRow(
  reportRow: SalesPostingsReportRow,
  productsByOfferId: Map<string, GridApiRow>,
  productsBySku: Map<string, GridApiRow>,
): SalesRow | null {
  const postingNumber = normalizeTextValue(reportRow?.posting_number)
  const sku = normalizeTextValue(reportRow?.sku)
  if (!postingNumber || !sku) return null
  const offerId = normalizeTextValue(reportRow?.offer_id)
  const productMeta = productsByOfferId.get(offerId) ?? productsBySku.get(sku) ?? null
  return {
    ...(productMeta ?? {}),
    offer_id: offerId || String((productMeta as any)?.offer_id ?? ''),
    sku,
    name: normalizeTextValue(reportRow?.product_name) || String((productMeta as any)?.name ?? ''),
    in_process_at: normalizeDateValue((reportRow as any)?.in_process_at) || '',
    posting_number: postingNumber,
    related_postings: '',
    shipment_date: normalizeDateValue(reportRow?.shipment_date) || '',
    shipment_origin: normalizeTextValue(reportRow?.shipment_origin) || '',
    status: normalizeTextValue(reportRow?.status) || '',
    status_details: '',
    carrier_status_details: '',
    delivery_date: normalizeDateValue(reportRow?.delivery_date) || '',
    delivery_cluster: pickReportRowValue(reportRow, ['Кластер отгрузки', 'Кластер отправления', 'cluster_from']) || '',
    delivery_model: normalizeTextValue(reportRow?.delivery_schema) || '',
    currency: pickReportRowValue(reportRow, ['Код валюты отправления', 'currency']) || '',
    item_currency: pickReportRowValue(reportRow, ['Код валюты товара', 'item_currency']) || '',
    customer_currency_in_item_currency: '',
    price: normalizeNumberValue(reportRow?.price),
    quantity: normalizeNumberValue(reportRow?.quantity),
    paid_by_customer: normalizeNumberValue(reportRow?.paid_by_customer),
  }
}

function mergeSalesRowWithReportRow(prev: SalesRow, reportRow: SalesPostingsReportRow): SalesRow {
  const next = { ...prev }
  const assignText = (field: keyof SalesRow, value: string) => {
    if (!normalizeTextValue((next as any)[field]) && normalizeTextValue(value)) {
      ;(next as any)[field] = value
    }
  }
  const assignNumber = (field: keyof SalesRow, value: number | '') => {
    if (normalizeNumberValue((next as any)[field]) === '' && normalizeNumberValue(value) !== '') {
      ;(next as any)[field] = value
    }
  }
  assignText('in_process_at', normalizeDateValue((reportRow as any)?.in_process_at) || '')
  assignText('shipment_date', normalizeDateValue(reportRow?.shipment_date) || '')
  assignText('shipment_origin', normalizeTextValue(reportRow?.shipment_origin) || '')
  assignText('status', normalizeTextValue(reportRow?.status) || '')
  assignText('delivery_date', normalizeDateValue(reportRow?.delivery_date) || '')
  assignText('delivery_model', normalizeTextValue(reportRow?.delivery_schema) || '')
  assignText('delivery_cluster', pickReportRowValue(reportRow, ['Кластер отгрузки', 'Кластер отправления', 'cluster_from']) || '')
  assignText('currency', pickReportRowValue(reportRow, ['Код валюты отправления', 'currency']) || '')
  assignText('item_currency', pickReportRowValue(reportRow, ['Код валюты товара', 'item_currency']) || '')
  assignNumber('price', normalizeNumberValue(reportRow?.price))
  assignNumber('quantity', normalizeNumberValue(reportRow?.quantity))
  assignNumber('paid_by_customer', normalizeNumberValue(reportRow?.paid_by_customer))
  return next
}

export function normalizeSalesRows(
  payloads: SalesPayloadEnvelope[],
  products: GridApiRow[],
  postingDetailsByKey?: Map<string, any>,
  reportRows: SalesPostingsReportRow[] = [],
): SalesRow[] {
  const productsByOfferId = new Map<string, GridApiRow>()
  const productsBySku = new Map<string, GridApiRow>()
  for (const product of products) {
    const offerId = normalizeTextValue((product as any)?.offer_id)
    const sku = normalizeTextValue((product as any)?.sku)
    if (offerId && !productsByOfferId.has(offerId)) productsByOfferId.set(offerId, product)
    if (sku && !productsBySku.has(sku)) productsBySku.set(sku, product)
  }
  const dedup = new Map<string, SalesRow>()
  const fboOrderPostingMap = new Map<string, Set<string>>()
  const fboOrderKeyByPosting = new Map<string, string>()
  for (const envelope of payloads) {
    if (normalizeSalesEndpointName(envelope.endpoint) !== 'FBO') continue
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const orderKey = normalizeTextValue(pickFirstPresent(posting, ['order_id', 'order_number']))
      const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
      if (!orderKey || !postingNumber) continue
      fboOrderKeyByPosting.set(postingNumber, orderKey)
      let bucket = fboOrderPostingMap.get(orderKey)
      if (!bucket) {
        bucket = new Set<string>()
        fboOrderPostingMap.set(orderKey, bucket)
      }
      bucket.add(postingNumber)
    }
  }
  for (const envelope of payloads) {
    const endpointKind = normalizeSalesEndpointName(envelope.endpoint)
    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const items = Array.isArray((posting as any)?.products)
        ? (posting as any).products
        : (Array.isArray((posting as any)?.items) ? (posting as any).items : [])
      if (items.length === 0) continue
      const acceptedAt = normalizeDateValue(pickFirstPresent(posting, ['in_process_at', 'created_at', 'acceptance_date']))
      const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
      const orderKey = normalizeTextValue(pickFirstPresent(posting, ['order_id', 'order_number']))
      const fallbackRelated = endpointKind === 'FBO' && orderKey
        ? Array.from(fboOrderPostingMap.get(orderKey) ?? []).filter((value) => value !== postingNumber)
        : []
      const detailPosting = postingDetailsByKey?.get(getSalesPostingDetailsKey(endpointKind, postingNumber)) ?? null
      const related = buildRelatedPostingsText(detailPosting, fallbackRelated, posting)
      const shipmentDate = getShipmentDateValue(detailPosting, posting, endpointKind)
      const status = translateSalesCodeValue(pickFirstPresentFromSources(['status', 'state', 'result.status', 'result.state'], detailPosting, posting), 'status')
      const statusDetails = buildSalesStatusDetailsValue(detailPosting, envelope.endpoint, posting)
      const carrierStatusDetails = buildSalesCarrierStatusDetailsValue(detailPosting, posting)
      const deliveredAt = resolvePostingDeliveryDate(detailPosting, posting)
      const deliveryCluster = normalizeTextValue(pickFirstPresentFromSources(['financial_data.cluster_to', 'result.financial_data.cluster_to', 'cluster_to', 'result.cluster_to'], detailPosting, posting))
      const deliverySchema = buildDeliveryModelValue(posting, detailPosting, envelope.endpoint)
      const shipmentOrigin = resolveSalesShipmentOriginValue(detailPosting, posting, envelope.endpoint, deliverySchema)
      if (!postingNumber) continue
      for (const item of items) {
        const sku = normalizeTextValue(pickFirstPresent(item, ['sku', 'sku_id', 'id']))
        if (!sku) continue
        const offerId = normalizeTextValue(pickFirstPresent(item, ['offer_id', 'offerId', 'article']))
        const productMeta = productsByOfferId.get(offerId) ?? productsBySku.get(sku) ?? null
        const row: SalesRow = {
          ...(productMeta ?? {}),
          offer_id: offerId || String((productMeta as any)?.offer_id ?? ''),
          sku,
          name: normalizeTextValue(pickFirstPresent(item, ['name', 'product_name'])) || String((productMeta as any)?.name ?? ''),
          in_process_at: acceptedAt || '',
          posting_number: postingNumber,
          related_postings: related || '',
          shipment_date: shipmentDate || '',
          shipment_origin: shipmentOrigin || '',
          status: status || '',
          status_details: statusDetails || '',
          carrier_status_details: carrierStatusDetails || '',
          delivery_date: deliveredAt || '',
          delivery_cluster: deliveryCluster || '',
          delivery_model: deliverySchema || '',
          currency: resolveSalesItemCurrencyValue(item, detailPosting, posting) || '',
          item_currency: resolveSalesItemProductCurrencyValue(item, detailPosting) || '',
          customer_currency_in_item_currency: '',
          price: resolveSalesItemPriceValue(item),
          quantity: resolveSalesItemQuantityValue(item),
          paid_by_customer: resolveSalesItemPaidByCustomerValue(item, detailPosting, posting),
        }
        const dedupKey = `${postingNumber}|${sku}`
        const prev = dedup.get(dedupKey)
        if (!prev || shouldReplaceSalesRow(prev, row)) dedup.set(dedupKey, row)
      }
    }
  }
  const rows = Array.from(dedup.values())
  const fboOrderClusterMap = new Map<string, string>()
  for (const row of rows) {
    if (String(row?.delivery_model ?? '') !== 'FBO') continue
    const postingNumber = normalizeTextValue(row?.posting_number)
    const orderKey = postingNumber ? fboOrderKeyByPosting.get(postingNumber) : ''
    const cluster = normalizeTextValue(row?.delivery_cluster)
    if (!orderKey || !cluster || fboOrderClusterMap.has(orderKey)) continue
    fboOrderClusterMap.set(orderKey, cluster)
  }
  for (const row of rows) {
    if (String(row?.delivery_model ?? '') !== 'FBO') continue
    if (normalizeTextValue(row?.delivery_cluster)) continue
    const postingNumber = normalizeTextValue(row?.posting_number)
    const orderKey = postingNumber ? fboOrderKeyByPosting.get(postingNumber) : ''
    if (!orderKey) continue
    const fallbackCluster = normalizeTextValue(fboOrderClusterMap.get(orderKey))
    if (fallbackCluster) row.delivery_cluster = fallbackCluster
  }
  for (const reportRow of Array.isArray(reportRows) ? reportRows : []) {
    const postingNumber = normalizeTextValue(reportRow?.posting_number)
    const sku = normalizeTextValue(reportRow?.sku)
    if (!postingNumber || !sku) continue
    const dedupKey = `${postingNumber}|${sku}`
    const prev = dedup.get(dedupKey)
    if (!prev) {
      const rowFromReport = buildSalesRowFromReportRow(reportRow, productsByOfferId, productsBySku)
      if (rowFromReport) dedup.set(dedupKey, rowFromReport)
      continue
    }
    dedup.set(dedupKey, mergeSalesRowWithReportRow(prev, reportRow))
  }
  const rowsWithReportPaid = applySalesPaidByCustomerFromReportRows(Array.from(dedup.values()), reportRows).rows
  return rowsWithReportPaid.sort((a, b) => {
    const aAccepted = String(a?.in_process_at ?? '')
    const bAccepted = String(b?.in_process_at ?? '')
    if (aAccepted !== bAccepted) return bAccepted.localeCompare(aAccepted)
    const aPosting = String(a?.posting_number ?? '')
    const bPosting = String(b?.posting_number ?? '')
    if (aPosting !== bPosting) return bPosting.localeCompare(aPosting)
    return String(b?.sku ?? '').localeCompare(String(a?.sku ?? ''))
  })
}

export function buildSalesPaidByCustomerTrace(
  payloads: SalesPayloadEnvelope[],
  postingDetailsByKey?: Map<string, any>,
  rows?: SalesRow[],
  reportRows: SalesPostingsReportRow[] = [],
): SalesPaidByCustomerTrace {
  const postingKeys = new Set<string>()
  const postingsWithDetail = new Set<string>()
  const sampledDetailKeys = new Set<string>()
  const detailShapeSamples: string[] = []
  let totalItemCount = 0
  let listItemDirectValueCount = 0
  let listFinancialValueCount = 0
  let detailItemDirectValueCount = 0
  let detailFinancialValueCount = 0
  let detailWithFinancialDataObjectCount = 0
  let detailWithFinancialProductsArrayCount = 0
  let detailWithNonEmptyFinancialProductsCount = 0

  for (const envelope of payloads) {
    const endpointKind = normalizeSalesEndpointName(envelope.endpoint)
    if (endpointKind !== 'FBS' && endpointKind !== 'FBO') continue

    for (const posting of extractPostingsFromPayload(envelope.payload)) {
      const postingNumber = normalizeTextValue(pickFirstPresent(posting, ['posting_number', 'postingNumber']))
      if (!postingNumber) continue

      const postingKey = getSalesPostingDetailsKey(endpointKind, postingNumber)
      postingKeys.add(postingKey)
      const detailPosting = postingDetailsByKey?.get(postingKey) ?? null
      if (detailPosting) {
        postingsWithDetail.add(postingKey)

        const financialData = pickFirstPresent(detailPosting, ['financial_data', 'result.financial_data'])
        if (financialData && typeof financialData === 'object') detailWithFinancialDataObjectCount += 1

        const financialProductsRaw = pickFirstPresent(detailPosting, ['financial_data.products', 'result.financial_data.products'])
        if (Array.isArray(financialProductsRaw)) detailWithFinancialProductsArrayCount += 1

        const financialProducts = extractFinancialProducts(detailPosting)
        if (financialProducts.length > 0) detailWithNonEmptyFinancialProductsCount += 1

        if (detailShapeSamples.length < 5 && !sampledDetailKeys.has(postingKey)) {
          sampledDetailKeys.add(postingKey)
          detailShapeSamples.push(summarizePaidByCustomerDetailShape(postingNumber, detailPosting))
        }
      }

      for (const item of extractPostingItems(posting)) {
        totalItemCount += 1

        const listDirectValue = normalizeNumberValue(pickFirstPresent(item, ['client_price', 'clientPrice', 'paid_by_customer', 'paidByCustomer']))
        if (listDirectValue !== '') listItemDirectValueCount += 1

        const listFinancialProduct = findSalesFinancialProductInSources(item, [posting])
        const listFinancialValue = normalizeNumberValue(pickFirstPresent(listFinancialProduct, ['client_price', 'clientPrice']))
        if (listFinancialValue !== '') listFinancialValueCount += 1

        if (detailPosting) {
          const detailItem = findMatchingSalesItemInSource(item, detailPosting)
          const detailDirectValue = normalizeNumberValue(pickFirstPresent(detailItem, ['client_price', 'clientPrice', 'paid_by_customer', 'paidByCustomer']))
          if (detailDirectValue !== '') detailItemDirectValueCount += 1

          const detailFinancialProduct = findSalesFinancialProductInSources(item, [detailPosting])
          const detailFinancialValue = normalizeNumberValue(pickFirstPresent(detailFinancialProduct, ['client_price', 'clientPrice']))
          if (detailFinancialValue !== '') detailFinancialValueCount += 1
        }
      }
    }
  }

  const finalRows = Array.isArray(rows) ? rows : []
  const reportMapStats = buildSalesPaidByCustomerReportMap(reportRows)
  let reportMatchedRowsCount = 0
  let reportResolvedRowsCount = 0
  for (const row of finalRows) {
    const reportPaid = resolveSalesPaidByCustomerFromReportRow(row, reportMapStats.valueByKey)
    if (reportPaid === '') continue
    reportMatchedRowsCount += 1
    if (normalizeNumberValue(row?.paid_by_customer) !== '') reportResolvedRowsCount += 1
  }
  const rowsWithPaid = finalRows.filter((row) => normalizeNumberValue(row?.paid_by_customer) !== '')
  const rowsWithoutPaid = finalRows.filter((row) => normalizeNumberValue(row?.paid_by_customer) === '')
  const uniqueMissingPostingNumbers = Array.from(new Set(
    rowsWithoutPaid
      .map((row) => normalizeTextValue(row?.posting_number))
      .filter(Boolean),
  ))

  const countRowsWithPaidByModel = (modelRaw: string): number => {
    const model = normalizeTextValue(modelRaw).toUpperCase()
    return rowsWithPaid.filter((row) => normalizeTextValue(row?.delivery_model).toUpperCase() === model).length
  }

  return {
    totalPostingCount: postingKeys.size,
    totalItemCount,
    postingsWithDetailCount: postingsWithDetail.size,
    listItemDirectValueCount,
    listFinancialValueCount,
    detailItemDirectValueCount,
    detailFinancialValueCount,
    detailWithFinancialDataObjectCount,
    detailWithFinancialProductsArrayCount,
    detailWithNonEmptyFinancialProductsCount,
    reportRowsCount: reportMapStats.reportRowsCount,
    reportRowsWithPaidByCustomerCount: reportMapStats.reportRowsWithPaidByCustomerCount,
    reportMatchedRowsCount,
    reportResolvedRowsCount,
    finalRowsCount: finalRows.length,
    finalRowsWithPaidByCustomer: rowsWithPaid.length,
    finalRowsWithoutPaidByCustomer: rowsWithoutPaid.length,
    fbsRowsWithPaidByCustomer: countRowsWithPaidByModel('FBS'),
    fboRowsWithPaidByCustomer: countRowsWithPaidByModel('FBO'),
    rfbsRowsWithPaidByCustomer: rowsWithPaid.filter((row) => normalizeTextValue(row?.delivery_model) === 'rFBS').length,
    missingPostingNumbers: uniqueMissingPostingNumbers.slice(0, 10),
    detailShapeSamples,
  }
}

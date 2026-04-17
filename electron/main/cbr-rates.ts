import { dbGetCbrRatesByDate, dbGetMissingCbrRateDays, dbSaveCbrRateDay, dbSaveCbrRates } from './storage/db'
import type { SalesRow } from './sales-sync'

const CBR_DAILY_URL = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req='
const CBR_REQUEST_TIMEOUT_MS = 20_000
const CBR_REQUEST_DELAY_MS = 180

function sleep(ms: number): Promise<void> {
  if (!Number.isFinite(ms) || ms <= 0) return Promise.resolve()
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function normalizeText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : String(value ?? '').trim()
}

function normalizeCurrency(value: unknown): string {
  const raw = normalizeText(value).toUpperCase()
  return /^[A-Z]{3}$/.test(raw) ? raw : ''
}

function normalizeDateKey(value: unknown): string {
  const raw = normalizeText(value)
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw
  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) return ''
  return parsed.toISOString().slice(0, 10)
}

function formatCbrDateParam(dateKey: string): string {
  const [year, month, day] = dateKey.split('-')
  return `${day}/${month}/${year}`
}

function parseCbrDateAttr(value: unknown): string | null {
  const raw = normalizeText(value)
  const match = raw.match(/^(\d{2})\.(\d{2})\.(\d{4})$/)
  if (!match) return null
  return `${match[3]}-${match[2]}-${match[1]}`
}

function decodeXmlText(value: string): string {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .trim()
}

function parseDecimal(value: unknown): number | null {
  const raw = normalizeText(value).replace(',', '.')
  const num = Number(raw)
  return Number.isFinite(num) ? num : null
}

function roundTo(value: number, digits = 6): number {
  const factor = 10 ** digits
  return Math.round(value * factor) / factor
}

function extractTag(block: string, tagName: string): string {
  const match = block.match(new RegExp(`<${tagName}>([\\s\\S]*?)</${tagName}>`, 'i'))
  return match ? decodeXmlText(match[1]) : ''
}

function parseCbrDailyXml(xml: string): { effectiveDate: string | null; rates: Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }> } {
  const rootMatch = xml.match(/<ValCurs[^>]*Date="([^"]+)"/i)
  const effectiveDate = parseCbrDateAttr(rootMatch?.[1] ?? '')
  const rates: Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }> = []
  const blocks = xml.match(/<Valute[\s\S]*?<\/Valute>/gi) ?? []

  for (const block of blocks) {
    const currencyCode = normalizeCurrency(extractTag(block, 'CharCode'))
    const nominal = parseDecimal(extractTag(block, 'Nominal'))
    const valueRub = parseDecimal(extractTag(block, 'Value'))
    if (!currencyCode || !nominal || !valueRub || nominal <= 0) continue
    rates.push({
      currencyCode,
      nominal: Math.max(1, Math.trunc(nominal)),
      valueRub,
      ratePerUnit: roundTo(valueRub / nominal, 10),
    })
  }

  return { effectiveDate, rates }
}

async function fetchCbrDailyRates(dateKey: string): Promise<{ effectiveDate: string | null; rates: Array<{ currencyCode: string; nominal: number; valueRub: number; ratePerUnit: number }> }> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), CBR_REQUEST_TIMEOUT_MS)
  try {
    const res = await fetch(`${CBR_DAILY_URL}${encodeURIComponent(formatCbrDateParam(dateKey))}`, {
      method: 'GET',
      headers: { Accept: 'application/xml, text/xml;q=0.9, */*;q=0.8' },
      signal: controller.signal,
    })
    const text = await res.text()
    if (!res.ok) {
      throw new Error(`CBR HTTP ${res.status}`)
    }
    return parseCbrDailyXml(text)
  } finally {
    clearTimeout(timeoutId)
  }
}

async function ensureCbrRateDaysLoaded(dateKeys: string[]): Promise<void> {
  const normalizedDates = Array.from(new Set(dateKeys.map((value) => normalizeDateKey(value)).filter(Boolean))).sort()
  if (normalizedDates.length === 0) return
  const missingDates = dbGetMissingCbrRateDays(normalizedDates)
  for (let index = 0; index < missingDates.length; index += 1) {
    const requestedDate = missingDates[index]
    try {
      const daily = await fetchCbrDailyRates(requestedDate)
      dbSaveCbrRates({
        requestedDate,
        effectiveDate: daily.effectiveDate,
        rates: daily.rates,
      })
      dbSaveCbrRateDay({
        requestedDate,
        effectiveDate: daily.effectiveDate,
        isSuccess: true,
        errorMessage: null,
      })
    } catch (error: any) {
      dbSaveCbrRateDay({
        requestedDate,
        effectiveDate: null,
        isSuccess: false,
        errorMessage: error?.message ? String(error.message) : String(error),
      })
    }
    if (index < missingDates.length - 1) await sleep(CBR_REQUEST_DELAY_MS)
  }
}

function resolveRatePerRub(cbrRatesByCurrency: Map<string, number>, currencyCode: string): number | null {
  const normalizedCurrency = normalizeCurrency(currencyCode)
  if (!normalizedCurrency) return null
  if (normalizedCurrency === 'RUB') return 1
  const rate = cbrRatesByCurrency.get(normalizedCurrency)
  return typeof rate === 'number' && Number.isFinite(rate) && rate > 0 ? rate : null
}

export async function applyCbrConversionsToSalesRows(rows: SalesRow[]): Promise<SalesRow[]> {
  const salesRows = Array.isArray(rows) ? rows : []
  const neededDates = new Set<string>()

  for (const row of salesRows) {
    const itemCurrency = normalizeCurrency(row?.item_currency)
    const customerCurrency = normalizeCurrency(row?.currency)
    if (!itemCurrency || !customerCurrency) continue
    if (itemCurrency === customerCurrency) continue
    const dateKey = normalizeDateKey(row?.in_process_at)
    if (!dateKey) continue
    neededDates.add(dateKey)
  }

  try {
    await ensureCbrRateDaysLoaded(Array.from(neededDates))
  } catch {
    // fail-soft: продажи должны отображаться даже если ЦБ временно недоступен
  }

  const rateCache = new Map<string, Map<string, number>>()
  for (const dateKey of neededDates) {
    const map = new Map<string, number>()
    for (const entry of dbGetCbrRatesByDate(dateKey)) {
      const currencyCode = normalizeCurrency(entry.currencyCode)
      const ratePerUnit = Number(entry.ratePerUnit)
      if (!currencyCode || !Number.isFinite(ratePerUnit) || ratePerUnit <= 0) continue
      map.set(currencyCode, ratePerUnit)
    }
    rateCache.set(dateKey, map)
  }

  return salesRows.map((row) => {
    const nextRow: SalesRow = { ...row, customer_currency_in_item_currency: '' }
    const paidByCustomer = Number(row?.paid_by_customer)
    if (!Number.isFinite(paidByCustomer)) return nextRow

    const itemCurrency = normalizeCurrency(row?.item_currency)
    const customerCurrency = normalizeCurrency(row?.currency)
    if (!itemCurrency || !customerCurrency) return nextRow

    if (itemCurrency === customerCurrency) {
      nextRow.customer_currency_in_item_currency = roundTo(paidByCustomer, 6)
      return nextRow
    }

    const dateKey = normalizeDateKey(row?.in_process_at)
    if (!dateKey) return nextRow
    const ratesByCurrency = rateCache.get(dateKey) ?? new Map<string, number>()
    const customerToRub = resolveRatePerRub(ratesByCurrency, customerCurrency)
    const itemToRub = resolveRatePerRub(ratesByCurrency, itemCurrency)
    if (!customerToRub || !itemToRub) return nextRow

    nextRow.customer_currency_in_item_currency = roundTo((paidByCustomer * customerToRub) / itemToRub, 6)
    return nextRow
  })
}

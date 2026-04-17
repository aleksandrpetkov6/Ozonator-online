export type UiDateRange = {
  from: string
  to: string
}

export const UI_DATE_RANGE_LS_KEY = 'ozonator_ui_date_range_v2'
export const DEFAULT_UI_DATE_RANGE_DAYS = 30

const DATE_INPUT_RE = /^\d{4}-\d{2}-\d{2}$/
const MOSCOW_TIME_ZONE = 'Europe/Moscow'

function pad(value: number): string {
  return String(value).padStart(2, '0')
}

function toUtcDateFromInput(value: string): Date | null {
  if (!DATE_INPUT_RE.test(value)) return null
  const [yearRaw, monthRaw, dayRaw] = value.split('-')
  const year = Number(yearRaw)
  const month = Number(monthRaw)
  const day = Number(dayRaw)
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null
  return new Date(Date.UTC(year, month - 1, day))
}

function toDateInputValue(date: Date): string {
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`
}

function getTimeZoneTodayValue(timeZone: string): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date())

  const year = parts.find((part) => part.type === 'year')?.value ?? ''
  const month = parts.find((part) => part.type === 'month')?.value ?? ''
  const day = parts.find((part) => part.type === 'day')?.value ?? ''

  if (!DATE_INPUT_RE.test(`${year}-${month}-${day}`)) {
    const fallback = new Date()
    return `${fallback.getFullYear()}-${pad(fallback.getMonth() + 1)}-${pad(fallback.getDate())}`
  }

  return `${year}-${month}-${day}`
}

export function sanitizeDateInput(value: unknown): string {
  const raw = typeof value === 'string' ? value.trim() : ''
  return DATE_INPUT_RE.test(raw) ? raw : ''
}

export function getDefaultDateRange(days = DEFAULT_UI_DATE_RANGE_DAYS): UiDateRange {
  const safeDays = Math.max(1, Math.trunc(Number(days) || DEFAULT_UI_DATE_RANGE_DAYS))
  const today = toUtcDateFromInput(getTimeZoneTodayValue(MOSCOW_TIME_ZONE))
  const end = today ?? new Date()

  const start = new Date(end.getTime())
  start.setUTCDate(start.getUTCDate() - safeDays)

  return {
    from: toDateInputValue(start),
    to: toDateInputValue(end),
  }
}

export function readDateRangeFromStorage(storageKey = UI_DATE_RANGE_LS_KEY): UiDateRange | undefined {
  try {
    const raw = localStorage.getItem(storageKey)
    if (!raw) return undefined

    const parsed = JSON.parse(raw) as Partial<UiDateRange>
    const from = sanitizeDateInput(parsed.from)
    const to = sanitizeDateInput(parsed.to)
    if (!from && !to) return undefined
    return { from, to }
  } catch {
    return undefined
  }
}

export function readDateRangeWithDefault(
  storageKey = UI_DATE_RANGE_LS_KEY,
  fallbackDays = DEFAULT_UI_DATE_RANGE_DAYS,
): UiDateRange {
  return readDateRangeFromStorage(storageKey) ?? getDefaultDateRange(fallbackDays)
}

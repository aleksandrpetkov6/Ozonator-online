export type DateOnlyBoundary = 'keep' | 'startOfDay' | 'endOfDay'

const DATE_ONLY_RE = /^(\d{4})-(\d{2})-(\d{2})$/
const TIME_ONLY_RE = /^([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d)(?:[.,]\d+)?)?$/

function pad(n: number) {
  return String(n).padStart(2, '0')
}

function formatDateRu(parsed: Date): string {
  const dd = pad(parsed.getDate())
  const mm = pad(parsed.getMonth() + 1)
  const yy = pad(parsed.getFullYear() % 100)
  return `${dd}.${mm}.${yy}.`
}

function formatDateCompactRu(parsed: Date): string {
  const dd = pad(parsed.getDate())
  const mm = pad(parsed.getMonth() + 1)
  const yy = pad(parsed.getFullYear() % 100)
  return `${dd}.${mm}.${yy}`
}

function formatTimeRu(value: string): string {
  const match = TIME_ONLY_RE.exec(value.trim())
  if (!match) return value
  const hh = match[1]
  const mi = match[2]
  const ss = match[3] ?? '00'
  return `${hh}:${mi}:${ss}`
}

function parseDateOnlyLocal(value: string, boundary: DateOnlyBoundary): Date | null {
  const match = DATE_ONLY_RE.exec(value.trim())
  if (!match) return null

  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null

  const isEnd = boundary === 'endOfDay'
  return new Date(
    year,
    month - 1,
    day,
    isEnd ? 23 : 0,
    isEnd ? 59 : 0,
    isEnd ? 59 : 0,
    isEnd ? 999 : 0,
  )
}

function toDate(value: unknown, boundary: DateOnlyBoundary): Date | null {
  if (value == null || value === '') return null

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : new Date(value.getTime())
  }

  if (typeof value === 'string') {
    const localDateOnly = parseDateOnlyLocal(value, boundary)
    if (localDateOnly) return localDateOnly
  }

  const parsed = new Date(value as any)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

function looksLikeTemporalText(value: string): boolean {
  const trimmed = value.trim()
  if (!trimmed) return false
  if (DATE_ONLY_RE.test(trimmed)) return true
  if (TIME_ONLY_RE.test(trimmed)) return true
  if (/^\d{4}-\d{2}-\d{2}[T\s]/.test(trimmed)) return true
  return false
}

export function isTemporalColumnId(columnId: unknown): boolean {
  const normalized = String(columnId ?? '').trim().toLowerCase()
  if (!normalized) return false
  if (/_at$/.test(normalized)) return true
  if (/(?:^|_)(date|time)(?:_|$)/.test(normalized)) return true
  return false
}

export function formatDateTimeRu(value: unknown, options?: { dateOnlyBoundary?: DateOnlyBoundary }): string {
  if (value == null || value === '') return ''

  const parsed = toDate(value, options?.dateOnlyBoundary ?? 'keep')
  if (!parsed) return String(value)

  const dd = pad(parsed.getDate())
  const mm = pad(parsed.getMonth() + 1)
  const yy = pad(parsed.getFullYear() % 100)
  const hh = pad(parsed.getHours())
  const mi = pad(parsed.getMinutes())
  const ss = pad(parsed.getSeconds())

  return `${dd}.${mm}.${yy} ${hh}:${mi}:${ss}`
}

export function formatTemporalValueRu(value: unknown, options?: { columnId?: unknown }): string {
  if (value == null || value === '') return ''

  if (typeof value === 'string') {
    const raw = value.trim()
    if (!raw) return ''
    if (TIME_ONLY_RE.test(raw)) return formatTimeRu(raw)
    if (DATE_ONLY_RE.test(raw)) {
      const parsedDate = toDate(raw, 'keep')
      return parsedDate ? formatDateRu(parsedDate) : raw
    }
    if (!looksLikeTemporalText(raw) && !isTemporalColumnId(options?.columnId)) return value
  } else if (!(value instanceof Date) && !isTemporalColumnId(options?.columnId)) {
    return String(value)
  }

  const parsed = toDate(value, 'keep')
  if (!parsed) return String(value)
  return formatDateTimeRu(parsed)
}

export function formatTemporalCellRu(columnId: unknown, value: unknown): string {
  if (!isTemporalColumnId(columnId)) {
    if (value == null || value === '') return ''
    return typeof value === 'string' ? value : String(value)
  }

  const normalizedColumnId = String(columnId ?? '').trim().toLowerCase()
  if (normalizedColumnId === 'delivery_date') {
    const parsed = toDate(value, 'keep')
    if (parsed && parsed.getMinutes() === 0 && parsed.getSeconds() === 0 && parsed.getMilliseconds() === 0) {
      return formatDateCompactRu(parsed)
    }
  }

  return formatTemporalValueRu(value, { columnId })
}

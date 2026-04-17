import React, { useEffect, useMemo, useState } from 'react'
import { formatTemporalCellRu } from '../utils/dateTime'
import { getSortButtonTitle, type SortableColumn, type TableSortState, sortTableRows, toggleTableSort } from '../utils/tableSort'

type LogRow = {
  id: number
  type: string
  status: string
  started_at: string
  finished_at: string | null
  items_count: number | null
  error_message: string | null
  error_details: string | null
  meta: string | null
}

type LogSortCol = 'id' | 'type' | 'status' | 'started_at' | 'finished_at' | 'details' | 'error_message'
type LogSortState = TableSortState<LogSortCol>
type LogColDef = SortableColumn<LogRow, LogSortCol> & { id: LogSortCol; title: string }
type LogGroup = { row: LogRow; children: LogRow[] }
type DownloadState = 'idle' | 'saving' | 'done' | 'error'
type TraceCategoryKey = 'shipment' | 'delivery' | 'origin' | 'status' | 'paid' | 'other'
type TraceMeta = Record<string, any>
type TraceEvent = {
  row: LogRow
  meta: TraceMeta
  category: TraceCategoryKey
  title: string
  summary: string
}
type TraceMetric = { label: string; value: string }
type TraceSection = {
  key: TraceCategoryKey
  title: string
  intro: string
  events: TraceEvent[]
  metrics: TraceMetric[]
  notes: string[]
  errors: string[]
}

const TYPE_RU: Record<string, string> = {
  sync_products: 'Синхронизация',
  check_auth: 'Проверка доступа',
  app_install: 'Установка программы',
  app_update: 'Обновление программы',
  app_reinstall: 'Переустановка программы',
  app_uninstall: 'Удаление программы',
  admin_settings: 'Настройки админки',
  sales_fbo_shipment_trace: 'Трассировка синхронизации продаж',
}

const STATUS_RU: Record<string, string> = {
  pending: 'Ожидает',
  running: 'В процессе',
  success: 'Успешно',
  error: 'Ошибка',
}

const TRACE_TYPES = new Set(['sales_fbo_shipment_trace'])
const TRACE_PARENT_TYPES = new Set(['sync_products'])
const TRACE_SECTION_ORDER: Record<TraceCategoryKey, number> = {
  shipment: 1,
  delivery: 2,
  origin: 3,
  status: 4,
  paid: 5,
  other: 6,
}

function typeRu(v?: string | null) {
  if (!v) return '-'
  return TYPE_RU[v] ?? v
}

function statusRu(v?: string | null) {
  if (!v) return '-'
  return STATUS_RU[v] ?? v
}

function fmtDt(colId: 'started_at' | 'finished_at', value?: string | null) {
  if (!value) return '-'
  return formatTemporalCellRu(colId, value) || '-'
}

function normalizeText(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function detailsRu(type?: string | null, meta?: string | null, itemsCount?: number | null): string {
  if (type === 'app_uninstall') return 'Удаление выполнено.'
  const safeItemsCount = typeof itemsCount === 'number' && Number.isFinite(itemsCount) ? itemsCount : null
  if (!meta) {
    return safeItemsCount != null ? `Обработано записей: ${safeItemsCount}.` : '—'
  }

  try {
    const m = JSON.parse(meta)
    if (typeof m?.updated === 'number' || typeof m?.added === 'number') {
      const synced = safeItemsCount ?? Math.max(0, Number(m?.updated) || 0)
      const added = Math.max(0, Number(m?.added) || 0)
      return `Синхронизировано: ${synced}. Новых записей: ${added}.`
    }
    if (typeof m?.logRetentionDays === 'number') return `Срок хранения журнала: ${m.logRetentionDays} дн.`
    if (m?.appVersion || m?.previousVersion) {
      const parts: string[] = []
      if (m?.appVersion) parts.push(`Текущая версия: ${m.appVersion}.`)
      if (m?.previousVersion) parts.push(`Предыдущая версия: ${m.previousVersion}.`)
      return parts.join(' ')
    }
    if (typeof m?.pages === 'number') {
      const parts = [`Страниц загружено: ${m.pages}.`]
      if (typeof m?.infoBatches === 'number') parts.push(`Пакетов деталей: ${m.infoBatches}.`)
      if (typeof m?.infoFetched === 'number') parts.push(`Расширенных записей: ${m.infoFetched}.`)
      return parts.join(' ')
    }
  } catch {
    return meta
  }

  return safeItemsCount != null ? `Обработано записей: ${safeItemsCount}.` : '—'
}

function toSortTimestamp(value?: string | null): number | null {
  if (!value) return null
  const time = Date.parse(String(value))
  return Number.isFinite(time) ? time : null
}

function getTraceParentId(traceRow: LogRow, syncRows: LogRow[]): number | null {
  const traceTime = toSortTimestamp(traceRow.started_at) ?? toSortTimestamp(traceRow.finished_at)
  let bestByTimeWindow: { id: number; startedAt: number } | null = null
  let bestPreviousByTime: { id: number; startedAt: number } | null = null
  let bestPreviousById: number | null = null

  for (const syncRow of syncRows) {
    if (syncRow.id < traceRow.id) {
      bestPreviousById = syncRow.id
    }

    const syncStart = toSortTimestamp(syncRow.started_at)
    if (traceTime == null || syncStart == null) continue

    if (traceTime >= syncStart) {
      if (!bestPreviousByTime || syncStart >= bestPreviousByTime.startedAt) {
        bestPreviousByTime = { id: syncRow.id, startedAt: syncStart }
      }
    }

    const syncFinishRaw = toSortTimestamp(syncRow.finished_at)
    const syncFinish = syncFinishRaw == null ? (syncStart + 10 * 60_000) : (syncFinishRaw + 2 * 60_000)
    if (traceTime < syncStart || traceTime > syncFinish) continue

    if (!bestByTimeWindow || syncStart >= bestByTimeWindow.startedAt) {
      bestByTimeWindow = { id: syncRow.id, startedAt: syncStart }
    }
  }

  return bestByTimeWindow?.id ?? bestPreviousByTime?.id ?? bestPreviousById
}

function buildLogGroups(rows: LogRow[]): LogGroup[] {
  const topLevelRows = rows.filter((row) => !TRACE_TYPES.has(row.type))
  const childMap = new Map<number, LogRow[]>()
  const syncRows = topLevelRows
    .filter((row) => TRACE_PARENT_TYPES.has(row.type))
    .slice()
    .sort((a, b) => a.id - b.id)

  const traceRows = rows
    .filter((row) => TRACE_TYPES.has(row.type))
    .slice()
    .sort((a, b) => {
      const timeDiff = (toSortTimestamp(a.started_at) ?? 0) - (toSortTimestamp(b.started_at) ?? 0)
      if (timeDiff !== 0) return timeDiff
      return a.id - b.id
    })

  for (const traceRow of traceRows) {
    const parentId = getTraceParentId(traceRow, syncRows)
    if (parentId == null) continue
    const bucket = childMap.get(parentId) ?? []
    bucket.push(traceRow)
    childMap.set(parentId, bucket)
  }

  return topLevelRows.map((row) => ({
    row,
    children: (childMap.get(row.id) ?? []).slice().sort((a, b) => {
      const timeDiff = (toSortTimestamp(a.started_at) ?? 0) - (toSortTimestamp(b.started_at) ?? 0)
      if (timeDiff !== 0) return timeDiff
      return a.id - b.id
    }),
  }))
}

function hasRowExtraPanel(row: LogRow): boolean {
  return Boolean(escapeTxtLine(row.error_details))
}

function hasExpandableDetails(group: LogGroup): boolean {
  return TRACE_PARENT_TYPES.has(group.row.type) && (group.children.length > 0 || hasRowExtraPanel(group.row))
}

function getMainRowDetails(row: LogRow, traceCount: number): string {
  const base = detailsRu(row.type, row.meta, row.items_count)
  if (!traceCount) return base
  if (base && base !== '—') return `${base} Этапов трассировки: ${traceCount}.`
  return `Этапов трассировки: ${traceCount}.`
}

function escapeTxtLine(value?: string | null): string {
  return String(value ?? '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim()
}

function formatFileStamp(value?: string | null): string {
  const date = value ? new Date(value) : new Date()
  if (Number.isNaN(date.getTime())) return '00.00.00 00：00：00'
  const dd = String(date.getDate()).padStart(2, '0')
  const mm = String(date.getMonth() + 1).padStart(2, '0')
  const yy = String(date.getFullYear()).slice(-2)
  const hh = String(date.getHours()).padStart(2, '0')
  const min = String(date.getMinutes()).padStart(2, '0')
  const ss = String(date.getSeconds()).padStart(2, '0')
  return `${dd}.${mm}.${yy} ${hh}：${min}：${ss}`
}

function parseMeta(meta?: string | null): TraceMeta {
  if (!meta) return {}
  try {
    const parsed = JSON.parse(meta)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function classifyTraceCategory(meta: TraceMeta): TraceCategoryKey {
  const stage = normalizeText(meta?.stage)
  const traceKind = normalizeText(meta?.traceKind)
  if (traceKind === 'paid_by_customer' || stage.includes('paid_by_customer')) return 'paid'
  if (stage.includes('origin.rows.built')) return 'origin'
  if (stage.includes('status.rows.built')) return 'status'
  if (stage.startsWith('sales.snapshot.') || stage.startsWith('sales.read.')) return 'delivery'
  if (stage.includes('report.') || stage.endsWith('.rows.built')) return 'delivery'
  if (stage.includes('webhook') || stage.includes('push.') || stage.includes('api.refresh') || stage.includes('raw-cache.rebuild')) return 'shipment'
  return 'other'
}

function formatStrategyLabel(strategy: unknown): string {
  const text = normalizeText(strategy)
  if (!text) return ''
  if (text === 'chunked-1d') return 'По дням'
  if (text === 'chunked-7d') return 'По неделям'
  return 'Один отчёт'
}

function createSentence(parts: Array<string | null | undefined>): string {
  const cleaned = parts.map((part) => normalizeText(part)).filter(Boolean)
  if (!cleaned.length) return ''
  return cleaned.join(' ')
}

function buildTraceStepTitle(meta: TraceMeta): string {
  const stage = normalizeText(meta?.stage)
  const map: Record<string, string> = {
    'api.refresh.begin': 'Запуск обновления',
    'api.refresh.list.loaded': 'Список отправлений загружен',
    'api.refresh.details.loaded': 'Детали отправлений загружены',
    'api.refresh.compat.loaded': 'Совместимые детали проверены',
    'api.refresh.report.begin': 'Формирование отчёта запущено',
    'api.refresh.report.created': 'Отчёт создан',
    'api.refresh.report.strategy': 'Стратегия формирования выбрана',
    'api.refresh.report.polled': 'Статус отчёта получен',
    'api.refresh.report.downloaded': 'Файл отчёта скачан',
    'api.refresh.report.parsed': 'CSV распарсен',
    'api.refresh.report.partial': 'Отчёт собран частично',
    'api.refresh.report.persisted': 'Строки отчёта сохранены',
    'api.refresh.report.snapshot.persisted': 'Snapshot отчёта обновлён в текущей сессии',
    'api.refresh.report.empty': 'Отчёт не вернул даты доставки',
    'api.refresh.report.error': 'Ошибка формирования отчёта',
    'api.refresh.snapshot.persisted': 'Онлайн-данные обновлены',
    'sales.snapshot.saved': 'Snapshot продаж сохранён',
    'sales.read.snapshot': 'Продажи прочитаны из точного snapshot',
    'sales.read.default_snapshot': 'Продажи прочитаны из rolling snapshot',
    'api.refresh.rows.built': 'Дата доставки применена к строкам продаж',
    'api.refresh.origin.rows.built': 'Склад / кластер отгрузки применён к строкам продаж',
    'api.refresh.status.rows.built': 'Статус применён к строкам продаж',
    'api.refresh.paid_by_customer.trace': 'Проверка поля «Оплачено покупателем» выполнена',
    'api.refresh.error': 'Ошибка обновления',
    'raw-cache.rebuild.begin': 'Пересборка из raw-cache запущена',
    'raw-cache.rebuild.snapshot.persisted': 'Онлайн-данные обновлены из текущего кэша',
    'raw-cache.rebuild.rows.built': 'Дата доставки пересобрана из raw-cache',
    'raw-cache.rebuild.origin.rows.built': 'Склад / кластер отгрузки пересобран из raw-cache',
    'raw-cache.rebuild.status.rows.built': 'Статус пересобран из raw-cache',
    'raw-cache.rebuild.paid_by_customer.trace': 'Проверка поля «Оплачено покупателем» выполнена из raw-cache',
    'raw-cache.rebuild.error': 'Ошибка пересборки из raw-cache',
    'push.ingest.received': 'Push получен',
    'push.ingest.persisted': 'Push учтён в текущей сессии',
    'push.ingest.error': 'Ошибка обработки push',
    'webhook.server.status': 'Webhook-контур активен',
    'webhook.probe.received': 'Проверочный ping webhook получен',
  }
  return map[stage] ?? (normalizeText(meta?.stageRu) || 'Шаг синхронизации')
}

function buildTraceStepSummary(meta: TraceMeta): string {
  const stage = normalizeText(meta?.stage)
  if (stage === 'api.refresh.begin') {
    return createSentence([
      normalizeText(meta?.requestedPeriod?.from || meta?.period?.from) || normalizeText(meta?.requestedPeriod?.to || meta?.period?.to)
        ? `Период: ${normalizeText(meta?.requestedPeriod?.from || meta?.period?.from)}..${normalizeText(meta?.requestedPeriod?.to || meta?.period?.to)}.`
        : '',
    ])
  }
  if (stage === 'api.refresh.list.loaded') {
    return createSentence([
      typeof meta?.fboPostingCount === 'number' ? `Отправлений FBO: ${meta.fboPostingCount}.` : '',
      normalizeText(meta?.fboAcceptedAtSpan) ? `Период FBO по «Принят в обработку»: ${meta.fboAcceptedAtSpan}.` : '',
      Array.isArray(meta?.samplePostingNumbers) && meta.samplePostingNumbers.length ? `Пример: ${meta.samplePostingNumbers.slice(0, 3).join(', ')}.` : '',
    ])
  }
  if (stage === 'api.refresh.details.loaded') {
    return createSentence([
      typeof meta?.mergedFboDetailCount === 'number' ? `Деталей FBO: ${meta.mergedFboDetailCount}.` : '',
    ])
  }
  if (stage === 'api.refresh.compat.loaded') {
    return createSentence([
      typeof meta?.compatLoadedCount === 'number' ? `Дополнительных compat-деталей: ${meta.compatLoadedCount}.` : '',
    ])
  }
  if (stage === 'api.refresh.report.created') {
    return createSentence([
      normalizeText(meta?.reportCode) ? `Код отчёта: ${meta.reportCode}.` : '',
      formatStrategyLabel(meta?.reportStrategy) ? `Режим: ${formatStrategyLabel(meta?.reportStrategy)}.` : '',
    ])
  }
  if (stage === 'api.refresh.report.parsed' || stage === 'api.refresh.report.persisted') {
    return createSentence([
      typeof meta?.reportRowsCount === 'number' ? `Строк отчёта: ${meta.reportRowsCount}.` : '',
      typeof meta?.reportRowsWithDeliveryDate === 'number' ? `Дат доставки найдено: ${meta.reportRowsWithDeliveryDate}.` : '',
      typeof meta?.reportRowsWithStatus === 'number' ? `Статусов найдено: ${meta.reportRowsWithStatus}.` : '',
      normalizeText(meta?.reportAcceptedAtSpan) ? `Период отчёта по «Принят в обработку»: ${meta.reportAcceptedAtSpan}.` : '',
    ])
  }
  if (stage === 'api.refresh.report.snapshot.persisted') {
    return createSentence([
      typeof meta?.reportSnapshotRowsCount === 'number' ? `Строк в snapshot: ${meta.reportSnapshotRowsCount}.` : '',
      typeof meta?.reportSnapshotRowsWithDeliveryDate === 'number' ? `Дат доставки в snapshot: ${meta.reportSnapshotRowsWithDeliveryDate}.` : '',
      typeof meta?.reportSnapshotRowsWithShipmentOrigin === 'number' ? `Источник отгрузки в snapshot: ${meta.reportSnapshotRowsWithShipmentOrigin}.` : '',
    ])
  }
  if (stage === 'sales.snapshot.saved') {
    return createSentence([
      typeof meta?.snapshotRowsRequested === 'number' ? `Строк до сохранения: ${meta.snapshotRowsRequested}.` : '',
      typeof meta?.snapshotRowsStored === 'number' ? `Строк сохранено: ${meta.snapshotRowsStored}.` : '',
      typeof meta?.snapshotRowsDroppedByLimit === 'number' ? `Обрезано лимитом: ${meta.snapshotRowsDroppedByLimit}.` : '',
      normalizeText(meta?.snapshotRowsSpan) ? `Диапазон строк: ${meta.snapshotRowsSpan}.` : '',
    ])
  }
  if (stage === 'sales.read.snapshot' || stage === 'sales.read.default_snapshot') {
    return createSentence([
      typeof meta?.readRowsCount === 'number' ? `Строк отдано в таблицу: ${meta.readRowsCount}.` : '',
      normalizeText(meta?.readRowsSpan) ? `Диапазон строк: ${meta.readRowsSpan}.` : '',
      normalizeText(meta?.snapshotScopeKey) ? `Scope: ${meta.snapshotScopeKey}.` : '',
    ])
  }
  if (stage === 'api.refresh.rows.built' || stage === 'raw-cache.rebuild.rows.built') {
    return createSentence([
      typeof meta?.deliveryDateResolvedRows === 'number' ? `Дат доставки применено: ${meta.deliveryDateResolvedRows}.` : '',
      typeof meta?.salesRowsWithoutDeliveryDate === 'number' ? `Строк без даты доставки: ${meta.salesRowsWithoutDeliveryDate}.` : '',
      typeof meta?.salesRowsBeforeStrictFilter === 'number' && typeof meta?.salesRowsAfterStrictFilter === 'number'
        ? `До фильтрации: ${meta.salesRowsBeforeStrictFilter}, после: ${meta.salesRowsAfterStrictFilter}.`
        : '',
    ])
  }
  if (stage === 'api.refresh.origin.rows.built' || stage === 'raw-cache.rebuild.origin.rows.built') {
    return createSentence([
      typeof meta?.shipmentOriginResolvedRows === 'number' ? `Заполнено строк: ${meta.shipmentOriginResolvedRows}.` : '',
      typeof meta?.finalRowsWithoutShipmentOrigin === 'number' ? `Строк без значения: ${meta.finalRowsWithoutShipmentOrigin}.` : '',
    ])
  }
  if (stage === 'api.refresh.status.rows.built' || stage === 'raw-cache.rebuild.status.rows.built') {
    return createSentence([
      typeof meta?.statusResolvedRows === 'number' ? `Статусов применено: ${meta.statusResolvedRows}.` : '',
      typeof meta?.finalDeliveredRows === 'number' ? `Строк со статусом «Доставлен»: ${meta.finalDeliveredRows}.` : '',
      typeof meta?.deliveredDetailsClearedRows === 'number' ? `Детали очищены у ${meta.deliveredDetailsClearedRows} строк.` : '',
    ])
  }
  if (stage === 'api.refresh.paid_by_customer.trace' || stage === 'raw-cache.rebuild.paid_by_customer.trace') {
    return createSentence([
      typeof meta?.finalRowsWithPaidByCustomer === 'number' ? `Заполнено строк: ${meta.finalRowsWithPaidByCustomer}.` : '',
      typeof meta?.finalRowsWithoutPaidByCustomer === 'number' ? `Строк без значения: ${meta.finalRowsWithoutPaidByCustomer}.` : '',
    ])
  }
  if (stage === 'api.refresh.snapshot.persisted' || stage === 'raw-cache.rebuild.snapshot.persisted') {
    return createSentence([
      typeof meta?.persisted?.shipmentDateCount === 'number' ? `Дат отгрузки в сессии: ${meta.persisted.shipmentDateCount}.` : '',
      typeof meta?.persisted?.deliveryDateCount === 'number' ? `Дат доставки в сессии: ${meta.persisted.deliveryDateCount}.` : '',
    ])
  }
  if (stage === 'webhook.server.status') {
    return createSentence([
      normalizeText(meta?.baseUrl) ? `Базовый адрес: ${meta.baseUrl}.` : '',
      normalizeText(meta?.webhookUrlLocal) ? `Webhook: ${meta.webhookUrlLocal}.` : '',
    ])
  }
  if (stage === 'push.ingest.received' || stage === 'push.ingest.persisted') {
    return createSentence([
      typeof meta?.incomingEventsCount === 'number' ? `Событий в пакете: ${meta.incomingEventsCount}.` : '',
      typeof meta?.acceptedPushEventCount === 'number' ? `Принято событий: ${meta.acceptedPushEventCount}.` : '',
    ])
  }
  if (normalizeText(meta?.errorMessage)) {
    return `Сообщение: ${normalizeText(meta.errorMessage)}.`
  }
  return ''
}

function uniqueSample(values: unknown, limit = 5): string[] {
  const arr = Array.isArray(values) ? values : []
  const out: string[] = []
  for (const value of arr) {
    const text = normalizeText(value)
    if (!text || out.includes(text)) continue
    out.push(text)
    if (out.length >= limit) break
  }
  return out
}

function pushMetric(metrics: TraceMetric[], label: string, value: unknown) {
  const text = normalizeText(value)
  if (!text) return
  metrics.push({ label, value: text })
}

function buildSectionMetrics(key: TraceCategoryKey, merged: TraceMeta): TraceMetric[] {
  const metrics: TraceMetric[] = []
  if (key === 'shipment') {
    pushMetric(metrics, 'Источник', 'API Ozon')
    if (normalizeText(merged?.requestedPeriodFrom) || normalizeText(merged?.requestedPeriodTo)) pushMetric(metrics, 'Запрошенный период', `${normalizeText(merged?.requestedPeriodFrom)}..${normalizeText(merged?.requestedPeriodTo)}`)
    if (typeof merged?.fboPostingCount === 'number') pushMetric(metrics, 'Отправлений FBO', merged.fboPostingCount)
    if (typeof merged?.mergedFboDetailCount === 'number') pushMetric(metrics, 'Деталей FBO', merged.mergedFboDetailCount)
    if (normalizeText(merged?.fboAcceptedAtSpan)) pushMetric(metrics, 'FBO: период по «Принят в обработку»', merged.fboAcceptedAtSpan)
    if (normalizeText(merged?.fbsAcceptedAtSpan)) pushMetric(metrics, 'FBS: период по «Принят в обработку»', merged.fbsAcceptedAtSpan)
    if (typeof merged?.trace?.postingsWithResolvedShipmentDate === 'number') pushMetric(metrics, 'Дат отгрузки найдено', merged.trace.postingsWithResolvedShipmentDate)
    if (typeof merged?.persisted?.shipmentDateCount === 'number') pushMetric(metrics, 'Дат отгрузки учтено в сессии', merged.persisted.shipmentDateCount)
    if (typeof merged?.persisted?.shipmentTransferEventCount === 'number') pushMetric(metrics, 'Событий отгрузки записано', merged.persisted.shipmentTransferEventCount)
    return metrics
  }
  if (key === 'delivery') {
    pushMetric(metrics, 'Источник', 'Отчёт postings CSV')
    if (normalizeText(merged?.requestedPeriodFrom) || normalizeText(merged?.requestedPeriodTo)) pushMetric(metrics, 'Запрошенный период', `${normalizeText(merged?.requestedPeriodFrom)}..${normalizeText(merged?.requestedPeriodTo)}`)
    pushMetric(metrics, 'Режим формирования', formatStrategyLabel(merged?.reportStrategy))
    if (typeof merged?.reportRowsCount === 'number') pushMetric(metrics, 'Строк в отчёте', merged.reportRowsCount)
    if (normalizeText(merged?.reportAcceptedAtSpan)) pushMetric(metrics, 'Период строк отчёта по «Принят в обработку»', merged.reportAcceptedAtSpan)
    if (normalizeText(merged?.reportDeliveryDateSpan)) pushMetric(metrics, 'Период строк отчёта по дате доставки', merged.reportDeliveryDateSpan)
    if (typeof merged?.reportRowsWithDeliveryDate === 'number') pushMetric(metrics, 'Строк с датой доставки', merged.reportRowsWithDeliveryDate)
    if (typeof merged?.deliveryDateMatchedRows === 'number') pushMetric(metrics, 'Совпало по posting_number', merged.deliveryDateMatchedRows)
    if (typeof merged?.deliveryDateResolvedRows === 'number') pushMetric(metrics, 'Дат доставки применено', merged.deliveryDateResolvedRows)
    if (typeof merged?.salesRowsWithoutDeliveryDate === 'number') pushMetric(metrics, 'Строк без даты доставки', merged.salesRowsWithoutDeliveryDate)
    if (typeof merged?.reportSnapshotRowsCount === 'number') pushMetric(metrics, 'Строк в snapshot текущей сессии', merged.reportSnapshotRowsCount)
    if (typeof merged?.salesRowsBeforeStrictFilter === 'number') pushMetric(metrics, 'Строк до строгой фильтрации', merged.salesRowsBeforeStrictFilter)
    if (normalizeText(merged?.salesRowsBeforeStrictFilterSpan)) pushMetric(metrics, 'Диапазон строк до строгой фильтрации', merged.salesRowsBeforeStrictFilterSpan)
    if (typeof merged?.salesRowsAfterStrictFilter === 'number') pushMetric(metrics, 'Строк после строгой фильтрации', merged.salesRowsAfterStrictFilter)
    if (normalizeText(merged?.salesRowsAfterStrictFilterSpan)) pushMetric(metrics, 'Диапазон строк после строгой фильтрации', merged.salesRowsAfterStrictFilterSpan)
    if (typeof merged?.reportSnapshotPersistedToApiRawCache === 'boolean') pushMetric(metrics, 'Snapshot записан в api_raw_cache', merged.reportSnapshotPersistedToApiRawCache ? 'Да' : 'Нет')
    if (typeof merged?.reportSnapshotResponseTruncated === 'number') pushMetric(metrics, 'Snapshot в api_raw_cache обрезан', Number(merged.reportSnapshotResponseTruncated) ? 'Да' : 'Нет')
    if (typeof merged?.reportSnapshotResponseBodyLen === 'number') pushMetric(metrics, 'Размер snapshot в api_raw_cache', merged.reportSnapshotResponseBodyLen)
    if (typeof merged?.reportSavedCsvCount === 'number') pushMetric(metrics, 'CSV-файлов сохранено на диск', merged.reportSavedCsvCount)
    if (typeof merged?.reportCsvHeaderCount === 'number') pushMetric(metrics, 'Колонок в CSV', merged.reportCsvHeaderCount)
    if (normalizeText(merged?.reportCode)) pushMetric(metrics, 'Код отчёта', merged.reportCode)
    if (typeof merged?.snapshotRowsRequested === 'number') pushMetric(metrics, 'Строк до сохранения snapshot продаж', merged.snapshotRowsRequested)
    if (typeof merged?.snapshotRowsStored === 'number') pushMetric(metrics, 'Строк сохранено в snapshot продаж', merged.snapshotRowsStored)
    if (typeof merged?.snapshotRowsDroppedByLimit === 'number') pushMetric(metrics, 'Строк обрезано лимитом snapshot', merged.snapshotRowsDroppedByLimit)
    if (typeof merged?.snapshotMaxRows === 'number' && merged.snapshotMaxRows > 0) pushMetric(metrics, 'Лимит строк snapshot продаж', merged.snapshotMaxRows)
    if (normalizeText(merged?.snapshotRowsSpan)) pushMetric(metrics, 'Диапазон строк в snapshot продаж', merged.snapshotRowsSpan)
    if (normalizeText(merged?.snapshotSourceKind)) pushMetric(metrics, 'Источник snapshot продаж', merged.snapshotSourceKind)
    if (typeof merged?.readRowsCount === 'number') pushMetric(metrics, 'Строк отдано в таблицу', merged.readRowsCount)
    if (normalizeText(merged?.readRowsSpan)) pushMetric(metrics, 'Диапазон строк на чтении', merged.readRowsSpan)
    return metrics
  }
  if (key === 'origin') {
    pushMetric(metrics, 'Источник', 'Отчёт postings CSV')
    if (typeof merged?.reportRowsFboWithShipmentOrigin === 'number') pushMetric(metrics, 'FBO: строк с «Кластер отгрузки» в отчёте', merged.reportRowsFboWithShipmentOrigin)
    if (typeof merged?.reportRowsFbsWithShipmentOrigin === 'number') pushMetric(metrics, 'FBS/rFBS: строк со «Склад отгрузки» в отчёте', merged.reportRowsFbsWithShipmentOrigin)
    if (typeof merged?.shipmentOriginMatchedRows === 'number') pushMetric(metrics, 'Совпало по posting_number', merged.shipmentOriginMatchedRows)
    if (typeof merged?.shipmentOriginResolvedRows === 'number') pushMetric(metrics, 'Значение применено', merged.shipmentOriginResolvedRows)
    if (typeof merged?.finalRowsWithoutShipmentOrigin === 'number') pushMetric(metrics, 'Строк без значения', merged.finalRowsWithoutShipmentOrigin)
    if (typeof merged?.fboRowsWithShipmentOrigin === 'number') pushMetric(metrics, 'FBO с заполненным значением', merged.fboRowsWithShipmentOrigin)
    if (typeof merged?.fbsRowsWithShipmentOrigin === 'number') pushMetric(metrics, 'FBS и rFBS с заполненным значением', Number(merged.fbsRowsWithShipmentOrigin ?? 0) + Number(merged.rfbsRowsWithShipmentOrigin ?? 0))
    return metrics
  }
  if (key === 'status') {
    pushMetric(metrics, 'Источник', 'Отчёт postings CSV')
    if (typeof merged?.reportRowsWithStatus === 'number') pushMetric(metrics, 'Строк со статусом в отчёте', merged.reportRowsWithStatus)
    if (typeof merged?.statusMatchedRows === 'number') pushMetric(metrics, 'Совпало по posting_number', merged.statusMatchedRows)
    if (typeof merged?.statusResolvedRows === 'number') pushMetric(metrics, 'Статусов применено', merged.statusResolvedRows)
    if (typeof merged?.salesRowsWithoutStatus === 'number') pushMetric(metrics, 'Строк без статуса', merged.salesRowsWithoutStatus)
    if (typeof merged?.finalDeliveredRows === 'number') pushMetric(metrics, 'Строк со статусом «Доставлен»', merged.finalDeliveredRows)
    if (typeof merged?.deliveredDetailsClearedRows === 'number') pushMetric(metrics, 'Строк, где очищены детали', merged.deliveredDetailsClearedRows)
    return metrics
  }
  if (key === 'paid') {
    pushMetric(metrics, 'Источник', 'API Ozon и отчёт postings')
    if (typeof merged?.finalRowsCount === 'number') pushMetric(metrics, 'Строк продаж', merged.finalRowsCount)
    if (typeof merged?.finalRowsWithPaidByCustomer === 'number') pushMetric(metrics, 'Строк с заполненной оплатой', merged.finalRowsWithPaidByCustomer)
    if (typeof merged?.finalRowsWithoutPaidByCustomer === 'number') pushMetric(metrics, 'Строк без значения', merged.finalRowsWithoutPaidByCustomer)
    if (typeof merged?.reportResolvedRowsCount === 'number') pushMetric(metrics, 'Строк подтверждено отчётом', merged.reportResolvedRowsCount)
    return metrics
  }
  return metrics
}

function buildSectionNotes(key: TraceCategoryKey, merged: TraceMeta): string[] {
  const notes: string[] = []
  if (key === 'shipment') {
    const sample = uniqueSample(merged?.missingShipmentDateSample, 4)
    if (sample.length) notes.push(`Без даты отгрузки: ${sample.join(', ')}.`)
  }
  if (key === 'delivery') {
    if (typeof merged?.reportSnapshotPeriodMatchesRequested === 'boolean') {
      notes.push(`Snapshot отчёта соответствует запрошенному периоду: ${merged.reportSnapshotPeriodMatchesRequested ? 'да' : 'нет'}.`)
    }
    const failedSegmentSample = Array.isArray(merged?.failedSegmentSample) ? merged.failedSegmentSample.slice(0, 6) : []
    if (failedSegmentSample.length) {
      notes.push(`Проблемные куски периода: ${failedSegmentSample.map((item: any) => `${normalizeText(item?.label)} → ${normalizeText(item?.error)}`).filter(Boolean).join(' | ')}.`)
    }
    const sample = uniqueSample(merged?.missingDeliveryDatePostingNumbers, 4)
    if (sample.length) notes.push(`Без даты доставки: ${sample.join(', ')}.`)
    const dateSample = uniqueSample(merged?.reportDeliveryDateSample, 3)
    if (dateSample.length) notes.push(`Примеры дат из отчёта: ${dateSample.join(', ')}.`)
    if (typeof merged?.reportSnapshotPersistedToApiRawCache === 'boolean') {
      notes.push(`Snapshot отчёта в api_raw_cache: ${merged.reportSnapshotPersistedToApiRawCache ? 'да' : 'нет'}.`)
    }
    if (typeof merged?.reportSnapshotResponseTruncated === 'number') {
      notes.push(`Snapshot отчёта в api_raw_cache обрезан: ${Number(merged.reportSnapshotResponseTruncated) ? 'да' : 'нет'}.`)
    }
    const headerSample = uniqueSample(merged?.reportCsvHeaderNames ?? merged?.reportSnapshotCsvHeaderNames, 12)
    if (headerSample.length) notes.push(`Заголовки CSV: ${headerSample.join(', ')}.`)
    const savedCsvPaths = uniqueSample(merged?.reportSavedCsvPaths ?? merged?.reportSnapshotSavedCsvPaths, 4)
    if (savedCsvPaths.length) notes.push(`CSV сохранён на диск: ${savedCsvPaths.join(' | ')}.`)
    if (Number(merged?.salesRowsBeforeStrictFilter) > 0 && Number(merged?.salesRowsAfterStrictFilter) === 0) {
      notes.push('После строгой фильтрации строки исчезли полностью. Сравни запрошенный период и фактический диапазон дат строк выше.')
    }
    if (Number(merged?.snapshotRowsDroppedByLimit) > 0) {
      notes.push(`Snapshot продаж был обрезан по лимиту строк: потеряно ${Number(merged.snapshotRowsDroppedByLimit)} строк.`)
    }
  }
  if (key === 'origin') {
    const sample = uniqueSample(merged?.reportShipmentOriginSample, 4)
    if (sample.length) notes.push(`Примеры значений из отчёта: ${sample.join(', ')}.`)
    const missing = uniqueSample(merged?.missingShipmentOriginPostingNumbers, 4)
    if (missing.length) notes.push(`Без значения: ${missing.join(', ')}.`)
  }
  if (key === 'status') {
    const sample = uniqueSample(merged?.reportStatusSample, 5)
    if (sample.length) notes.push(`Примеры статусов из отчёта: ${sample.join(', ')}.`)
    const missing = uniqueSample(merged?.missingStatusPostingNumbers, 4)
    if (missing.length) notes.push(`Без статуса: ${missing.join(', ')}.`)
  }
  return notes
}

function getSectionTitle(key: TraceCategoryKey): string {
  if (key === 'shipment') return 'FBO: дата отгрузки'
  if (key === 'delivery') return 'Продажи: дата доставки'
  if (key === 'origin') return 'Продажи: склад / кластер отгрузки'
  if (key === 'status') return 'Продажи: статус'
  if (key === 'paid') return 'Продажи: оплачено покупателем'
  return 'Прочая трассировка'
}

function getSectionIntro(key: TraceCategoryKey): string {
  if (key === 'shipment') return 'Показывает, как загружались отправления FBO и как дата отгрузки попала в текущую сессию приложения.'
  if (key === 'delivery') return 'Показывает путь даты доставки: от формирования postings CSV до применения в строки продаж.'
  if (key === 'origin') return 'Показывает заполнение столбца «Склад / кластер отгрузки»: итоговое значение берётся из API Ozon, а postings CSV используется как диагностический источник.'
  if (key === 'status') return 'Показывает путь статуса: от postings CSV до применения в строки продаж и очистки деталей для «Доставлен».'
  if (key === 'paid') return 'Показывает, как собиралось поле «Оплачено покупателем» и сколько строк было заполнено.'
  return 'Служебная трассировка.'
}

function buildTraceSections(children: LogRow[]): TraceSection[] {
  const events: TraceEvent[] = children.map((child) => {
    const meta = parseMeta(child.meta)
    return {
      row: child,
      meta,
      category: classifyTraceCategory(meta),
      title: buildTraceStepTitle(meta),
      summary: buildTraceStepSummary(meta),
    }
  })

  const byKey = new Map<TraceCategoryKey, TraceEvent[]>()
  for (const event of events) {
    const bucket = byKey.get(event.category) ?? []
    bucket.push(event)
    byKey.set(event.category, bucket)
  }

  return Array.from(byKey.entries())
    .sort((a, b) => TRACE_SECTION_ORDER[a[0]] - TRACE_SECTION_ORDER[b[0]])
    .map(([key, sectionEvents]) => {
      const merged = sectionEvents.reduce<TraceMeta>((acc, event) => ({ ...acc, ...event.meta }), {})
      const errors = sectionEvents
        .map((event) => normalizeText(event.row.error_message))
        .filter(Boolean)
      const explicitTraceError = normalizeText(merged?.reportBuildError)
      if (explicitTraceError) errors.push(explicitTraceError)
      return {
        key,
        title: getSectionTitle(key),
        intro: getSectionIntro(key),
        events: sectionEvents,
        metrics: buildSectionMetrics(key, merged),
        notes: buildSectionNotes(key, merged),
        errors: Array.from(new Set(errors)),
      }
    })
}

function buildSyncReportText(group: LogGroup, visibleId: number): string {
  const sections = buildTraceSections(group.children)
  const childDisplayIdMap = new Map(group.children.map((child, index) => [child.id, `${visibleId}/${index + 1}`]))
  const lines: string[] = [
    'Синхронизация',
    '',
    `ID: ${visibleId}`,
    `Статус: ${statusRu(group.row.status)}`,
    `Старт: ${fmtDt('started_at', group.row.started_at)}`,
    `Финиш: ${fmtDt('finished_at', group.row.finished_at)}`,
    `Сводка: ${getMainRowDetails(group.row, group.children.length)}`,
    `Ошибка: ${group.row.error_message ?? '-'}`,
  ]

  if (group.row.error_details) {
    lines.push('')
    lines.push('Технические детали ошибки:')
    lines.push(escapeTxtLine(group.row.error_details) || '-')
  }

  lines.push('')
  lines.push('Блоки трассировки:')

  if (!sections.length) {
    lines.push('— Дополнительных блоков трассировки нет.')
  } else {
    for (const section of sections) {
      lines.push('')
      lines.push(section.title)
      lines.push(section.intro)
      if (section.metrics.length) {
        lines.push('Ключевые показатели:')
        for (const metric of section.metrics) {
          lines.push(`- ${metric.label}: ${metric.value}`)
        }
      }
      if (section.notes.length) {
        lines.push('Комментарий:')
        for (const note of section.notes) lines.push(`- ${note}`)
      }
      if (section.errors.length) {
        lines.push('Ошибки:')
        for (const error of section.errors) lines.push(`- ${error}`)
      }
      if (section.events.length) {
        lines.push('Ход обработки:')
        for (const event of section.events) {
          const childId = childDisplayIdMap.get(event.row.id) ?? `${visibleId}/?`
          const eventLine = [`[${childId}]`, `[${fmtDt('started_at', event.row.started_at)}]`, `[${statusRu(event.row.status)}]`, event.title].join(' ')
          lines.push(`- ${eventLine}`)
          if (event.summary) lines.push(`  ${event.summary}`)
        }
      }
    }
  }

  return `${lines.join('\n').trim()}\n`
}

async function downloadSyncReport(group: LogGroup, visibleId: number): Promise<string> {
  const fileName = `Синхронизация ${formatFileStamp(group.row.started_at)}.txt`
  const content = buildSyncReportText(group, visibleId)
  const resp = await window.api.saveLogReportToDesktop(fileName, content)
  if (!resp?.ok) {
    throw new Error(resp?.error || 'Не удалось сохранить отчёт на Рабочий стол')
  }
  return normalizeText(resp?.path) || fileName
}

const LOG_COLUMNS: readonly LogColDef[] = [
  { id: 'id', title: 'ID' },
  { id: 'type', title: 'Тип', getSortValue: (row) => typeRu(row.type) },
  { id: 'status', title: 'Статус', getSortValue: (row) => statusRu(row.status) },
  { id: 'started_at', title: 'Старт', getSortValue: (row) => toSortTimestamp(row.started_at) ?? '' },
  { id: 'finished_at', title: 'Финиш', getSortValue: (row) => toSortTimestamp(row.finished_at) ?? '' },
  { id: 'details', title: 'Сводка', getSortValue: (row) => detailsRu(row.type, row.meta, row.items_count) },
  { id: 'error_message', title: 'Ошибка' },
]

export default function LogsPage() {
  const [logs, setLogs] = useState<LogRow[]>([])
  const [sortState, setSortState] = useState<LogSortState>(null)
  const [expandedIds, setExpandedIds] = useState<number[]>([])
  const [downloadStateByRowId, setDownloadStateByRowId] = useState<Record<number, DownloadState>>({})

  async function load() {
    const resp = await window.api.getSyncLog()
    setLogs(resp.logs as any)
  }

  useEffect(() => { void load() }, [])
  useEffect(() => {
    const onUpdated = () => { void load() }
    window.addEventListener('ozon:logs-updated', onUpdated)
    return () => window.removeEventListener('ozon:logs-updated', onUpdated)
  }, [])

  const groups = useMemo(() => buildLogGroups(logs), [logs])
  const visibleIdByRowId = useMemo(() => {
    const ordered = groups
      .slice()
      .sort((a, b) => {
        const aTime = toSortTimestamp(a.row.started_at) ?? toSortTimestamp(a.row.finished_at) ?? 0
        const bTime = toSortTimestamp(b.row.started_at) ?? toSortTimestamp(b.row.finished_at) ?? 0
        if (aTime !== bTime) return aTime - bTime
        return a.row.id - b.row.id
      })
    return new Map(ordered.map((group, index) => [group.row.id, index + 1]))
  }, [groups])
  const sortedTopLevelRows = useMemo(
    () => sortTableRows(groups.map((group) => group.row), LOG_COLUMNS, sortState),
    [groups, sortState],
  )

  const sortedGroups = useMemo(() => {
    const byId = new Map(groups.map((group) => [group.row.id, group]))
    return sortedTopLevelRows
      .map((row) => byId.get(row.id))
      .filter((group): group is LogGroup => Boolean(group))
  }, [groups, sortedTopLevelRows])

  useEffect(() => {
    setExpandedIds((prev) => prev.filter((id) => sortedGroups.some((group) => group.row.id === id && hasExpandableDetails(group))))
  }, [sortedGroups])

  function toggleSort(colId: LogSortCol) {
    const column = LOG_COLUMNS.find((item) => item.id === colId)
    if (!column || column.sortable === false) return
    setSortState((prev) => toggleTableSort(prev, colId, column.sortable !== false))
  }

  function toggleExpanded(id: number) {
    setExpandedIds((prev) => (prev.includes(id) ? prev.filter((value) => value !== id) : [...prev, id]))
  }

  function handleTextAction(event: React.KeyboardEvent<HTMLElement>, action: () => void) {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      action()
    }
  }

  function renderSortHeader(label: string, colId: LogSortCol) {
    const isSorted = sortState?.colId === colId
    return (
      <button
        type="button"
        onClick={() => toggleSort(colId)}
        title={getSortButtonTitle(isSorted, sortState?.dir)}
        style={{ display: 'inline-flex', alignItems: 'center', gap: 6, width: '100%', padding: 0, border: 'none', background: 'transparent', color: 'inherit', font: 'inherit', fontWeight: 'inherit', cursor: 'pointer', textAlign: 'left' }}
      >
        <span className="tableHeaderLabel" data-table-header-label="true" style={{ flex: '1 1 auto', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>{label}</span>
        <span aria-hidden="true" style={{ flex: '0 0 auto', opacity: isSorted ? 1 : 0.4 }}>{isSorted ? (sortState?.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
      </button>
    )
  }

  function handleDownload(group: LogGroup) {
    const visibleId = visibleIdByRowId.get(group.row.id) ?? 0
    setDownloadStateByRowId((prev) => ({ ...prev, [group.row.id]: 'saving' }))
    void downloadSyncReport(group, visibleId)
      .then(() => {
        setDownloadStateByRowId((prev) => ({ ...prev, [group.row.id]: 'done' }))
        window.setTimeout(() => {
          setDownloadStateByRowId((prev) => ({ ...prev, [group.row.id]: 'idle' }))
        }, 2200)
      })
      .catch((error: any) => {
        setDownloadStateByRowId((prev) => ({ ...prev, [group.row.id]: 'error' }))
        window.setTimeout(() => {
          setDownloadStateByRowId((prev) => ({ ...prev, [group.row.id]: 'idle' }))
        }, 2200)
        window.alert(error?.message ?? 'Не удалось сохранить отчёт на Рабочий стол')
      })
  }

  function getDownloadLabel(state: DownloadState): string {
    if (state === 'saving') return 'Скачиваем…'
    if (state === 'done') return 'Сохранено'
    if (state === 'error') return 'Ошибка'
    return 'Скачать отчёт'
  }

  return (
    <div className="card logsCard">
      <div className="tableWrap logsTableWrap">
        <div className="tableWrapX">
          <div className="tableWrapY">
            <table className="table logsTable">
              <colgroup>
                <col className="logsColId" />
                <col className="logsColType" />
                <col className="logsColStatus" />
                <col className="logsColStart" />
                <col className="logsColFinish" />
                <col className="logsColDetails" />
                <col className="logsColError" />
              </colgroup>
              <thead>
                <tr>{LOG_COLUMNS.map((column) => <th key={column.id}>{renderSortHeader(column.title, column.id)}</th>)}</tr>
              </thead>
              <tbody>
                {sortedGroups.map((group) => {
                  const { row, children } = group
                  const expandable = hasExpandableDetails(group)
                  const expanded = expandedIds.includes(row.id)
                  const mainDetails = getMainRowDetails(row, children.length)
                  const visibleId = visibleIdByRowId.get(row.id) ?? 0
                  const childDisplayIdMap = new Map(children.map((child, index) => [child.id, `${visibleId}/${index + 1}`]))
                  const traceSections = buildTraceSections(children)
                  const downloadState = downloadStateByRowId[row.id] ?? 'idle'

                  return (
                    <React.Fragment key={row.id}>
                      <tr className={expandable ? 'logMainRow logMainRowExpandable' : 'logMainRow'}>
                        <td>
                          <div className="logCellWrap" title={`Видимый ID: ${visibleId}. Системный ID: ${row.id}.`}>
                            {visibleId}
                          </div>
                        </td>
                        <td>
                          <div className="logTypeCell">
                            {expandable ? (
                              <span
                                role="button"
                                tabIndex={0}
                                className={expanded ? 'logTypeText logTypeTextExpandable expanded' : 'logTypeText logTypeTextExpandable'}
                                onClick={() => toggleExpanded(row.id)}
                                onKeyDown={(event) => handleTextAction(event, () => toggleExpanded(row.id))}
                                aria-expanded={expanded}
                                aria-label={expanded ? 'Свернуть детали синхронизации' : 'Показать детали синхронизации'}
                                title={expanded ? 'Свернуть детали синхронизации' : 'Показать детали синхронизации'}
                              >
                                {typeRu(row.type)}
                              </span>
                            ) : (
                              <span className="logTypeText" title={typeRu(row.type)}>{typeRu(row.type)}</span>
                            )}
                          </div>
                        </td>
                        <td><div className="logCellWrap"><span className={`statusText ${row.status ?? ''}`.trim()}>{statusRu(row.status)}</span></div></td>
                        <td><div className="logCellWrap" title={fmtDt('started_at', row.started_at)}>{fmtDt('started_at', row.started_at)}</div></td>
                        <td><div className="logCellWrap" title={fmtDt('finished_at', row.finished_at)}>{fmtDt('finished_at', row.finished_at)}</div></td>
                        <td><div className="logCellWrap" title={mainDetails}>{mainDetails}</div></td>
                        <td><div className="logCellWrap" title={row.error_message ?? '-'}>{row.error_message ?? '-'}</div></td>
                      </tr>

                      {expandable && expanded && (
                        <tr className="logDetailsRow">
                          <td colSpan={7}>
                            <div className="logDetailsPanel">
                              <div className="logDetailsToolbar">
                                <div>
                                  <div className="logDetailsTitle">Детали синхронизации</div>
                                  <div className="logDetailsSubtitle">Номер синхронизации: {visibleId}.</div>
                                </div>
                                <button
                                  type="button"
                                  className={`logActionButton is-${downloadState}`}
                                  onClick={() => handleDownload(group)}
                                  aria-label="Скачать отчёт по синхронизации"
                                  title="Скачать отчёт по синхронизации"
                                >
                                  <span className="logActionButtonIcon" aria-hidden="true">{downloadState === 'done' ? '✓' : '↓'}</span>
                                  <span>{getDownloadLabel(downloadState)}</span>
                                </button>
                              </div>

                              <div className="logDetailsSummary">
                                <div><span>Номер:</span> {visibleId}</div>
                                <div><span>Статус:</span> {statusRu(row.status)}</div>
                                <div><span>Старт:</span> {fmtDt('started_at', row.started_at)}</div>
                                <div><span>Финиш:</span> {fmtDt('finished_at', row.finished_at)}</div>
                                <div><span>Шагов:</span> {children.length}</div>
                              </div>

                              {row.error_details && (
                                <div className="logSection logSectionError">
                                  <div className="logSectionHeader">
                                    <div>
                                      <div className="logSectionTitle">Технические детали ошибки</div>
                                      <div className="logSectionIntro">Этот блок показывается только если у самой синхронизации сохранены технические детали ошибки.</div>
                                    </div>
                                  </div>
                                  <div className="logSectionErrorText">{escapeTxtLine(row.error_details) || '-'}</div>
                                </div>
                              )}

                              {traceSections.length > 0 ? (
                                <div className="logSectionList">
                                  {traceSections.map((section) => (
                                    <div key={section.key} className="logSection">
                                      <div className="logSectionHeader">
                                        <div>
                                          <div className="logSectionTitle">{section.title}</div>
                                          <div className="logSectionIntro">{section.intro}</div>
                                        </div>
                                      </div>

                                      {section.metrics.length > 0 && (
                                        <div className="logSectionMetrics">
                                          {section.metrics.map((metric) => (
                                            <div key={`${section.key}-${metric.label}`} className="logMetricItem">
                                              <span className="logMetricLabel">{metric.label}</span>
                                              <span className="logMetricValue">{metric.value}</span>
                                            </div>
                                          ))}
                                        </div>
                                      )}

                                      {section.notes.length > 0 && (
                                        <div className="logSectionNotes">
                                          {section.notes.map((note, index) => (
                                            <div key={`${section.key}-note-${index}`} className="logSectionNote">{note}</div>
                                          ))}
                                        </div>
                                      )}

                                      {section.errors.length > 0 && (
                                        <div className="logSectionErrors">
                                          {section.errors.map((error, index) => (
                                            <div key={`${section.key}-error-${index}`} className="logSectionErrorText">Ошибка: {error}</div>
                                          ))}
                                        </div>
                                      )}

                                      <div className="logSectionSteps">
                                        {section.events.map((event) => {
                                          const eventId = childDisplayIdMap.get(event.row.id) ?? `${visibleId}/?`
                                          return (
                                            <div key={event.row.id} className="logStepItem">
                                              <div className="logStepMeta">
                                                <span className="logStepId">{eventId}</span>
                                                <span className="logStepTime">{fmtDt('started_at', event.row.started_at)}</span>
                                                <span className={`statusText ${event.row.status ?? ''}`.trim()}>{statusRu(event.row.status)}</span>
                                              </div>
                                              <div className="logStepTitle">{event.title}</div>
                                              {event.summary && <div className="logStepSummary">{event.summary}</div>}
                                            </div>
                                          )
                                        })}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <div className="logSection">
                                  <div className="logSectionIntro">Для этой синхронизации дополнительные этапы трассировки не зарегистрированы.</div>
                                </div>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  )
                })}
                {sortedGroups.length === 0 && <tr><td colSpan={7} className="small">Пока нет записей.</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}

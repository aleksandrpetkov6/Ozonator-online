import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { formatTemporalCellRu, isTemporalColumnId } from '../utils/dateTime'
import { type TableSortState, sortTableRows, toggleTableSort } from '../utils/tableSort'
import ProductsGridView from './products/ProductsGridView'
import {
 type ColDef,
 type DataSet,
 type GridColId,
 type GridRow,
 type HiddenBucket,
 buildDefaultCols,
 colsStorageKey,
 fetchRowsCached,
 getCachedRows,
 mergeColsWithDefaults,
 appendDiscoveredCols,
 formatMoneyValue,
 isMoneyColumnId,
 readCols,
 saveCols,
 toText,
 visibilityReasonText,
 visibilityText,
} from './products/shared'

type SortState = TableSortState<GridColId>

type Props = {
 dataset?: DataSet
 viewKey?: string
 query?: string
 period?: { from?: string; to?: string }
 onStats?: (s: { total: number; filtered: number }) => void
}

const PHOTO_PREVIEW_SIZE = 200
const PHOTO_PREVIEW_DELAY_MS = 1000
const AUTO_MIN_W = 80
const AUTO_PAD = 34
const AUTO_MAX_W: Record<string, number> = {
 offer_id: 240,
 product_id: 120,
 ozon_sku: 220,
 seller_sku: 240,
 fbo_sku: 220,
 fbs_sku: 220,
 sku: 220,
 barcode: 260,
 brand: 220,
 is_visible: 180,
 hidden_reasons: 440,
 created_at: 240,
 updated_at: 240,
 in_process_at: 240,
 customer_currency_in_item_currency: 320,
 warehouse_name: 240,
 shipment_origin: 280,
 placement_zone: 320,
 type: 380,
 name: 460,
 photo_url: 90,
}

function normDay(value: unknown): string {
 const raw = typeof value === 'string' ? value.trim() : ''
 return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : ''
}

function normalizeCurrencyCode(value: unknown): string {
 const raw = typeof value === 'string' ? value.trim().toUpperCase() : String(value ?? '').trim().toUpperCase()
 return /^[A-Z]{3}$/.test(raw) ? raw : ''
}

function formatCellNumberWithCurrency(value: unknown, currencyCode: unknown): string {
 if (value == null || value === '') return '-'
 const formatted = formatMoneyValue(value)
 const code = normalizeCurrencyCode(currencyCode)
 return code ? `${formatted} ${code}` : formatted
}

function parseCellNumber(value: unknown): number | null {
 if (typeof value === 'number') return Number.isFinite(value) ? value : null
 if (typeof value !== 'string') return null
 const trimmed = value.trim()
 if (!trimmed) return null
 const normalized = trimmed.replace(/\s+/g, '').replace(',', '.')
 if (!/^-?\d+(?:\.\d+)?$/.test(normalized)) return null
 const num = Number(normalized)
 return Number.isFinite(num) ? num : null
}

function getSalesLineTotalValue(row: GridRow, field: 'price' | 'paid_by_customer' | 'customer_currency_in_item_currency'): number | '' {
 const base = parseCellNumber((row as any)?.[field])
 if (base == null) return ''
 const quantityRaw = parseCellNumber((row as any)?.quantity)
 const quantity = quantityRaw == null || quantityRaw <= 0 ? 1 : quantityRaw
 const total = base * quantity
 return Number.isFinite(total) ? Number(total.toFixed(2)) : base
}
function rowDay(value: unknown): string {
 const raw = typeof value === 'string' ? value.trim() : ''
 if (!raw) return ''
 const head = raw.slice(0, 10)
 if (/^\d{4}-\d{2}-\d{2}$/.test(head)) return head
 const parsed = new Date(raw)
 return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString().slice(0, 10)
}
function getSalesRowScopeDay(row: GridRow): string {
 const salesRow = row as any
 return rowDay(salesRow?.in_process_at)
   || rowDay(salesRow?.accepted_at)
   || rowDay(salesRow?.delivery_date)
   || rowDay(salesRow?.shipment_date)
}
function scopeSalesRows(rows: GridRow[], period?: { from?: string; to?: string }): GridRow[] {
 let from = normDay(period?.from)
 let to = normDay(period?.to)
 if (!from && !to) return rows
 if (!from) from = to
 if (!to) to = from
 if (!from || !to) return rows
 if (from > to) [from, to] = [to, from]
 return rows.filter((row) => {
   const day = getSalesRowScopeDay(row)
   return !!day && day >= from && day <= to
 })
}

const COLUMN_FILTER_STORAGE_KEY_PREFIX = 'ozonator_column_filters_'
const EMPTY_FILTER_KEY = '__EMPTY__'

type ColumnFilterMode = 'all' | 'empty' | 'nonempty'
type ColumnFilter = {
 needle?: string
 selectedKeys?: string[]
 mode?: ColumnFilterMode
}
type ColumnFiltersState = Record<string, ColumnFilter>
type FilterOption = {
 key: string
 label: string
 count: number
}

function columnFiltersStorageKey(viewKey: string): string {
 return `${COLUMN_FILTER_STORAGE_KEY_PREFIX}${String(viewKey || 'products').trim() || 'products'}`
}

function normalizeColumnFilter(value: unknown): ColumnFilter | null {
 if (!value || typeof value !== 'object') return null
 const raw = value as ColumnFilter
 const needle = typeof raw.needle === 'string' ? raw.needle.trim() : ''
 const mode: ColumnFilterMode = raw.mode === 'empty' || raw.mode === 'nonempty' ? raw.mode : 'all'
 const selectedKeys = Array.isArray(raw.selectedKeys)
   ? Array.from(new Set(raw.selectedKeys.map((item) => String(item ?? '').trim()).filter(Boolean))).slice(0, 200)
   : []
 if (!needle && selectedKeys.length === 0 && mode === 'all') return null
 return { needle, selectedKeys, mode }
}

function isColumnFilterActive(filter: ColumnFilter | null | undefined): boolean {
 return !!normalizeColumnFilter(filter)
}

function readColumnFilters(viewKey: string): ColumnFiltersState {
 try {
   const raw = localStorage.getItem(columnFiltersStorageKey(viewKey))
   if (!raw) return {}
   const parsed = JSON.parse(raw)
   if (!parsed || typeof parsed !== 'object') return {}
   const out: ColumnFiltersState = {}
   for (const [colId, filter] of Object.entries(parsed as Record<string, unknown>)) {
     const key = String(colId ?? '').trim()
     if (!key) continue
     const normalized = normalizeColumnFilter(filter)
     if (!normalized) continue
     out[key] = normalized
   }
   return out
 } catch {
   return {}
 }
}

function writeColumnFilters(viewKey: string, filters: ColumnFiltersState) {
 try {
   const payload: ColumnFiltersState = {}
   for (const [colId, filter] of Object.entries(filters)) {
     const key = String(colId ?? '').trim()
     const normalized = normalizeColumnFilter(filter)
     if (!key || !normalized) continue
     payload[key] = normalized
   }
   if (Object.keys(payload).length === 0) {
     localStorage.removeItem(columnFiltersStorageKey(viewKey))
     return
   }
   localStorage.setItem(columnFiltersStorageKey(viewKey), JSON.stringify(payload))
 } catch {
   // ignore
 }
}

export default function ProductsPage({ dataset = 'products', viewKey = dataset, query = '', period, onStats }: Props) {
 const [products, setProducts] = useState<GridRow[]>(() => (dataset === 'sales' ? [] : getCachedRows(dataset)))
 const [cols, setCols] = useState<ColDef[]>(() => readCols(dataset))
 const [sortState, setSortState] = useState<SortState>(null)
 const [draggingId, setDraggingId] = useState<string | null>(null)
 const [dropHint, setDropHint] = useState<{ id: string; side: 'left' | 'right'; x: number } | null>(null)
 const [collapsedOpen, setCollapsedOpen] = useState(false)
 const [addColumnMenuOpen, setAddColumnMenuOpen] = useState(false)
 const [bodyWindowAnchorRow, setBodyWindowAnchorRow] = useState(0)
 const [bodyViewportH, setBodyViewportH] = useState(600)
 const [photoPreview, setPhotoPreview] = useState<{ url: string; alt: string; x: number; y: number } | null>(null)
 const [colsSyncReady, setColsSyncReady] = useState(false)
 const [hasStoredCols, setHasStoredCols] = useState<boolean>(() => {
   try { return !!localStorage.getItem(colsStorageKey(dataset)) } catch { return true }
 })
 const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>(() => readColumnFilters(viewKey))
 const [openFilterColId, setOpenFilterColId] = useState<string | null>(null)
 const [filterOptionQuery, setFilterOptionQuery] = useState('')

 const collapsedBtnRef = useRef<HTMLButtonElement | null>(null)
 const collapsedMenuRef = useRef<HTMLDivElement | null>(null)
 const resizingRef = useRef<{
   id: string
   startX: number
   startW: number
   startRight: number
   startTableW: number
   colIdx: number
   headCol?: HTMLTableColElement | null
   bodyCol?: HTMLTableColElement | null
 } | null>(null)
 const headInnerRef = useRef<HTMLDivElement | null>(null)
 const bodyInnerRef = useRef<HTMLDivElement | null>(null)
 const headTableRef = useRef<HTMLTableElement | null>(null)
 const bodyTableRef = useRef<HTMLTableElement | null>(null)
 const resizeIndicatorRef = useRef<HTMLDivElement | null>(null)
 const measureCanvasRef = useRef<HTMLCanvasElement | null>(null)
 const measureFontCacheRef = useRef<{ headerFont: string | null; cellFont: string | null; headerAt: number; cellAt: number }>({ headerFont: null, cellFont: null, headerAt: 0, cellAt: 0 })
 const didAutoInitRef = useRef(false)
 const photoHoverTimerRef = useRef<number | null>(null)
 const photoHoverPendingRef = useRef<{ url: string; alt: string; clientX: number; clientY: number } | null>(null)
 const headScrollRef = useRef<HTMLDivElement | null>(null)
 const bodyScrollRef = useRef<HTMLDivElement | null>(null)
 const scrollSyncLockRef = useRef(false)
 const headerRowRef = useRef<HTMLTableRowElement | null>(null)
 const filterPopoverRef = useRef<HTMLDivElement>(null)

 const clearPhotoHoverTimer = useCallback(() => {
   if (photoHoverTimerRef.current != null) {
     window.clearTimeout(photoHoverTimerRef.current)
     photoHoverTimerRef.current = null
   }
 }, [])

 function getPhotoPreviewPos(clientX: number, clientY: number) {
   const offsetX = 18
   const offsetY = 14
   const margin = 10
   const box = PHOTO_PREVIEW_SIZE + 16
   let x = clientX + offsetX
   let y = clientY - box - offsetY
   const maxX = Math.max(margin, window.innerWidth - box - margin)
   const maxY = Math.max(margin, window.innerHeight - box - margin)
   if (y < margin) y = clientY + offsetY
   if (x > maxX) x = maxX
   if (y > maxY) y = maxY
   if (x < margin) x = margin
   if (y < margin) y = margin
   return { x, y }
 }

 function queuePhotoPreview(url: string, alt: string, clientX: number, clientY: number) {
   photoHoverPendingRef.current = { url, alt, clientX, clientY }
   clearPhotoHoverTimer()
   photoHoverTimerRef.current = window.setTimeout(() => {
     const pending = photoHoverPendingRef.current
     if (!pending) return
     const pos = getPhotoPreviewPos(pending.clientX, pending.clientY)
     setPhotoPreview({ url: pending.url, alt: pending.alt, x: pos.x, y: pos.y })
     photoHoverTimerRef.current = null
   }, PHOTO_PREVIEW_DELAY_MS)
 }

 function movePhotoPreview(clientX: number, clientY: number) {
   if (photoHoverPendingRef.current) photoHoverPendingRef.current = { ...photoHoverPendingRef.current, clientX, clientY }
   setPhotoPreview((prev) => {
     if (!prev) return prev
     const pos = getPhotoPreviewPos(clientX, clientY)
     return { ...prev, x: pos.x, y: pos.y }
   })
 }

 const hidePhotoPreview = useCallback(() => {
   clearPhotoHoverTimer()
   photoHoverPendingRef.current = null
   setPhotoPreview(null)
 }, [clearPhotoHoverTimer])

 useEffect(() => {
   let active = true
   setColsSyncReady(false)
   ;(async () => {
     const localCols = (() => {
       try {
         const raw = localStorage.getItem(colsStorageKey(dataset))
         if (!raw) return null
         return JSON.parse(raw)
       } catch {
         return null
       }
     })()

     try {
       const dbResp = await window.api.getGridColumns(dataset)
       if (!active) return
       if (Array.isArray(dbResp?.cols) && dbResp.cols.length > 0) {
         const merged = mergeColsWithDefaults(dataset, dbResp.cols)
         setCols(merged)
         setHasStoredCols(true)
         try { saveCols(dataset, merged) } catch {}
         setColsSyncReady(true)
         return
       }
       if (localCols) {
         const merged = mergeColsWithDefaults(dataset, localCols)
         setCols(merged)
         setHasStoredCols(true)
         try { await window.api.saveGridColumns(dataset, merged.map((c) => ({ id: String(c.id), w: c.w, visible: c.visible, hiddenBucket: c.hiddenBucket }))) } catch {}
         setColsSyncReady(true)
         return
       }
       setCols(buildDefaultCols(dataset))
       setHasStoredCols(false)
       setColsSyncReady(true)
     } catch {
       if (!active) return
       if (localCols) {
         const merged = mergeColsWithDefaults(dataset, localCols)
         setCols(merged)
         setHasStoredCols(true)
       } else {
         setCols(buildDefaultCols(dataset))
         setHasStoredCols(false)
       }
       setColsSyncReady(true)
     }
   })()

   return () => { active = false }
 }, [dataset])

 useEffect(() => {
   setColumnFilters(readColumnFilters(viewKey))
   setOpenFilterColId(null)
   setFilterOptionQuery('')
 }, [viewKey])

 const load = useCallback(async (force = false) => {
   const list = await fetchRowsCached(dataset, force, period)
   if (!Array.isArray(list)) return
   const nextRows = dataset === 'sales' ? scopeSalesRows(list, period) : list
   setProducts(nextRows)
   setCols((prev) => appendDiscoveredCols(prev, nextRows))
 }, [dataset, period])

 useEffect(() => { load() }, [load])

 useEffect(() => {
   const onUpdated = () => load(true)
   window.addEventListener('ozon:products-updated', onUpdated)
   return () => window.removeEventListener('ozon:products-updated', onUpdated)
 }, [load])

 useEffect(() => {
   if (products.length === 0) return
   setCols((prev) => appendDiscoveredCols(prev, products))
 }, [products, colsSyncReady])

 useEffect(() => () => { clearPhotoHoverTimer() }, [clearPhotoHoverTimer])

 const autoInitCap = dataset === 'sales' ? 400 : 800
 const autoInitSample = useMemo(() => (products.length > autoInitCap ? products.slice(0, autoInitCap) : products), [products, autoInitCap])

 useEffect(() => {
   if (!colsSyncReady) return
   const id = window.setTimeout(() => {
     const payload = cols.map((c) => ({ id: String(c.id), w: c.w, visible: c.visible, hiddenBucket: c.hiddenBucket }))
     try { saveCols(dataset, cols) } catch {}
     window.api.saveGridColumns(dataset, payload).catch(() => {})
   }, 250)
   return () => window.clearTimeout(id)
 }, [dataset, cols, colsSyncReady])

 useEffect(() => {
   writeColumnFilters(viewKey, columnFilters)
 }, [viewKey, columnFilters])

 const visibleCols = useMemo(() => cols.filter((c) => c.visible), [cols])
 const rowH = useMemo(() => (visibleCols.some((c) => c.id === 'photo_url') ? 58 : 28), [visibleCols])
 const hiddenCols = useMemo(() => cols.filter((c) => !c.visible), [cols])
 const primaryHiddenCols = useMemo(() => hiddenCols.filter((c) => c.hiddenBucket !== 'add'), [hiddenCols])
 const addMenuHiddenCols = useMemo(() => hiddenCols.filter((c) => c.hiddenBucket === 'add'), [hiddenCols])

 useEffect(() => {
   if (!collapsedOpen) return
   const onDown = (ev: MouseEvent) => {
     const t = ev.target as Node | null
     if (!t) return
     if (collapsedMenuRef.current?.contains(t)) return
     if (collapsedBtnRef.current?.contains(t)) return
     setCollapsedOpen(false)
     setAddColumnMenuOpen(false)
   }
   const onKey = (ev: KeyboardEvent) => {
     if (ev.key === 'Escape') {
       setCollapsedOpen(false)
       setAddColumnMenuOpen(false)
     }
   }
   window.addEventListener('mousedown', onDown)
   window.addEventListener('keydown', onKey)
   return () => {
     window.removeEventListener('mousedown', onDown)
     window.removeEventListener('keydown', onKey)
   }
 }, [collapsedOpen])

 useEffect(() => {
   if (collapsedOpen && hiddenCols.length === 0) {
     setCollapsedOpen(false)
     setAddColumnMenuOpen(false)
   }
 }, [collapsedOpen, hiddenCols.length])

 useEffect(() => {
   if (!addColumnMenuOpen) return
   if (addMenuHiddenCols.length > 0) return
   setAddColumnMenuOpen(false)
 }, [addColumnMenuOpen, addMenuHiddenCols.length])

 useEffect(() => {
   setCollapsedOpen(false)
   setAddColumnMenuOpen(false)
 }, [dataset])

 useEffect(() => {
   if (!openFilterColId) return
   const onDown = (ev: MouseEvent) => {
     const target = ev.target as HTMLElement | null
     if (!target) return
     if (target.closest('[data-column-filter-popover="true"]')) return
     if (target.closest('[data-column-filter-trigger="true"]')) return
     setOpenFilterColId(null)
   }
   const onKey = (ev: KeyboardEvent) => {
     if (ev.key === 'Escape') setOpenFilterColId(null)
   }
   window.addEventListener('mousedown', onDown)
   window.addEventListener('keydown', onKey)
   return () => {
     window.removeEventListener('mousedown', onDown)
     window.removeEventListener('keydown', onKey)
   }
 }, [openFilterColId])

 useEffect(() => {
   if (!openFilterColId) return
   if (visibleCols.some((c) => String(c.id) === openFilterColId)) return
   setOpenFilterColId(null)
 }, [openFilterColId, visibleCols])

 const visibleSearchKey = useMemo(() => cols.map((c) => `${c.id}:${c.visible ? 1 : 0}`).join('|'), [cols])
 const visibleSearchCols = useMemo(
   () => visibleSearchKey.split('|').filter(Boolean).flatMap((entry) => {
     const splitAt = entry.lastIndexOf(':')
     if (splitAt < 0) return []
     return entry.slice(splitAt + 1) === '1' ? [entry.slice(0, splitAt)] : []
   }),
   [visibleSearchKey],
 )

 const getFilterMeta = useCallback((p: GridRow, colId: ColDef['id']): { text: string; empty: boolean; key: string } => {
   let text = ''
   let empty = false

   if (colId === 'archived') {
     empty = true
   } else if (colId === 'is_visible') {
     text = visibilityText(p)
   } else if (colId === 'hidden_reasons') {
     const reasons = visibilityReasonText((p as any)[colId])
     text = reasons === '-' ? '' : reasons
     empty = !text
   } else if (colId === 'brand') {
     const brand = (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : ''
     text = brand || 'Не указан'
     empty = !brand
   } else if (colId === 'name') {
     const name = (p.name && String(p.name).trim()) ? String(p.name).trim() : ''
     text = name || 'Без названия'
     empty = !name
   } else if (colId === 'ozon_sku') {
     const value = p.ozon_sku ?? p.sku
     const raw = (value == null || String(value).trim() === '') ? '' : String(value)
     text = raw || '-'
     empty = !raw
   } else if (colId === 'seller_sku') {
     const value = p.seller_sku ?? p.offer_id
     const raw = (value == null || String(value).trim() === '') ? '' : String(value)
     text = raw || '-'
     empty = !raw
   } else if (colId === 'fbo_sku' || colId === 'fbs_sku') {
     const value = (p as any)[colId]
     const raw = (value == null || String(value).trim() === '') ? '' : String(value)
     text = raw || '-'
     empty = !raw
   } else if (colId === 'photo_url') {
     const raw = (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : ''
     text = raw || 'Нет фото'
     empty = !raw
   } else if (colId === 'warehouse_name') {
     const rawName = (p.warehouse_name == null ? '' : String(p.warehouse_name)).trim()
     const rawId = (p.warehouse_id == null ? '' : String(p.warehouse_id)).trim()
     text = rawName || (rawId ? `Склад #${rawId}` : 'Нет данных синхронизации')
     empty = !rawName && !rawId
   } else if (colId === 'placement_zone') {
     const zone = (p.placement_zone == null ? '' : String(p.placement_zone)).trim()
     text = zone || 'Нет данных синхронизации'
     empty = !zone
   } else if (isTemporalColumnId(colId)) {
     text = formatTemporalCellRu(colId, (p as any)[colId])
     empty = !text
   } else if (dataset === 'sales' && colId === 'price') {
     const total = getSalesLineTotalValue(p, 'price')
     text = formatMoneyValue(total)
     empty = total === ''
   } else if (dataset === 'sales' && colId === 'paid_by_customer') {
     const total = getSalesLineTotalValue(p, 'paid_by_customer')
     text = formatCellNumberWithCurrency(total, (p as any).currency)
     empty = total === ''
   } else if (dataset === 'sales' && colId === 'customer_currency_in_item_currency') {
     const total = getSalesLineTotalValue(p, 'customer_currency_in_item_currency')
     text = formatCellNumberWithCurrency(total, (p as any).item_currency)
     empty = total === ''
   } else if (isMoneyColumnId(colId)) {
     const raw = (p as any)[colId]
     text = formatMoneyValue(raw)
     empty = raw == null || raw === ''
   } else {
     const raw = toText((p as any)[colId]).trim()
     text = raw || '-'
     empty = !raw
   }

   return { text, empty, key: empty ? EMPTY_FILTER_KEY : text }
 }, [dataset])

 const rowMatchesColumnFilter = useCallback((row: GridRow, colId: string, filter: ColumnFilter | null | undefined): boolean => {
   const normalized = normalizeColumnFilter(filter)
   if (!normalized) return true
   const meta = getFilterMeta(row, colId)
   if (normalized.mode === 'empty' && !meta.empty) return false
   if (normalized.mode === 'nonempty' && meta.empty) return false
   const needle = String(normalized.needle ?? '').trim().toLowerCase()
   if (needle && !meta.text.toLowerCase().includes(needle)) return false
   const selectedKeys = normalized.selectedKeys ?? []
   if (selectedKeys.length > 0 && !selectedKeys.includes(meta.key)) return false
   return true
 }, [getFilterMeta])

 const activeColumnFilterEntries = useMemo(() => Object.entries(columnFilters)
   .map(([colId, filter]) => [colId, normalizeColumnFilter(filter)] as const)
   .filter((entry): entry is readonly [string, ColumnFilter] => !!entry[0] && !!entry[1]), [columnFilters])

 const globalQueryFilteredRows = useMemo(() => {
   const q = String(query ?? '').trim().toLowerCase()
   if (!q) return products
   return products.filter((p) => {
     const hay = visibleSearchCols
       .map((colId) => getFilterMeta(p, colId).text)
       .join(' ')
       .toLowerCase()
     return hay.includes(q)
   })
 }, [products, query, visibleSearchCols, getFilterMeta])

 const filtered = useMemo(() => {
   if (activeColumnFilterEntries.length === 0) return globalQueryFilteredRows
   return globalQueryFilteredRows.filter((row) => activeColumnFilterEntries.every(([colId, filter]) => rowMatchesColumnFilter(row, colId, filter)))
 }, [globalQueryFilteredRows, activeColumnFilterEntries, rowMatchesColumnFilter])

 const filterBaseRows = useMemo(() => {
   if (!openFilterColId) return globalQueryFilteredRows
   return globalQueryFilteredRows.filter((row) => activeColumnFilterEntries.every(([colId, filter]) => colId === openFilterColId || rowMatchesColumnFilter(row, colId, filter)))
 }, [globalQueryFilteredRows, activeColumnFilterEntries, openFilterColId, rowMatchesColumnFilter])

 const openFilterOptions = useMemo<FilterOption[]>(() => {
   if (!openFilterColId) return []
   const counts = new Map<string, FilterOption>()
   for (const row of filterBaseRows) {
     const meta = getFilterMeta(row, openFilterColId)
     const key = meta.key
     const label = meta.empty ? 'Пусто' : meta.text
     const existing = counts.get(key)
     if (existing) existing.count += 1
     else counts.set(key, { key, label: label || 'Пусто', count: 1 })
   }
   const searchNeedle = filterOptionQuery.trim().toLowerCase()
   return Array.from(counts.values())
     .filter((option) => !searchNeedle || option.label.toLowerCase().includes(searchNeedle))
     .sort((left, right) => {
       if (left.key === EMPTY_FILTER_KEY && right.key !== EMPTY_FILTER_KEY) return 1
       if (right.key === EMPTY_FILTER_KEY && left.key !== EMPTY_FILTER_KEY) return -1
       const countDiff = right.count - left.count
       if (countDiff) return countDiff
       return left.label.localeCompare(right.label, 'ru')
     })
 }, [openFilterColId, filterBaseRows, getFilterMeta, filterOptionQuery])

 const getColumnFilterState = useCallback((colId: string) => {
   const filter = normalizeColumnFilter(columnFilters[colId])
   return {
     needle: filter?.needle ?? '',
     mode: filter?.mode ?? 'all',
     selectedKeys: filter?.selectedKeys ?? [],
     active: isColumnFilterActive(filter),
   }
 }, [columnFilters])

 const updateColumnFilter = useCallback((colId: string, updater: (prev: ColumnFilter) => ColumnFilter | null) => {
   setColumnFilters((prev) => {
     const prevFilter = normalizeColumnFilter(prev[colId]) ?? { needle: '', selectedKeys: [], mode: 'all' }
     const nextFilter = normalizeColumnFilter(updater(prevFilter))
     const currentNormalized = normalizeColumnFilter(prev[colId])
     if (!nextFilter) {
       if (!currentNormalized) return prev
       const nextState = { ...prev }
       delete nextState[colId]
       return nextState
     }
     if (JSON.stringify(currentNormalized) === JSON.stringify(nextFilter)) return prev
     return { ...prev, [colId]: nextFilter }
   })
 }, [])

 const sortedRows = useMemo(() => sortTableRows(filtered, cols, sortState), [filtered, cols, sortState])

 useEffect(() => {
   onStats?.({ total: products.length, filtered: filtered.length })
 }, [products.length, filtered.length, onStats])

 function toggleSort(id: string) {
   const col = cols.find((item) => String(item.id) === id)
   if (!col || col.sortable === false) return
   setSortState((prev) => toggleTableSort(prev, col.id, col.sortable !== false))
 }

 function hideCol(id: string) {
   setCols((prev) => prev.map((c) => String(c.id) === id ? { ...c, visible: false } : c))
   setSortState((prev) => (prev?.colId === id ? null : prev))
 }

 function showCol(id: string) {
   setCols((prev) => prev.map((c) => String(c.id) === id ? { ...c, visible: true } : c))
 }

 function moveHiddenColToBucket(id: string, hiddenBucket: HiddenBucket) {
   setCols((prev) => prev.map((c) => (String(c.id) === id ? { ...c, hiddenBucket } : c)))
 }

 function onDragStart(e: React.DragEvent, id: string) {
   setDraggingId(id)
   e.dataTransfer.setData('text/plain', id)
   e.dataTransfer.effectAllowed = 'move'
 }

 function onDragOverHeader(e: React.DragEvent) {
   e.preventDefault()
   e.dataTransfer.dropEffect = 'move'
   const head = headScrollRef.current
   const row = headerRowRef.current
   if (!head || !row || visibleCols.length === 0) return

   const headRect = head.getBoundingClientRect()
   const x = (e.clientX - headRect.left) + head.scrollLeft
   const cells = Array.from(row.children) as HTMLElement[]
   if (cells.length === 0) return

   let targetId = String(visibleCols[0].id)
   let side: 'left' | 'right' = 'left'
   let lineX = 0

   for (let i = 0; i < cells.length; i++) {
     const cell = cells[i]
     const left = cell.offsetLeft
     const w = cell.offsetWidth
     const mid = left + (w / 2)
     if (x < mid) {
       targetId = String(visibleCols[i].id)
       side = 'left'
       lineX = left
       break
     }
     if (i === cells.length - 1) {
       targetId = String(visibleCols[i].id)
       side = 'right'
       lineX = left + w
     }
   }

   const next = { id: targetId, side, x: Math.round(lineX) }
   setDropHint((prev) => {
     if (!prev) return next
     const stableX = (Math.abs(next.x - prev.x) <= 3) ? prev.x : next.x
     const stable = { ...next, x: stableX }
     if (prev.id === stable.id && prev.side === stable.side && prev.x === stable.x) return prev
     return stable
   })
 }

 function onDrop(e: React.DragEvent) {
   e.preventDefault()
   const draggedId = e.dataTransfer.getData('text/plain')
   if (!draggedId) return
   const hint = dropHint
   if (!hint) {
     setDraggingId(null)
     setDropHint(null)
     return
   }

   setCols((prev) => {
     const fromIdx = prev.findIndex((c) => String(c.id) === draggedId)
     const toIdxRaw = prev.findIndex((c) => String(c.id) === hint.id)
     if (fromIdx < 0 || toIdxRaw < 0 || fromIdx === toIdxRaw) return prev
     const insertBase = toIdxRaw + (hint.side === 'right' ? 1 : 0)
     const next = [...prev]
     const [moved] = next.splice(fromIdx, 1)
     let insertIdx = insertBase
     if (fromIdx < insertIdx) insertIdx -= 1
     if (insertIdx < 0) insertIdx = 0
     if (insertIdx > next.length) insertIdx = next.length
     next.splice(insertIdx, 0, moved)
     return next
   })

   setDraggingId(null)
   setDropHint(null)
 }

 function onDragEnd() {
   setDraggingId(null)
   setDropHint(null)
 }

 function startResize(e: React.MouseEvent, colId: string) {
   e.preventDefault()
   e.stopPropagation()
   const col = cols.find((c) => String(c.id) === colId)
   if (!col) return
   const head = headScrollRef.current
   const row = headerRowRef.current
   const cell = row?.querySelector<HTMLElement>(`th[data-col-id="${colId}"]`)
   if (!head || !cell) return

   const colIdx = visibleCols.findIndex((c) => String(c.id) === colId)
   if (colIdx < 0) return
   const startRight = cell.offsetLeft + cell.offsetWidth
   const headCols = headTableRef.current?.querySelectorAll('colgroup col') ?? []
   const bodyCols = bodyTableRef.current?.querySelectorAll('colgroup col') ?? []
   const headCol = (headCols[colIdx] as any) as HTMLTableColElement | null
   const bodyCol = (bodyCols[colIdx] as any) as HTMLTableColElement | null

   resizingRef.current = { id: colId, startX: e.clientX, startW: col.w, startRight, startTableW: tableWidth, colIdx, headCol, bodyCol }

   const indicator = resizeIndicatorRef.current
   if (indicator) {
     indicator.style.display = 'block'
     indicator.style.left = `${Math.round(startRight - (head.scrollLeft ?? 0))}px`
   }

   const prevCursor = document.body.style.cursor
   const prevSelect = document.body.style.userSelect
   document.body.style.cursor = 'col-resize'
   document.body.style.userSelect = 'none'

   let raf: number | null = null
   let pendingDx = 0
   let lastW = col.w

   const flush = () => {
     raf = null
     const r = resizingRef.current
     if (!r) return
     const w = Math.max(AUTO_MIN_W, Math.round(r.startW + pendingDx))
     const delta = w - r.startW
     const newTableW = Math.max(1, Math.round(r.startTableW + delta))

     if (w !== lastW) {
       lastW = w
       if (r.headCol) (r.headCol as any).style.width = `${w}px`
       if (r.bodyCol) (r.bodyCol as any).style.width = `${w}px`
       if (headInnerRef.current) headInnerRef.current.style.width = `${newTableW}px`
       if (bodyInnerRef.current) bodyInnerRef.current.style.width = `${newTableW}px`
       if (headTableRef.current) headTableRef.current.style.width = `${newTableW}px`
       if (bodyTableRef.current) bodyTableRef.current.style.width = `${newTableW}px`
     }

     const sl = headScrollRef.current?.scrollLeft ?? 0
     if (indicator) indicator.style.left = `${Math.round(r.startRight + delta - sl)}px`
   }

   const schedule = () => {
     if (raf != null) return
     raf = window.requestAnimationFrame(flush)
   }

   const onMove = (ev: MouseEvent) => {
     const r = resizingRef.current
     if (!r) return
     pendingDx = ev.clientX - r.startX
     schedule()
   }

   const onUp = () => {
     if (raf != null) {
       window.cancelAnimationFrame(raf)
       raf = null
     }
     const r = resizingRef.current
     resizingRef.current = null
     document.body.style.cursor = prevCursor
     document.body.style.userSelect = prevSelect
     if (indicator) indicator.style.display = 'none'
     window.removeEventListener('mousemove', onMove)
     window.removeEventListener('mouseup', onUp)
     if (!r) return
     const finalW = Math.max(AUTO_MIN_W, Math.round(r.startW + pendingDx))
     setCols((prev) => prev.map((c) => String(c.id) === r.id ? { ...c, w: finalW } : c))
   }

   window.addEventListener('mousemove', onMove)
   window.addEventListener('mouseup', onUp)
 }

 function cellText(p: GridRow, colId: ColDef['id']): { text: string; title?: string } {
   if (colId === 'offer_id') return { text: p.offer_id }
   if (colId === 'name') return { text: (p.name && String(p.name).trim()) ? String(p.name).trim() : 'Без названия' }
   if (colId === 'brand') return { text: (p.brand && String(p.brand).trim()) ? String(p.brand).trim() : 'Не указан' }
   if (colId === 'photo_url') return { text: '', title: (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : 'Нет фото' }
   if (colId === 'ozon_sku') {
     const v = p.ozon_sku ?? p.sku
     return { text: (v == null || String(v).trim() === '') ? '-' : String(v) }
   }
   if (colId === 'seller_sku') {
     const v = p.seller_sku ?? p.offer_id
     return { text: (v == null || String(v).trim() === '') ? '-' : String(v) }
   }
   if (colId === 'fbo_sku' || colId === 'fbs_sku') {
     const v = (p as any)[colId]
     return { text: (v == null || String(v).trim() === '') ? '-' : String(v) }
   }
   if (colId === 'is_visible') {
     const txt = visibilityText(p)
     const rs = visibilityReasonText(p.hidden_reasons)
     return { text: txt, title: rs !== '-' ? rs : undefined }
   }

   const v = (p as any)[colId]
   if (colId === 'hidden_reasons') {
     const rs = visibilityReasonText(v)
     return { text: rs, title: rs !== '-' ? rs : undefined }
   }
   if (isTemporalColumnId(colId)) {
     const f = formatTemporalCellRu(colId, v)
     return { text: f || (colId === 'delivery_date' ? '' : '-'), title: f || undefined }
   }
   if (colId === 'warehouse_name') {
     const rawName = (p.warehouse_name == null ? '' : String(p.warehouse_name)).trim()
     if (rawName) return { text: rawName }
     const rawId = (p.warehouse_id == null ? '' : String(p.warehouse_id)).trim()
     return { text: rawId ? `Склад #${rawId}` : 'Нет данных синхронизации' }
   }
   if (colId === 'placement_zone') {
     const zone = (p.placement_zone == null ? '' : String(p.placement_zone)).trim()
     return { text: zone || 'Нет данных синхронизации' }
   }
   if (dataset === 'sales' && colId === 'price') {
     return { text: formatMoneyValue(getSalesLineTotalValue(p, 'price')) }
   }
   if (dataset === 'sales' && colId === 'paid_by_customer') {
     return { text: formatCellNumberWithCurrency(getSalesLineTotalValue(p, 'paid_by_customer'), (p as any).currency) }
   }
   if (dataset === 'sales' && colId === 'customer_currency_in_item_currency') {
     return { text: formatCellNumberWithCurrency(getSalesLineTotalValue(p, 'customer_currency_in_item_currency'), (p as any).item_currency) }
   }
   if (isMoneyColumnId(colId)) {
     return { text: formatMoneyValue(v) }
   }
   return { text: (v == null || v === '') ? '-' : String(v) }
 }

 function measureTextWidth(text: string, kind: 'cell' | 'header' = 'cell'): number {
   const canvas = measureCanvasRef.current ?? (measureCanvasRef.current = document.createElement('canvas'))
   const ctx = canvas.getContext('2d')
   if (!ctx) return text.length * 7

   const cache = measureFontCacheRef.current
   const now = Date.now()
   const isHeader = kind === 'header'
   const ttlMs = 1500

   const cachedFont = isHeader ? cache.headerFont : cache.cellFont
   const cachedAt = isHeader ? cache.headerAt : cache.cellAt

   if (!cachedFont || now - cachedAt > ttlMs) {
     const probe = document.querySelector(isHeader ? '.thTitle' : '.cellText') as HTMLElement | null
     const cs = window.getComputedStyle(probe ?? document.body)
     const fontStyle = cs.fontStyle || 'normal'
     const fontVariant = cs.fontVariant || 'normal'
     const fontWeight = cs.fontWeight || '400'
     const fontSize = cs.fontSize || '13px'
     const lineHeight = cs.lineHeight && cs.lineHeight !== 'normal' ? `/${cs.lineHeight}` : ''
     const fontFamily = cs.fontFamily || 'system-ui'
     const font = `${fontStyle} ${fontVariant} ${fontWeight} ${fontSize}${lineHeight} ${fontFamily}`
     if (isHeader) {
       cache.headerFont = font
       cache.headerAt = now
     } else {
       cache.cellFont = font
       cache.cellAt = now
     }
   }

   ctx.font = (isHeader ? cache.headerFont : cache.cellFont) || ctx.font
   return ctx.measureText(text).width
 }

 const getCellString = useCallback((p: GridRow, colId: ColDef['id']): string => getFilterMeta(p, colId).text, [getFilterMeta])

 function autoSizeColumn(colId: string, rows: GridRow[], mode: 'default' | 'fit' = 'default') {
   const col = cols.find((c) => String(c.id) === colId)
   if (!col) return
   if (colId === 'photo_url') {
     setCols((prev) => prev.map((c) => String(c.id) === colId ? { ...c, w: 120 } : c))
     return
   }

   const headerExtra = 44
   const baseCap = AUTO_MAX_W[colId] ?? 320
   const cap = mode === 'fit' ? 4000 : baseCap
   let max = measureTextWidth(col.title, 'header') + headerExtra
   const sample = mode === 'fit' ? rows : (rows.length > 1600 ? rows.slice(0, 1600) : rows)
   for (const p of sample) {
     const s = getCellString(p, col.id)
     if (!s) continue
     const w = measureTextWidth(s, 'cell')
     if (w > max) max = w
   }

   const nextW = Math.max(AUTO_MIN_W, Math.min(cap, Math.round(max + AUTO_PAD)))
   setCols((prev) => prev.map((c) => String(c.id) === colId ? { ...c, w: nextW } : c))
 }

 useEffect(() => {
   if (!colsSyncReady) return
   if (didAutoInitRef.current) return
   if (hasStoredCols) {
     didAutoInitRef.current = true
     return
   }

   didAutoInitRef.current = true
   setCols((prev) => prev.map((c) => {
     const id = String(c.id)
     if (id === 'photo_url') return { ...c, w: 120 }
     const headerExtra = 44
     const baseCap = AUTO_MAX_W[id] ?? 320
     let max = measureTextWidth(c.title, 'header') + headerExtra
     for (const p of autoInitSample) {
       const s = getCellString(p, c.id)
       if (!s) continue
       const w = measureTextWidth(s)
       if (w > max) max = w
     }
     return { ...c, w: Math.max(AUTO_MIN_W, Math.min(baseCap, Math.round(max + AUTO_PAD))) }
   }))
 }, [autoInitSample, colsSyncReady, getCellString, hasStoredCols, dataset])

 const tableWidth = useMemo(() => Math.max(1, visibleCols.reduce((s, c) => s + c.w, 0)), [visibleCols])

 useEffect(() => {
   const head = headScrollRef.current
   const body = bodyScrollRef.current
   if (!head || !body) return

   const syncFromBody = () => {
     hidePhotoPreview()
     if (scrollSyncLockRef.current) return
     scrollSyncLockRef.current = true
     head.scrollLeft = body.scrollLeft
     scrollSyncLockRef.current = false
   }

   const syncFromHead = () => {
     hidePhotoPreview()
     if (scrollSyncLockRef.current) return
     scrollSyncLockRef.current = true
     body.scrollLeft = head.scrollLeft
     scrollSyncLockRef.current = false
   }

   body.addEventListener('scroll', syncFromBody, { passive: true })
   head.addEventListener('scroll', syncFromHead, { passive: true })
   head.scrollLeft = body.scrollLeft

   return () => {
     body.removeEventListener('scroll', syncFromBody)
     head.removeEventListener('scroll', syncFromHead)
   }
 }, [visibleCols.length, hidePhotoPreview])

 useEffect(() => {
   const body = bodyScrollRef.current
   if (!body) return
   const updateViewport = () => setBodyViewportH(body.clientHeight || 0)
   updateViewport()
   const ro = new ResizeObserver(() => updateViewport())
   ro.observe(body)
   return () => ro.disconnect()
 }, [])

 useEffect(() => {
   const body = bodyScrollRef.current
   if (!body) return
   const viewH = bodyViewportH || 600
   const viewportRows = Math.max(1, Math.ceil(viewH / rowH))
   const windowStepRows = Math.max(12, Math.ceil(viewportRows / 2))

   const onScroll = () => {
     const nextTop = Math.max(0, body.scrollTop || 0)
     const anchorRow = Math.floor(nextTop / rowH)
     const nextWindowAnchorRow = Math.floor(anchorRow / windowStepRows) * windowStepRows
     setBodyWindowAnchorRow((prev) => (prev === nextWindowAnchorRow ? prev : nextWindowAnchorRow))
   }

   body.addEventListener('scroll', onScroll, { passive: true })
   onScroll()
   return () => body.removeEventListener('scroll', onScroll)
 }, [bodyViewportH, rowH])

 const totalRows = sortedRows.length
 const viewH = bodyViewportH || 600
 const viewportRows = Math.max(1, Math.ceil(viewH / rowH))
 const OVERSCAN = Math.max(64, viewportRows * 3)
 const WINDOW_STEP_ROWS = Math.max(12, Math.ceil(viewportRows / 2))
 const startRow = Math.max(0, bodyWindowAnchorRow - OVERSCAN)
 const endRow = Math.min(totalRows, bodyWindowAnchorRow + viewportRows + (OVERSCAN * 2) + WINDOW_STEP_ROWS)
 const visibleRows = useMemo(() => sortedRows.slice(startRow, endRow), [sortedRows, startRow, endRow])
 const topSpace = startRow * rowH
 const bottomSpace = Math.max(0, (totalRows - endRow) * rowH)

 const salesItemCurrencyHeaderSuffix = useMemo(() => {
   if (dataset !== 'sales') return ''
   const codes = Array.from(new Set(
     sortedRows
       .map((row) => String(row.item_currency ?? '').trim().toUpperCase())
       .filter(Boolean),
   ))
   if (codes.length === 0) return ''
   if (codes.length === 1) return codes[0]
   return codes.join(' / ')
 }, [dataset, sortedRows])

 const getHeaderTitleText = useCallback((c: ColDef): string => {
   const colId = String(c.id)
   if (colId === 'offer_id' && dataset === 'products') return `${c.title} ${totalRows}`
   if (colId === 'price' && dataset === 'sales') {
     return salesItemCurrencyHeaderSuffix
       ? `Ваша цена в ${salesItemCurrencyHeaderSuffix}`
       : 'Ваша цена в коде валюты товара'
   }
   if (colId === 'customer_currency_in_item_currency' && dataset === 'sales') {
     return salesItemCurrencyHeaderSuffix
       ? `Оплачено покупателем в ${salesItemCurrencyHeaderSuffix}`
       : 'Оплачено покупателем в валюте товара'
   }
   return c.title
 }, [dataset, salesItemCurrencyHeaderSuffix, totalRows])

 const getRowKey = useCallback((p: GridRow, absoluteRowIndex: number): string => {
   if (dataset === 'stocks') return `${p.offer_id}__${p.sku ?? ''}__${p.warehouse_id ?? ''}__${(p.placement_zone ?? '').toString().trim()}`
   if (dataset === 'sales') {
     const row = p as any
     return `${row.posting_number ?? ''}__${p.offer_id}__${p.sku ?? ''}__${row.in_process_at ?? row.created_at ?? ''}__${absoluteRowIndex}`
   }
   if (dataset === 'returns') {
     const row = p as any
     return `${row.return_id ?? ''}__${p.offer_id}__${p.sku ?? ''}__${row.created_at ?? ''}__${absoluteRowIndex}`
   }
   return p.offer_id
 }, [dataset])

 const handleColumnFilterNeedleChange = useCallback((colId: string, needle: string) => {
   updateColumnFilter(colId, (prev) => ({ ...prev, needle }))
 }, [updateColumnFilter])

 const handleColumnFilterModeChange = useCallback((colId: string, mode: ColumnFilterMode) => {
   updateColumnFilter(colId, (prev) => ({ ...prev, mode }))
 }, [updateColumnFilter])

 const handleColumnFilterOptionToggle = useCallback((colId: string, optionKey: string) => {
   updateColumnFilter(colId, (prev) => {
     const nextKeys = new Set(prev.selectedKeys ?? [])
     if (nextKeys.has(optionKey)) nextKeys.delete(optionKey)
     else nextKeys.add(optionKey)
     return { ...prev, selectedKeys: Array.from(nextKeys) }
   })
 }, [updateColumnFilter])

 const handleColumnFilterClear = useCallback((colId: string) => {
   updateColumnFilter(colId, () => null)
   setFilterOptionQuery('')
 }, [updateColumnFilter])

 return (
   <ProductsGridView
     hiddenCols={hiddenCols}
     collapsedOpen={collapsedOpen}
     addColumnMenuOpen={addColumnMenuOpen}
     addMenuHiddenCols={addMenuHiddenCols}
     primaryHiddenCols={primaryHiddenCols}
     visibleCols={visibleCols}
     draggingId={draggingId}
     dropHint={dropHint}
     tableWidth={tableWidth}
     visibleRows={visibleRows}
     startRow={startRow}
     topSpace={topSpace}
     bottomSpace={bottomSpace}
     empty={sortedRows.length === 0}
     sortColId={sortState?.colId ?? null}
     sortDir={sortState?.dir}
     openFilterColId={openFilterColId}
     openFilterOptions={openFilterOptions}
     filterOptionQuery={filterOptionQuery}
     photoPreview={photoPreview}
     collapsedBtnRef={collapsedBtnRef}
     collapsedMenuRef={collapsedMenuRef}
     resizeIndicatorRef={resizeIndicatorRef}
     headScrollRef={headScrollRef}
     headInnerRef={headInnerRef}
     headTableRef={headTableRef}
     headerRowRef={headerRowRef}
     bodyScrollRef={bodyScrollRef}
     bodyInnerRef={bodyInnerRef}
     bodyTableRef={bodyTableRef}
     filterPopoverRef={filterPopoverRef}
     getHeaderTitleText={getHeaderTitleText}
     getRowKey={getRowKey}
     cellText={cellText}
     setCollapsedOpen={setCollapsedOpen}
     setAddColumnMenuOpen={setAddColumnMenuOpen}
     setOpenFilterColId={setOpenFilterColId}
     setFilterOptionQuery={setFilterOptionQuery}
     onShowCol={showCol}
     onHideCol={hideCol}
     onMoveHiddenColToBucket={moveHiddenColToBucket}
     onDragStart={onDragStart}
     onDragOverHeader={onDragOverHeader}
     onDrop={onDrop}
     onDragEnd={onDragEnd}
     getColumnFilterState={getColumnFilterState}
     onColumnFilterNeedleChange={handleColumnFilterNeedleChange}
     onColumnFilterModeChange={handleColumnFilterModeChange}
     onColumnFilterOptionToggle={handleColumnFilterOptionToggle}
     onClearColumnFilter={handleColumnFilterClear}
     onToggleSort={toggleSort}
     onStartResize={startResize}
     onAutoSize={(id) => autoSizeColumn(id, sortedRows, 'fit')}
     queuePhotoPreview={queuePhotoPreview}
     movePhotoPreview={movePhotoPreview}
     hidePhotoPreview={hidePhotoPreview}
   />
 )
}

import React from 'react'
import { getSortButtonTitle, type TableSortDir } from '../../utils/tableSort'
import { type ColDef, type GridRow, type HiddenBucket } from './shared'

const SALES_SHIPMENT_CONFIRMED_MARKERS = [
  'отгружен',
  'отправлен продавцом',
  'передан в доставку',
  'передан в службу доставки',
  'забирает курьер',
  'в пути',
  'доставляется',
  'доставлен',
  'доставлен покупателю',
  'получен покупателем',
  'возвращается',
  'возвращён',
  'возвращен',
  'возврат',
]

type ColumnFilterMode = 'all' | 'empty' | 'nonempty'
type ColumnFilterViewState = {
  needle: string
  mode: ColumnFilterMode
  selectedKeys: string[]
  active: boolean
}
type FilterOption = {
  key: string
  label: string
  count: number
}

function toRowText(value: unknown): string {
  if (value == null) return ''
  return String(value).trim()
}

function toRowTimestamp(value: unknown): number | null {
  const raw = toRowText(value)
  if (!raw) return null
  const parsed = Date.parse(raw)
  return Number.isFinite(parsed) ? parsed : null
}

function hasConfirmedShipmentDate(row: GridRow): boolean {
  const rawShipmentDate = toRowText(row.shipment_date)
  if (!rawShipmentDate) return false

  const shipmentTs = toRowTimestamp(rawShipmentDate)
  if (shipmentTs != null && shipmentTs > (Date.now() + 60_000)) return false
  if (toRowTimestamp(row.delivery_date) != null) return true

  const signalText = [row.status, row.status_details, row.carrier_status_details]
    .map((value) => toRowText(value).toLowerCase())
    .filter(Boolean)
    .join(' | ')

  if (!signalText) return false
  return SALES_SHIPMENT_CONFIRMED_MARKERS.some((marker) => signalText.includes(marker))
}

type FloatingPopoverState = {
  top: number
  left: number
  width: number
}

type Props = {
  hiddenCols: ColDef[]
  collapsedOpen: boolean
  addColumnMenuOpen: boolean
  addMenuHiddenCols: ColDef[]
  primaryHiddenCols: ColDef[]
  visibleCols: ColDef[]
  draggingId: string | null
  dropHint: { id: string; side: 'left' | 'right'; x: number } | null
  tableWidth: number
  visibleRows: GridRow[]
  startRow: number
  topSpace: number
  bottomSpace: number
  empty: boolean
  sortColId: ColDef['id'] | null
  sortDir?: TableSortDir
  openFilterColId: string | null
  openFilterOptions: FilterOption[]
  filterOptionQuery: string
  photoPreview: { url: string; alt: string; x: number; y: number } | null
  collapsedBtnRef: React.RefObject<HTMLButtonElement>
  collapsedMenuRef: React.RefObject<HTMLDivElement>
  resizeIndicatorRef: React.RefObject<HTMLDivElement>
  headScrollRef: React.RefObject<HTMLDivElement>
  headInnerRef: React.RefObject<HTMLDivElement>
  headTableRef: React.RefObject<HTMLTableElement>
  headerRowRef: React.RefObject<HTMLTableRowElement>
  bodyScrollRef: React.RefObject<HTMLDivElement>
  bodyInnerRef: React.RefObject<HTMLDivElement>
  bodyTableRef: React.RefObject<HTMLTableElement>
  filterPopoverRef: React.RefObject<HTMLDivElement>
  getHeaderTitleText: (c: ColDef) => string
  getRowKey: (p: GridRow, absoluteRowIndex: number) => string
  cellText: (p: GridRow, colId: ColDef['id']) => { text: string; title?: string }
  setCollapsedOpen: React.Dispatch<React.SetStateAction<boolean>>
  setAddColumnMenuOpen: React.Dispatch<React.SetStateAction<boolean>>
  setOpenFilterColId: React.Dispatch<React.SetStateAction<string | null>>
  setFilterOptionQuery: React.Dispatch<React.SetStateAction<string>>
  onShowCol: (id: string) => void
  onHideCol: (id: string) => void
  onMoveHiddenColToBucket: (id: string, hiddenBucket: HiddenBucket) => void
  onDragStart: (e: React.DragEvent, id: string) => void
  onDragOverHeader: (e: React.DragEvent) => void
  onDrop: (e: React.DragEvent) => void
  onDragEnd: () => void
  getColumnFilterState: (id: string) => ColumnFilterViewState
  onColumnFilterNeedleChange: (id: string, value: string) => void
  onColumnFilterModeChange: (id: string, mode: ColumnFilterMode) => void
  onColumnFilterOptionToggle: (id: string, optionKey: string) => void
  onClearColumnFilter: (id: string) => void
  onToggleSort: (id: string) => void
  onStartResize: (e: React.MouseEvent, id: string) => void
  onAutoSize: (id: string) => void
  queuePhotoPreview: (url: string, alt: string, clientX: number, clientY: number) => void
  movePhotoPreview: (clientX: number, clientY: number) => void
  hidePhotoPreview: () => void
}

export default function ProductsGridView(props: Props) {
  const {
    hiddenCols,
    collapsedOpen,
    addColumnMenuOpen,
    addMenuHiddenCols,
    primaryHiddenCols,
    visibleCols,
    draggingId,
    dropHint,
    tableWidth,
    visibleRows,
    startRow,
    topSpace,
    bottomSpace,
    empty,
    sortColId,
    sortDir,
    openFilterColId,
    openFilterOptions,
    filterOptionQuery,
    photoPreview,
    collapsedBtnRef,
    collapsedMenuRef,
    resizeIndicatorRef,
    headScrollRef,
    headInnerRef,
    headTableRef,
    headerRowRef,
    bodyScrollRef,
    bodyInnerRef,
    bodyTableRef,
    filterPopoverRef,
    getHeaderTitleText,
    getRowKey,
    cellText,
    setCollapsedOpen,
    setAddColumnMenuOpen,
    setOpenFilterColId,
    setFilterOptionQuery,
    onShowCol,
    onHideCol,
    onMoveHiddenColToBucket,
    onDragStart,
    onDragOverHeader,
    onDrop,
    onDragEnd,
    getColumnFilterState,
    onColumnFilterNeedleChange,
    onColumnFilterModeChange,
    onColumnFilterOptionToggle,
    onClearColumnFilter,
    onToggleSort,
    onStartResize,
    onAutoSize,
    queuePhotoPreview,
    movePhotoPreview,
    hidePhotoPreview,
  } = props

  const tableWrapRef = React.useRef<HTMLDivElement | null>(null)
  const filterTriggerRefs = React.useRef<Record<string, HTMLButtonElement | null>>({})
  const [floatingPopover, setFloatingPopover] = React.useState<FloatingPopoverState | null>(null)

  const updateFloatingPopover = React.useCallback(() => {
    if (!openFilterColId) {
      setFloatingPopover(null)
      return
    }
    const trigger = filterTriggerRefs.current[openFilterColId]
    const wrap = tableWrapRef.current
    if (!trigger || !wrap) {
      setFloatingPopover(null)
      return
    }

    const triggerRect = trigger.getBoundingClientRect()
    const wrapRect = wrap.getBoundingClientRect()
    const maxWidth = Math.max(240, Math.min(320, Math.floor(wrapRect.width - 16)))
    let left = Math.round(triggerRect.right - wrapRect.left - maxWidth)
    const minLeft = 8
    const maxLeft = Math.max(minLeft, Math.round(wrapRect.width - maxWidth - 8))
    if (left < minLeft) left = minLeft
    if (left > maxLeft) left = maxLeft
    const top = Math.round(triggerRect.bottom - wrapRect.top + 6)
    setFloatingPopover({ top, left, width: maxWidth })
  }, [openFilterColId])

  React.useLayoutEffect(() => {
    updateFloatingPopover()
  }, [updateFloatingPopover, tableWidth, visibleCols, openFilterOptions.length, filterOptionQuery])

  React.useEffect(() => {
    if (!openFilterColId) return
    const update = () => updateFloatingPopover()
    const headEl = headScrollRef.current
    const bodyEl = bodyScrollRef.current
    window.addEventListener('resize', update)
    headEl?.addEventListener('scroll', update, { passive: true })
    bodyEl?.addEventListener('scroll', update, { passive: true })
    return () => {
      window.removeEventListener('resize', update)
      headEl?.removeEventListener('scroll', update)
      bodyEl?.removeEventListener('scroll', update)
    }
  }, [openFilterColId, updateFloatingPopover, headScrollRef, bodyScrollRef])

  const openFilterCol = openFilterColId ? visibleCols.find((col) => String(col.id) === openFilterColId) ?? null : null
  const openColumnFilterState = openFilterColId ? getColumnFilterState(openFilterColId) : { needle: '', mode: 'all' as ColumnFilterMode, selectedKeys: [], active: false }

  return (
    <div className="productsCard">
      <div className="productsTableArea">
        <div className="tableWrap" ref={tableWrapRef} style={{ marginTop: 0, position: 'relative' }}>
          <div className="resizeIndicator" ref={resizeIndicatorRef} style={{ display: 'none' }} />
          {hiddenCols.length > 0 && (
            <div className="collapsedCorner" style={{ position: 'absolute', top: 6, right: 6, zIndex: 5 }}>
              <button
                type="button"
                className="colToggle colTogglePlus"
                ref={collapsedBtnRef}
                title="Показать скрытый столбец"
                aria-haspopup="menu"
                aria-expanded={collapsedOpen}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => {
                  if (collapsedOpen) setAddColumnMenuOpen(false)
                  setCollapsedOpen((v) => !v)
                }}
              >
                +
              </button>

              {collapsedOpen && (
                <div
                  className="collapsedMenu"
                  ref={collapsedMenuRef}
                  role="menu"
                  style={{ position: 'absolute', top: 0, right: 'calc(100% + 6px)', left: 'auto', zIndex: 6 }}
                >
                  <button
                    type="button"
                    className="collapsedMenuItem"
                    role="menuitem"
                    style={{ padding: '6px 10px', lineHeight: 1.1, display: 'flex', alignItems: 'center', gap: 8, fontWeight: 600 }}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => {
                      if (addMenuHiddenCols.length === 0) {
                        setAddColumnMenuOpen(false)
                        return
                      }
                      setAddColumnMenuOpen((v) => !v)
                    }}
                  >
                    <span style={{ flex: '1 1 auto', minWidth: 0 }}>Добавить столбец</span>
                    <span aria-hidden="true" style={{ fontSize: 11, opacity: 0.7 }}>{addColumnMenuOpen ? '▾' : '▸'}</span>
                  </button>

                  {addColumnMenuOpen && addMenuHiddenCols.length > 0 && (
                    <div style={{ display: 'grid', gap: 4, marginBottom: 6, padding: '2px 0 6px 10px' }}>
                      {addMenuHiddenCols.map((c) => {
                        const id = String(c.id)
                        return (
                          <div key={id} style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
                            <button
                              type="button"
                              className="collapsedMenuItem"
                              role="menuitem"
                              style={{ padding: '6px 10px', lineHeight: 1.1, flex: '1 1 auto', minWidth: 0 }}
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={() => onShowCol(id)}
                            >
                              {c.title}
                            </button>
                            <button
                              type="button"
                              className="colToggle"
                              title="Вернуть в общий список"
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={(e) => {
                                e.stopPropagation()
                                onMoveHiddenColToBucket(id, 'main')
                              }}
                            >
                              +
                            </button>
                          </div>
                        )
                      })}
                    </div>
                  )}

                  {primaryHiddenCols.map((c) => {
                    const id = String(c.id)
                    return (
                      <div key={id} style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
                        <button
                          type="button"
                          className="collapsedMenuItem"
                          role="menuitem"
                          style={{ padding: '6px 10px', lineHeight: 1.1, flex: '1 1 auto', minWidth: 0 }}
                          onMouseDown={(e) => e.preventDefault()}
                          onClick={() => onShowCol(id)}
                        >
                          {c.title}
                        </button>
                        <button
                          type="button"
                          className="colToggle"
                          title="Перенести в список «Добавить столбец»"
                          onMouseDown={(e) => e.preventDefault()}
                          onClick={(e) => {
                            e.stopPropagation()
                            onMoveHiddenColToBucket(id, 'add')
                          }}
                        >
                          −
                        </button>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )}

          <div className="tableHeadX" ref={headScrollRef}>
            <div className="tableWrapY tableHeadInner" ref={headInnerRef} style={{ width: tableWidth }}>
              {dropHint && <div className="dropIndicator" style={{ left: dropHint.x }} />}
              <table ref={headTableRef} className="table tableFixed tableHead" style={{ width: tableWidth }}>
                <colgroup>{visibleCols.map((c) => <col key={String(c.id)} style={{ width: c.w }} />)}</colgroup>
                <thead onDragOver={onDragOverHeader} onDrop={onDrop}>
                  <tr ref={headerRowRef}>
                    {visibleCols.map((c) => {
                      const id = String(c.id)
                      const isSorted = sortColId === c.id
                      const columnFilter = getColumnFilterState(id)
                      const isFilterOpen = openFilterColId === id
                      return (
                        <th
                          key={id}
                          data-col-id={id}
                          draggable
                          onDragStart={(e) => onDragStart(e, id)}
                          onDragEnd={onDragEnd}
                          onClick={() => onToggleSort(id)}
                          className={`thDraggable ${draggingId === id ? 'thDragging' : ''}`.trim()}
                          title={getSortButtonTitle(isSorted, sortDir)}
                        >
                          <div className="thInner">
                            <button
                              type="button"
                              className="colToggle"
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={(e) => {
                                e.stopPropagation()
                                onHideCol(id)
                              }}
                              title="Скрыть"
                            >
                              −
                            </button>
                            <span className="thLabelGroup">
                              <span className="thTitle" data-table-header-label="true" title={getHeaderTitleText(c)}>{getHeaderTitleText(c)}</span>
                              {isSorted && (
                                <span aria-hidden="true" style={{ fontSize: 10, opacity: 0.72, flex: '0 0 auto' }}>
                                  {sortDir === 'asc' ? '▲' : '▼'}
                                </span>
                              )}
                            </span>
                            <button
                              type="button"
                              className={`thFilterBtn ${columnFilter.active ? 'active' : ''}`.trim()}
                              data-column-filter-trigger="true"
                              aria-haspopup="dialog"
                              aria-expanded={isFilterOpen}
                              title={columnFilter.active ? 'Фильтр включён' : 'Фильтр по столбцу'}
                              draggable={false}
                              ref={(node) => {
                                filterTriggerRefs.current[id] = node
                              }}
                              onMouseDown={(e) => {
                                e.preventDefault()
                                e.stopPropagation()
                              }}
                              onClick={(e) => {
                                e.preventDefault()
                                e.stopPropagation()
                                setFilterOptionQuery('')
                                setOpenFilterColId((prev) => prev === id ? null : id)
                              }}
                            >
                              <svg viewBox="0 0 16 16" className="filterGlyph" aria-hidden="true">
                                <path d="M2.5 3.25h11L9.3 8.05v3.2l-2.6 1.5v-4.7L2.5 3.25Z" fill="none" stroke="currentColor" strokeWidth="1.35" strokeLinejoin="round" />
                                <path d="M5 5.15h6" fill="none" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" />
                                <path d="M5.8 6.85h4.4" fill="none" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" />
                              </svg>
                            </button>
                          </div>
                          <div
                            className="thResizer"
                            title="Изменить ширину (двойной клик — по содержимому)"
                            onClick={(e) => e.stopPropagation()}
                            onMouseDown={(e) => {
                              e.stopPropagation()
                              onStartResize(e, id)
                            }}
                            onDoubleClick={(e) => {
                              e.preventDefault()
                              e.stopPropagation()
                              onAutoSize(id)
                            }}
                          />
                        </th>
                      )
                    })}
                  </tr>
                </thead>
              </table>
            </div>
          </div>

          <div className="tableWrapX" ref={bodyScrollRef}>
            <div className="tableWrapY" ref={bodyInnerRef} style={{ width: tableWidth }}>
              <table ref={bodyTableRef} className="table tableFixed tableBody" style={{ width: tableWidth }}>
                <colgroup>{visibleCols.map((c) => <col key={String(c.id)} style={{ width: c.w }} />)}</colgroup>
                <tbody>
                  {topSpace > 0 && (
                    <tr className="spacerRow">
                      <td colSpan={visibleCols.length} style={{ height: topSpace, padding: 0, border: 'none' }} />
                    </tr>
                  )}

                  {visibleRows.map((p, rowIdx) => (
                    <tr key={getRowKey(p, startRow + rowIdx)}>
                      {visibleCols.map((c) => {
                        const id = String(c.id)
                        const baseCell = cellText(p, c.id)
                        let text = baseCell.text
                        let title = baseCell.title
                        if (id === 'shipment_date' && !hasConfirmedShipmentDate(p)) {
                          text = ''
                          title = undefined
                        }
                        if (id === 'photo_url') {
                          const url = (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : ''
                          return (
                            <td key={id}>
                              <div
                                className="photoCell"
                                onMouseEnter={(e) => {
                                  if (!url) return
                                  queuePhotoPreview(url, p.offer_id, e.clientX, e.clientY)
                                }}
                                onMouseMove={(e) => {
                                  if (!url) return
                                  movePhotoPreview(e.clientX, e.clientY)
                                }}
                                onMouseLeave={hidePhotoPreview}
                              >
                                {url ? (
                                  <>
                                    <img
                                      className="photoThumb"
                                      src={url}
                                      alt={p.offer_id}
                                      loading="lazy"
                                      onError={(e) => {
                                        hidePhotoPreview()
                                        const img = e.currentTarget
                                        img.style.display = 'none'
                                        const fb = img.parentElement?.querySelector('.photoThumbFallback') as HTMLElement | null | undefined
                                        if (fb) fb.style.display = 'flex'
                                      }}
                                    />
                                    <div className="photoThumbFallback" style={{ display: 'none' }}>Нет фото</div>
                                  </>
                                ) : (
                                  <div className="photoThumbFallback">Нет фото</div>
                                )}
                              </div>
                            </td>
                          )
                        }
                        return (
                          <td key={id}>
                            <div className="cellText" title={title ?? text}>{text}</div>
                          </td>
                        )
                      })}
                    </tr>
                  ))}

                  {bottomSpace > 0 && (
                    <tr className="spacerRow">
                      <td colSpan={visibleCols.length} style={{ height: bottomSpace, padding: 0, border: 'none' }} />
                    </tr>
                  )}

                  {empty && (
                    <tr>
                      <td colSpan={visibleCols.length} className="empty">Ничего не найдено.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {openFilterCol && openFilterColId && floatingPopover && (
            <div
              className="columnFilterPopover columnFilterPopoverFloating"
              data-column-filter-popover="true"
              ref={filterPopoverRef}
              style={{ top: floatingPopover.top, left: floatingPopover.left, width: floatingPopover.width }}
              onMouseDown={(e) => e.stopPropagation()}
              onClick={(e) => e.stopPropagation()}
            >
              <div className="columnFilterHeader">
                <div className="columnFilterTitle">{getHeaderTitleText(openFilterCol)}</div>
                {openColumnFilterState.active && (
                  <button
                    type="button"
                    className="columnFilterClearBtn"
                    onClick={(e) => {
                      e.stopPropagation()
                      onClearColumnFilter(openFilterColId)
                    }}
                  >
                    Сбросить
                  </button>
                )}
              </div>
              <div className="columnFilterModes" role="group" aria-label="Режим фильтра">
                {([
                  ['all', 'Все'],
                  ['empty', 'Пустые'],
                  ['nonempty', 'Не пустые'],
                ] as Array<[ColumnFilterMode, string]>).map(([mode, label]) => (
                  <button
                    key={mode}
                    type="button"
                    className={`columnFilterModeBtn ${openColumnFilterState.mode === mode ? 'active' : ''}`.trim()}
                    onClick={(e) => {
                      e.stopPropagation()
                      onColumnFilterModeChange(openFilterColId, mode)
                    }}
                  >
                    {label}
                  </button>
                ))}
              </div>
              <label className="columnFilterField">
                <span>Содержит</span>
                <input
                  value={openColumnFilterState.needle}
                  placeholder="Текст в столбце"
                  onChange={(e) => onColumnFilterNeedleChange(openFilterColId, e.currentTarget.value)}
                />
              </label>
              <label className="columnFilterField compact">
                <span>Значения</span>
                <input
                  value={filterOptionQuery}
                  placeholder="Поиск по значениям"
                  onChange={(e) => setFilterOptionQuery(e.currentTarget.value)}
                />
              </label>
              <div className="columnFilterOptions">
                {openFilterOptions.length > 0 ? openFilterOptions.map((option) => {
                  const checked = openColumnFilterState.selectedKeys.includes(option.key)
                  return (
                    <label key={option.key} className="columnFilterOption">
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => onColumnFilterOptionToggle(openFilterColId, option.key)}
                      />
                      <span className="columnFilterOptionText" title={option.label}>{option.label}</span>
                      <span className="columnFilterOptionCount">{option.count}</span>
                    </label>
                  )
                }) : (
                  <div className="columnFilterEmpty">Нет значений для выбора</div>
                )}
              </div>
            </div>
          )}

          {photoPreview && (
            <div className="photoPreviewPopover" style={{ left: photoPreview.x, top: photoPreview.y }} aria-hidden="true">
              <img className="photoPreviewImage" src={photoPreview.url} alt={photoPreview.alt} loading="eager" decoding="async" />
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

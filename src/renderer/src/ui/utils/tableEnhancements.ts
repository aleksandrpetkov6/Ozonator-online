import { useEffect } from 'react'

const HEADER_WRAP_CLASS = 'tableHeaderWrapActive'
const HEADER_LABEL_CLASS = 'tableHeaderLabel'
const DOM_RESIZER_ATTR = 'data-dom-table-resizer'
const HANDLE_TITLE = 'Подогнать ширину по содержимому столбца'
const AUTO_MIN_W = 60

function getHeaderCells(table: HTMLTableElement): HTMLTableCellElement[] {
  const row = table.tHead?.rows?.[0]
  if (!row) return []
  return Array.from(row.cells)
}

function collectCandidateLabels(th: HTMLTableCellElement): HTMLElement[] {
  const picked: HTMLElement[] = []
  const tryPush = (node: Element | null) => {
    if (!(node instanceof HTMLElement)) return
    if (picked.includes(node)) return
    if (!node.textContent?.trim()) return
    picked.push(node)
  }

  tryPush(th.querySelector<HTMLElement>('[data-table-header-label]'))
  tryPush(th.querySelector<HTMLElement>('.thTitle'))

  const buttonLabel = Array.from(th.querySelectorAll<HTMLElement>('button span')).find((node) => !!node.textContent?.trim())
  tryPush(buttonLabel ?? null)

  const genericSpan = Array.from(th.querySelectorAll<HTMLElement>('span')).find((node) => !!node.textContent?.trim())
  tryPush(genericSpan ?? null)

  if (picked.length === 0 && th.textContent?.trim()) return [th]

  for (const node of picked) {
    node.classList.add(HEADER_LABEL_CLASS)
    if (!node.getAttribute('lang')) node.setAttribute('lang', 'ru')
  }

  return picked
}

function updateHeaderWrap(table: HTMLTableElement) {
  const cells = getHeaderCells(table)
  if (cells.length === 0) {
    table.classList.remove(HEADER_WRAP_CLASS)
    return
  }

  table.classList.remove(HEADER_WRAP_CLASS)

  let needsWrap = false
  for (const th of cells) {
    const labels = collectCandidateLabels(th)
    const measureTargets = labels.length > 0 ? labels : [th]
    for (const target of measureTargets) {
      const overflowX = target.scrollWidth - target.clientWidth
      if (overflowX > 1) {
        needsWrap = true
        break
      }
    }
    if (needsWrap) break
  }

  table.classList.toggle(HEADER_WRAP_CLASS, needsWrap)
}

function ensureColgroup(table: HTMLTableElement, columnCount: number) {
  let colgroup = table.querySelector(':scope > colgroup') as HTMLTableColElement | null
  if (!colgroup) {
    colgroup = document.createElement('colgroup') as unknown as HTMLTableColElement
    table.insertBefore(colgroup as unknown as Node, table.firstChild)
  }

  const host = colgroup as unknown as HTMLTableColElement & HTMLElement
  const cols = Array.from(host.querySelectorAll(':scope > col')) as HTMLTableColElement[]
  const widthSourceRow = table.tHead?.rows?.[0] ?? table.tBodies?.[0]?.rows?.[0] ?? null

  while (cols.length < columnCount) {
    const col = document.createElement('col')
    const idx = cols.length
    const fallbackWidth = widthSourceRow?.cells?.[idx]?.getBoundingClientRect().width
    col.style.width = `${Math.max(AUTO_MIN_W, Math.round(fallbackWidth || AUTO_MIN_W))}px`
    host.appendChild(col)
    cols.push(col)
  }

  for (let i = 0; i < Math.min(cols.length, columnCount); i += 1) {
    const col = cols[i]
    if (col.style.width) continue
    const fallbackWidth = widthSourceRow?.cells?.[i]?.getBoundingClientRect().width
    col.style.width = `${Math.max(AUTO_MIN_W, Math.round(fallbackWidth || AUTO_MIN_W))}px`
  }

  return cols
}

function getMeasurementTable(table: HTMLTableElement): HTMLTableElement | null {
  if (!table.classList.contains('tableHead')) return table
  const wrap = table.closest('.tableWrap')
  return wrap?.querySelector<HTMLTableElement>('table.tableBody') ?? null
}

function getApplyTables(table: HTMLTableElement): HTMLTableElement[] {
  if (!table.classList.contains('tableHead')) return [table]
  const wrap = table.closest('.tableWrap')
  const bodyTable = wrap?.querySelector<HTMLTableElement>('table.tableBody')
  return bodyTable ? [table, bodyTable] : [table]
}

function measureColumnWidth(table: HTMLTableElement, columnIndex: number): number | null {
  const measureTable = getMeasurementTable(table)
  if (!measureTable) return null

  const bodyRows = Array.from(measureTable.tBodies).flatMap((tbody) => Array.from(tbody.rows))
  const usableRows = bodyRows.filter((row) => !row.classList.contains('spacerRow'))
  if (usableRows.length === 0) return null

  let maxContentWidth = 0
  let horizontalPad = 0

  for (const row of usableRows) {
    const cell = row.cells?.[columnIndex] as HTMLTableCellElement | undefined
    if (!cell || cell.colSpan > 1) continue

    if (horizontalPad === 0) {
      const cs = window.getComputedStyle(cell)
      const left = Number.parseFloat(cs.paddingLeft || '0') || 0
      const right = Number.parseFloat(cs.paddingRight || '0') || 0
      horizontalPad = Math.ceil(left + right)
    }

    const content = cell.querySelector<HTMLElement>('.cellText, [data-cell-measure], .photoCell, img') ?? cell
    maxContentWidth = Math.max(maxContentWidth, Math.ceil(content.scrollWidth))
  }

  if (maxContentWidth <= 0) return null
  return Math.max(AUTO_MIN_W, maxContentWidth + horizontalPad)
}

function autoSizeColumn(table: HTMLTableElement, columnIndex: number) {
  if (table.classList.contains('tableHead')) return

  const headerCells = getHeaderCells(table)
  const columnCount = headerCells.length || Math.max(0, columnIndex + 1)
  const nextWidth = measureColumnWidth(table, columnIndex)
  if (!nextWidth) return

  for (const targetTable of getApplyTables(table)) {
    const cols = ensureColgroup(targetTable, columnCount)
    const targetCol = cols[columnIndex]
    if (!targetCol) continue
    targetCol.style.width = `${Math.round(nextWidth)}px`
  }

  updateHeaderWrap(table)
}

function ensureDomResizers(table: HTMLTableElement) {
  const cells = getHeaderCells(table)
  for (const th of cells) {
    if (th.querySelector('.thResizer')) continue

    const resizer = document.createElement('div')
    resizer.className = 'thResizer'
    resizer.title = HANDLE_TITLE
    resizer.setAttribute(DOM_RESIZER_ATTR, 'true')

    resizer.addEventListener('mousedown', (event) => {
      event.preventDefault()
      event.stopPropagation()
    })

    resizer.addEventListener('dblclick', (event) => {
      event.preventDefault()
      event.stopPropagation()
      const columnIndex = Array.from(th.parentElement?.children ?? []).indexOf(th)
      if (columnIndex < 0) return
      const ownerTable = th.closest('table') as HTMLTableElement | null
      if (!ownerTable) return
      autoSizeColumn(ownerTable, columnIndex)
    })

    th.appendChild(resizer)
  }
}

function enhanceTable(table: HTMLTableElement) {
  ensureDomResizers(table)
  updateHeaderWrap(table)
}

export function useGlobalTableEnhancements() {
  useEffect(() => {
    if (typeof window === 'undefined') return
    const root = document.getElementById('root')
    if (!root) return

    let rafId = 0

    const run = () => {
      rafId = 0
      const tables = Array.from(root.querySelectorAll<HTMLTableElement>('table.table'))
      for (const table of tables) enhanceTable(table)
    }

    const schedule = () => {
      if (rafId) return
      rafId = window.requestAnimationFrame(run)
    }

    const mutationObserver = new MutationObserver(() => schedule())
    mutationObserver.observe(root, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: ['style'],
    })

    const resizeObserver = new ResizeObserver(() => schedule())
    resizeObserver.observe(root)
    window.addEventListener('resize', schedule)

    schedule()

    return () => {
      if (rafId) window.cancelAnimationFrame(rafId)
      mutationObserver.disconnect()
      resizeObserver.disconnect()
      window.removeEventListener('resize', schedule)
    }
  }, [])
}

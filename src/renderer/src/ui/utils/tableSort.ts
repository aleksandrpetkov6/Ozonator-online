export type TableSortDir = 'asc' | 'desc'

export type TableSortState<K extends string = string> = {
  colId: K
  dir: TableSortDir
} | null

export type SortableColumn<Row, K extends string = string> = {
  id: K
  sortable?: boolean
  getSortValue?: (row: Row) => unknown
}

export function toggleTableSort<K extends string>(
  prev: TableSortState<K>,
  colId: K,
  sortable = true,
): TableSortState<K> {
  if (!sortable) return prev
  if (prev?.colId === colId) return { colId, dir: prev.dir === 'asc' ? 'desc' : 'asc' }
  return { colId, dir: 'asc' }
}

export function getSortButtonTitle(isSorted: boolean, dir?: TableSortDir): string {
  return isSorted && dir === 'asc' ? 'Сортировка от Я до А' : 'Сортировка от А до Я'
}

function compareTableSortValues(a: unknown, b: unknown, dir: TableSortDir): number {
  const aEmpty = a == null || a === ''
  const bEmpty = b == null || b === ''
  if (aEmpty || bEmpty) {
    if (aEmpty && bEmpty) return 0
    return aEmpty ? 1 : -1
  }

  let result = 0
  if (typeof a === 'number' && typeof b === 'number') result = a - b
  else if (typeof a === 'boolean' && typeof b === 'boolean') result = Number(a) - Number(b)
  else result = String(a).localeCompare(String(b), 'ru', { numeric: true, sensitivity: 'base' })

  return dir === 'asc' ? result : -result
}

function getColumnSortValue<Row, K extends string>(row: Row, col: SortableColumn<Row, K>): unknown {
  if (typeof col.getSortValue === 'function') return col.getSortValue(row)
  return (row as Record<string, unknown>)[String(col.id)] ?? ''
}

export function sortTableRows<Row, K extends string>(
  rows: readonly Row[],
  cols: readonly SortableColumn<Row, K>[],
  sortState: TableSortState<K>,
): Row[] {
  if (!sortState) return rows as Row[]
  const col = cols.find((item) => String(item.id) === String(sortState.colId))
  if (!col || col.sortable === false) return rows as Row[]

  return rows
    .map((row, index) => ({ row, index }))
    .sort((a, b) => {
      const compared = compareTableSortValues(
        getColumnSortValue(a.row, col),
        getColumnSortValue(b.row, col),
        sortState.dir,
      )
      return compared || (a.index - b.index)
    })
    .map((entry) => entry.row)
}

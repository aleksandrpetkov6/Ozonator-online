// src/renderer/testing/uiTestAttrs.ts

type BoolLike = boolean | undefined | null;

function toAttr(v: BoolLike): 'true' | 'false' {
  return v ? 'true' : 'false';
}

export function screenTestAttrs(params: {
  testId: string;
  ready?: BoolLike;
  loading?: BoolLike;
  empty?: BoolLike;
}) {
  return {
    'data-testid': params.testId,
    'data-ready': toAttr(params.ready),
    'data-loading': toAttr(params.loading),
    'data-empty-state': toAttr(params.empty),
  } as const;
}

export function elTestId(testId: string) {
  return { 'data-testid': testId } as const;
}

export function rowTestAttrs(params: { testId: string; rowIndex: number | string }) {
  return {
    'data-testid': params.testId,
    'data-row-index': String(params.rowIndex),
  } as const;
}

export function cellTextTestAttrs(params: {
  testId: string;
  colKey?: string;
  rowId?: string | number;
}) {
  return {
    'data-testid': params.testId,
    ...(params.colKey ? { 'data-col-key': String(params.colKey) } : {}),
    ...(params.rowId !== undefined ? { 'data-row-id': String(params.rowId) } : {}),
  } as const;
}

import type { HTMLAttributes, PropsWithChildren } from 'react';

import { cellTextTestAttrs, elTestId, rowTestAttrs, screenTestAttrs } from './uiTestAttrs';

type BoolLike = boolean | undefined | null;

type ScreenBoxProps = PropsWithChildren<
  HTMLAttributes<HTMLDivElement> & {
    testId: string;
    ready?: BoolLike;
    loading?: BoolLike;
    empty?: BoolLike;
  }
>;

export function ScreenBox(props: ScreenBoxProps) {
  const { testId, ready, loading, empty, children, ...rest } = props;

  return (
    <div {...screenTestAttrs({ testId, ready, loading, empty })} {...rest}>
      {children}
    </div>
  );
}

type TestDivProps = PropsWithChildren<
  HTMLAttributes<HTMLDivElement> & {
    testId: string;
  }
>;

export function TestDiv(props: TestDivProps) {
  const { testId, children, ...rest } = props;
  return (
    <div {...elTestId(testId)} {...rest}>
      {children}
    </div>
  );
}

type GridViewportBoxProps = PropsWithChildren<
  HTMLAttributes<HTMLDivElement> & {
    testId: string;
    loading?: BoolLike;
  }
>;

export function GridViewportBox(props: GridViewportBoxProps) {
  const { testId, loading, children, ...rest } = props;

  return (
    <div
      {...elTestId(testId)}
      data-loading={loading ? 'true' : 'false'}
      {...rest}
    >
      {children}
    </div>
  );
}

type GridRowBoxProps = PropsWithChildren<
  HTMLAttributes<HTMLDivElement> & {
    testId: string;
    rowIndex: number | string;
  }
>;

export function GridRowBox(props: GridRowBoxProps) {
  const { testId, rowIndex, children, ...rest } = props;

  return (
    <div {...rowTestAttrs({ testId, rowIndex })} {...rest}>
      {children}
    </div>
  );
}

type GridCellTextProps = PropsWithChildren<
  HTMLAttributes<HTMLSpanElement> & {
    testId: string;
    colKey?: string;
    rowId?: string | number;
  }
>;

export function GridCellText(props: GridCellTextProps) {
  const { testId, colKey, rowId, children, ...rest } = props;

  return (
    <span {...cellTextTestAttrs({ testId, colKey, rowId })} {...rest}>
      {children}
    </span>
  );
}

import type { HTMLAttributes, InputHTMLAttributes, PropsWithChildren, SelectHTMLAttributes } from 'react';

import { TID } from './uiTestIds';
import { GridCellText, GridRowBox, GridViewportBox, ScreenBox, TestDiv } from './uiTestPrimitives';

type BoolLike = boolean | undefined | null;

export function ProductsScreenRoot(
  props: PropsWithChildren<
    HTMLAttributes<HTMLDivElement> & {
      ready?: BoolLike;
      loading?: BoolLike;
      empty?: BoolLike;
    }
  >
) {
  const { ready, loading, empty, children, ...rest } = props;
  return (
    <ScreenBox
      testId={TID.screenProducts}
      ready={ready}
      loading={loading}
      empty={empty}
      {...rest}
    >
      {children}
    </ScreenBox>
  );
}

export function ProductsGridRoot(props: PropsWithChildren<HTMLAttributes<HTMLDivElement>>) {
  const { children, ...rest } = props;
  return (
    <TestDiv testId={TID.gridProducts} {...rest}>
      {children}
    </TestDiv>
  );
}

export function ProductsGridViewport(
  props: PropsWithChildren<HTMLAttributes<HTMLDivElement> & { loading?: BoolLike }>
) {
  const { loading, children, ...rest } = props;
  return (
    <GridViewportBox testId={TID.gridProductsViewport} loading={loading} {...rest}>
      {children}
    </GridViewportBox>
  );
}

export function ProductsGridRow(
  props: PropsWithChildren<HTMLAttributes<HTMLDivElement> & { rowIndex: number | string }>
) {
  const { rowIndex, children, ...rest } = props;
  return (
    <GridRowBox testId={TID.gridProductsRow} rowIndex={rowIndex} {...rest}>
      {children}
    </GridRowBox>
  );
}

export function ProductsCellText(
  props: PropsWithChildren<HTMLAttributes<HTMLSpanElement> & { colKey?: string; rowId?: string | number }>
) {
  const { colKey, rowId, children, ...rest } = props;
  return (
    <GridCellText testId={TID.gridProductsCellText} colKey={colKey} rowId={rowId} {...rest}>
      {children}
    </GridCellText>
  );
}

export function ProductsSearchInput(props: InputHTMLAttributes<HTMLInputElement>) {
  return <input data-testid={TID.inputSearch} {...props} />;
}

export function ProductsStatusFilter(props: SelectHTMLAttributes<HTMLSelectElement>) {
  return <select data-testid={TID.filterStatus} {...props} />;
}

export function ProductsCategoryFilter(props: SelectHTMLAttributes<HTMLSelectElement>) {
  return <select data-testid={TID.filterCategory} {...props} />;
}

export function ProductsSortPriceButton(
  props: PropsWithChildren<HTMLAttributes<HTMLButtonElement>>
) {
  const { children, ...rest } = props;
  return (
    <button data-testid={TID.sortPrice} {...rest}>
      {children}
    </button>
  );
}

export function ProductsSortNameButton(
  props: PropsWithChildren<HTMLAttributes<HTMLButtonElement>>
) {
  const { children, ...rest } = props;
  return (
    <button data-testid={TID.sortName} {...rest}>
      {children}
    </button>
  );
}

export function ApplyButton(
  props: PropsWithChildren<HTMLAttributes<HTMLButtonElement>>
) {
  const { children, ...rest } = props;
  return (
    <button data-testid={TID.btnApply} {...rest}>
      {children}
    </button>
  );
}

export function ResetButton(
  props: PropsWithChildren<HTMLAttributes<HTMLButtonElement>>
) {
  const { children, ...rest } = props;
  return (
    <button data-testid={TID.btnReset} {...rest}>
      {children}
    </button>
  );
}

export function SaveButton(
  props: PropsWithChildren<HTMLAttributes<HTMLButtonElement>>
) {
  const { children, ...rest } = props;
  return (
    <button data-testid={TID.btnSave} {...rest}>
      {children}
    </button>
  );
}

export function MainLoader(props: PropsWithChildren<HTMLAttributes<HTMLDivElement>>) {
  const { children, ...rest } = props;
  return (
    <div data-testid={TID.loaderMain} {...rest}>
      {children}
    </div>
  );
}

export function EmptyStateBox(props: PropsWithChildren<HTMLAttributes<HTMLDivElement>>) {
  const { children, ...rest } = props;
  return (
    <div data-testid={TID.emptyState} {...rest}>
      {children}
    </div>
  );
}

export function ErrorToastBox(props: PropsWithChildren<HTMLAttributes<HTMLDivElement>>) {
  const { children, ...rest } = props;
  return (
    <div data-testid={TID.toastError} {...rest}>
      {children}
    </div>
  );
}

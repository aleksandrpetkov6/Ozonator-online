// src/renderer/testing/uiTestIds.ts

export const TID = {
  // Screens
  screenProducts: 'screen-products',
  screenOrders: 'screen-orders',
  screenSettings: 'screen-settings',

  // Main grid / table
  gridProducts: 'grid-products',
  gridProductsViewport: 'grid-products-viewport',
  gridProductsRow: 'grid-products-row',
  gridProductsCellText: 'grid-products-cell-text',

  // Common controls
  inputSearch: 'input-search',
  filterStatus: 'filter-status',
  filterCategory: 'filter-category',
  sortPrice: 'sort-price',
  sortName: 'sort-name',
  btnApply: 'btn-apply',
  btnReset: 'btn-reset',
  btnSave: 'btn-save',

  // UI states
  loaderMain: 'loader-main',
  emptyState: 'empty-state',
  toastError: 'toast-error',
  toastSuccess: 'toast-success',
} as const;

export type Secrets = {
  clientId: string
  apiKey: string
  /**
   * Название магазина (не секрет). Кэшируем локально, чтобы не спрашивать Ozon каждый раз.
   * Может быть null/undefined, если ещё не удалось получить.
   */
  storeName?: string | null
}

export type ProductRow = {
  offer_id: string
  product_id?: number | null
  sku?: string | null
  ozon_sku?: string | null
  seller_sku?: string | null
  fbo_sku?: string | null
  fbs_sku?: string | null
  barcode?: string | null
  brand?: string | null
  category?: string | null
  type?: string | null
  name?: string | null
  photo_url?: string | null
  is_visible?: number | boolean | null
  hidden_reasons?: string | null
  created_at?: string | null

  /**
   * Чтобы не смешивать товары разных кабинетов (если вы меняете ключи).
   * Равно Client-Id активного магазина на момент синхронизации.
   */
  store_client_id?: string | null

  archived?: number | null
  updated_at: string
}


export type ProductPlacementRow = {
  store_client_id?: string | null
  warehouse_id: number
  warehouse_name?: string | null
  /**
   * Legacy canonical key kept for backward compatibility.
   * Prefer ozon_sku / seller_sku for matching.
   */
  sku: string
  ozon_sku?: string | null
  seller_sku?: string | null
  placement_zone?: string | null
  updated_at: string
}

export type StockViewRow = ProductRow & {
  warehouse_id?: number | null
  warehouse_name?: string | null
  placement_zone?: string | null
}

export type SyncLogRow = {
  id: number
  type:
    | 'check_auth'
    | 'sync_products'
    | 'app_install'
    | 'app_update'
    | 'app_reinstall'
    | 'app_uninstall'
    | 'admin_settings'
    | string
  status: 'pending' | 'success' | 'error' | string
  started_at: string
  finished_at: string | null
  items_count: number | null
  error_message: string | null
  error_details: string | null
  meta: string | null
}

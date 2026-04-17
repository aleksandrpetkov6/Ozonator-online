import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import SettingsPage from './pages/SettingsPage'
import ProductsPage from './pages/ProductsPage'
import LogsPage from './pages/LogsPage'
import AdminPage from './pages/AdminPage'
import { formatDateTimeRu } from './utils/dateTime'
import { DEFAULT_UI_DATE_RANGE_DAYS, UI_DATE_RANGE_LS_KEY, getDefaultDateRange, readDateRangeWithDefault, sanitizeDateInput, type UiDateRange } from './utils/dateRangeDefaults'
import { useGlobalTableEnhancements } from './utils/tableEnhancements'

const baseTitle = 'Ozonator'
const STORE_NAME_LS_KEY = 'ozonator_store_name'
const DEMAND_FORECAST_PERIOD_LS_KEY = UI_DATE_RANGE_LS_KEY
const APP_UI_DRAFT_LS_KEY = 'ozonator_app_ui_draft'

type DemandForecastPeriod = UiDateRange
type SalesPeriod = UiDateRange

const DEMAND_PERIOD_PRESETS = [DEFAULT_UI_DATE_RANGE_DAYS, 90, 180, 365] as const

function normalizePresetDays(value: unknown): number | null {
  const n = Number(value)
  if (!Number.isFinite(n)) return null
  const normalized = Math.max(1, Math.trunc(n))
  return DEMAND_PERIOD_PRESETS.includes(normalized as (typeof DEMAND_PERIOD_PRESETS)[number]) ? normalized : null
}

function getDateRangeLengthDays(range: UiDateRange | null | undefined): number | null {
  if (!isValidUiDateRange(range)) return null
  const from = Date.parse(`${range.from}T00:00:00.000Z`)
  const to = Date.parse(`${range.to}T00:00:00.000Z`)
  if (!Number.isFinite(from) || !Number.isFinite(to)) return null
  const diffDays = Math.round((to - from) / 86400000)
  if (!Number.isFinite(diffDays) || diffDays < 0) return null
  return diffDays
}

function isSameUiDateRange(left: UiDateRange | null | undefined, right: UiDateRange | null | undefined): boolean {
  if (!isValidUiDateRange(left) || !isValidUiDateRange(right)) return false
  return left.from === right.from && left.to === right.to
}

function inferLegacyRollingSalesPresetDays(range: UiDateRange | null | undefined): number | null {
  if (!isValidUiDateRange(range)) return null
  const todayDefault = getDefaultDateRange(DEFAULT_UI_DATE_RANGE_DAYS)
  if (range.to >= todayDefault.to) return null
  const rangeLengthDays = getDateRangeLengthDays(range)
  if (rangeLengthDays == null) return null

  for (const days of DEMAND_PERIOD_PRESETS) {
    if (rangeLengthDays !== days) continue
    const expected = getDefaultDateRange(days)
    const expectedLengthDays = getDateRangeLengthDays(expected)
    if (expectedLengthDays === rangeLengthDays) return days
  }

  return null
}

function resolveInitialSalesPeriodDraft(draft: AppUiDraft): { period: SalesPeriod; presetDays: number | null } {
  const explicitPresetDays = normalizePresetDays(draft.salesPeriodPresetDays)
  const legacyPresetDays = explicitPresetDays == null ? inferLegacyRollingSalesPresetDays(draft.salesPeriod ?? null) : null
  const presetDays = explicitPresetDays ?? legacyPresetDays
  if (presetDays != null) {
    return { period: getDefaultDateRange(presetDays), presetDays }
  }
  if (isValidUiDateRange(draft.salesPeriod)) {
    return { period: draft.salesPeriod, presetDays: null }
  }
  return { period: getDefaultDateRange(DEFAULT_UI_DATE_RANGE_DAYS), presetDays: DEFAULT_UI_DATE_RANGE_DAYS }
}


function toShortRuDate(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return ''
  const [, m, d] = value.split('-')
  return `${d}.${m}`
}

function formatPeriodBoundary(value: string, boundary: 'startOfDay' | 'endOfDay'): string {
  const formatted = formatDateTimeRu(value, { dateOnlyBoundary: boundary })
  return formatted || ''
}

function readDemandForecastPeriod(): DemandForecastPeriod {
  return readDateRangeWithDefault(DEMAND_FORECAST_PERIOD_LS_KEY, DEFAULT_UI_DATE_RANGE_DAYS)
}

type AppUiDraft = {
  path?: string
  productsQuery?: string
  demandPeriod?: UiDateRange | null
  salesPeriod?: UiDateRange | null
  salesPeriodPresetDays?: number | null
  adminLogLifeDraft?: string
}

function isValidUiDateRange(value: unknown): value is UiDateRange {
  return !!value
    && typeof value === 'object'
    && typeof (value as UiDateRange).from === 'string'
    && typeof (value as UiDateRange).to === 'string'
}

function sanitizeDraftPath(value: unknown): string | null {
  const raw = String(value ?? '').trim()
  if (!raw) return null
  const allowed = new Set(['/', '/sales', '/returns', '/forecast-demand', '/stocks', '/logs', '/settings', '/admin'])
  return allowed.has(raw) ? raw : null
}

function readAppUiDraft(): AppUiDraft {
  try {
    const raw = localStorage.getItem(APP_UI_DRAFT_LS_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return {}
    const draft = parsed as AppUiDraft
    return {
      path: sanitizeDraftPath(draft.path) ?? undefined,
      productsQuery: typeof draft.productsQuery === 'string' ? draft.productsQuery : undefined,
      demandPeriod: isValidUiDateRange(draft.demandPeriod) ? draft.demandPeriod : null,
      salesPeriod: isValidUiDateRange(draft.salesPeriod) ? draft.salesPeriod : null,
      salesPeriodPresetDays: normalizePresetDays((draft as any).salesPeriodPresetDays),
      adminLogLifeDraft: typeof draft.adminLogLifeDraft === 'string' ? draft.adminLogLifeDraft : undefined,
    }
  } catch {
    return {}
  }
}

function writeAppUiDraft(draft: AppUiDraft) {
  try {
    localStorage.setItem(APP_UI_DRAFT_LS_KEY, JSON.stringify({
      path: sanitizeDraftPath(draft.path) ?? '/',
      productsQuery: String(draft.productsQuery ?? ''),
      demandPeriod: isValidUiDateRange(draft.demandPeriod) ? draft.demandPeriod : null,
      salesPeriod: isValidUiDateRange(draft.salesPeriod) ? draft.salesPeriod : null,
      salesPeriodPresetDays: normalizePresetDays(draft.salesPeriodPresetDays),
      adminLogLifeDraft: String(draft.adminLogLifeDraft ?? ''),
    }))
  } catch {
    // ignore
  }
}

const ProductsPageMemo = React.memo(ProductsPage)

function useOnline() {
  const [online, setOnline] = useState<boolean>(true)

  async function check() {
    try {
      const r = await window.api.netCheck()
      setOnline(!!r.online)
    } catch {
      setOnline(false)
    }
  }

  useEffect(() => {
    check()
    const id = setInterval(check, 15000)
    return () => clearInterval(id)
  }, [])

  return online
}

function parseLogLifeDays(value: string): number | null {
  const trimmed = String(value ?? '').trim()
  if (!trimmed) return null
  const n = Number(trimmed)
  if (!Number.isFinite(n)) return null
  const i = Math.trunc(n)
  if (i <= 0) return null
  return i
}

type BootstrapStep = 'idle' | 'needs-secrets' | 'waiting-online' | 'syncing' | 'error' | 'done'

type BootstrapUiState = {
  checked: boolean
  required: boolean
  hasSecrets: boolean
  storageRoot: string
  skipInitialSync: boolean
  step: BootstrapStep
  error: string | null
}

type BootstrapProgressTimelineItem = {
  key: string
  label: string
  status: 'pending' | 'active' | 'done' | 'error'
  startedAt: string | null
  finishedAt: string | null
  detail: string | null
}

type BootstrapProgressUiState = {
  active: boolean
  stageLabel: string
  stageMessage: string
  percent: number
  completedStages: number
  totalStages: number
  currentLoaded: number
  currentTotal: number | null
  currentUnitLabel: string
  etaSeconds: number | null
  error: string | null
  timeline: BootstrapProgressTimelineItem[]
}

const INITIAL_BOOTSTRAP_UI_STATE: BootstrapUiState = {
  checked: false,
  required: false,
  hasSecrets: false,
  storageRoot: '',
  skipInitialSync: false,
  step: 'idle',
  error: null,
}

const INITIAL_BOOTSTRAP_PROGRESS_STATE: BootstrapProgressUiState = {
  active: false,
  stageLabel: '',
  stageMessage: '',
  percent: 0,
  completedStages: 0,
  totalStages: 5,
  currentLoaded: 0,
  currentTotal: null,
  currentUnitLabel: 'этапов',
  etaSeconds: null,
  error: null,
  timeline: [],
}

function formatBootstrapEta(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return 'Уточняется'
  if (seconds < 60) return `~${seconds} сек.`
  const minutes = Math.floor(seconds / 60)
  const restSeconds = seconds % 60
  if (minutes < 60) return restSeconds > 0 ? `~${minutes} мин. ${restSeconds} сек.` : `~${minutes} мин.`
  const hours = Math.floor(minutes / 60)
  const restMinutes = minutes % 60
  return restMinutes > 0 ? `~${hours} ч. ${restMinutes} мин.` : `~${hours} ч.`
}

function formatBootstrapTime(value: string | null | undefined): string {
  if (!value) return ''
  const formatted = formatDateTimeRu(value)
  return formatted || ''
}

export default function App() {
  useGlobalTableEnhancements()
  const location = useLocation()
  const navigate = useNavigate()
  const bootDraft = useMemo(() => readAppUiDraft(), [])
  const online = useOnline()

  const [running, setRunning] = useState(false)
  const runningRef = useRef(false)
  useEffect(() => {
    runningRef.current = running
  }, [running])

  const [lastError, setLastError] = useState<string | null>(null)

  const [storeName, setStoreName] = useState<string>('')
  const [productsQuery, setProductsQuery] = useState(() => bootDraft.productsQuery ?? '')
  const [productsTotal, setProductsTotal] = useState(0)
  const [productsFiltered, setProductsFiltered] = useState(0)
  const [demandPeriod, setDemandPeriod] = useState<DemandForecastPeriod>(() => isValidUiDateRange(bootDraft.demandPeriod) ? bootDraft.demandPeriod : readDemandForecastPeriod())
  const [salesPeriodPresetDays, setSalesPeriodPresetDays] = useState<number | null>(() => resolveInitialSalesPeriodDraft(bootDraft).presetDays)
  const [salesPeriod, setSalesPeriod] = useState<SalesPeriod>(() => resolveInitialSalesPeriodDraft(bootDraft).period)
  const [salesRefreshTick, setSalesRefreshTick] = useState(0)
  const didOnlineBootstrapRef = useRef(false)
  const bootDraftRestoreDoneRef = useRef(false)

  const [adminLoading, setAdminLoading] = useState(true)
  const [adminSaving, setAdminSaving] = useState(false)
  const [adminLogLifeDraft, setAdminLogLifeDraft] = useState(() => bootDraft.adminLogLifeDraft ?? '')
  const [adminLogLifeSaved, setAdminLogLifeSaved] = useState<number>(30)
  const [adminNotice, setAdminNotice] = useState<{ kind: 'success' | 'error'; text: string } | null>(null)
  const [datePresetOpen, setDatePresetOpen] = useState(false)
  const dateRangeRef = useRef<HTMLDivElement | null>(null)
  const [bootstrapUi, setBootstrapUi] = useState<BootstrapUiState>(INITIAL_BOOTSTRAP_UI_STATE)
  const [bootstrapProgress, setBootstrapProgress] = useState<BootstrapProgressUiState>(INITIAL_BOOTSTRAP_PROGRESS_STATE)
  const bootstrapAutoStartedRef = useRef(false)

  const pathname = location.pathname || '/'
  const isLogs = pathname.startsWith('/logs')
  const isSettings = pathname.startsWith('/settings')
  const isAdmin = pathname.startsWith('/admin')
  const isDemandForecast = pathname.startsWith('/forecast-demand')
  const isSales = pathname.startsWith('/sales')
  const isReturns = pathname.startsWith('/returns')
  const isStocks = pathname.startsWith('/stocks')
  const isProducts = !isLogs && !isSettings && !isAdmin && !isDemandForecast && !isSales && !isReturns && !isStocks
  const isDataGridTab = isProducts || isSales || isReturns || isStocks
  const isProductsLike = isDataGridTab || isDemandForecast

  const syncNow = useCallback(async (reason: 'manual' | 'auto' | 'bootstrap' = 'manual') => {
    if (runningRef.current) return { ok: false, error: 'SYNC_ALREADY_RUNNING' }

    setLastError(null)

    if (!online) {
      return { ok: false, error: 'OFFLINE' }
    }

    const st = await window.api.secretsStatus()
    if (!st.hasSecrets) {
      const error = 'Ключи не сохранены. Откройте Настройки.'
      if (reason === 'manual' || reason === 'bootstrap') setLastError(error)
      return { ok: false, error }
    }

    setRunning(true)

    try {
      const isSalesRefresh = reason !== 'bootstrap' && isSales
      if (isSalesRefresh) {
        const resp = await window.api.refreshSales(salesPeriod)
        if (!resp.ok) {
          const error = resp.error ?? 'Ошибка обновления продаж'
          setLastError(error)
          return { ok: false, error }
        }
        setLastError(null)
        setSalesRefreshTick((prev) => prev + 1)
        window.dispatchEvent(new Event('ozon:products-updated'))
        window.dispatchEvent(new Event('ozon:logs-updated'))
        return { ok: true }
      }

      const resp = await window.api.syncProducts(salesPeriod)
      if (!resp.ok) {
        const error = resp.error ?? 'Ошибка синхронизации'
        setLastError(error)
        return { ok: false, error }
      }

      setLastError(null)
      setSalesRefreshTick((prev) => prev + 1)
      window.dispatchEvent(new Event('ozon:products-updated'))
      window.dispatchEvent(new Event('ozon:logs-updated'))
      window.dispatchEvent(new Event('ozon:store-updated'))
      return { ok: true }
    } finally {
      setRunning(false)
    }
  }, [isSales, online, salesPeriod])

  useEffect(() => {
    if (bootDraftRestoreDoneRef.current) return
    bootDraftRestoreDoneRef.current = true
    const nextPath = sanitizeDraftPath(bootDraft.path)
    if (!nextPath || nextPath === pathname) return
    navigate(nextPath, { replace: true })
  }, [bootDraft.path, navigate, pathname])

  useEffect(() => {
    writeAppUiDraft({
      path: pathname,
      productsQuery,
      demandPeriod,
      salesPeriod,
      salesPeriodPresetDays,
      adminLogLifeDraft,
    })
  }, [pathname, productsQuery, demandPeriod, salesPeriod, salesPeriodPresetDays, adminLogLifeDraft])

  useEffect(() => {
    const flushDraft = () => {
      writeAppUiDraft({
        path: pathname,
        productsQuery,
        demandPeriod,
        salesPeriod,
        salesPeriodPresetDays,
        adminLogLifeDraft,
      })
    }

    window.addEventListener('ozon:prepare-install-exit', flushDraft)
    return () => window.removeEventListener('ozon:prepare-install-exit', flushDraft)
  }, [pathname, productsQuery, demandPeriod, salesPeriod, salesPeriodPresetDays, adminLogLifeDraft])

  const onProductStats = useCallback((s: { total: number; filtered: number }) => {
    setProductsTotal(s.total)
    setProductsFiltered(s.filtered)
  }, [])

  useEffect(() => {
    try {
      localStorage.setItem(DEMAND_FORECAST_PERIOD_LS_KEY, JSON.stringify(demandPeriod))
    } catch {
      // ignore
    }
  }, [demandPeriod])

  const setDemandPeriodField = useCallback((field: keyof DemandForecastPeriod, value: string) => {
    const normalized = sanitizeDateInput(value)
    setDemandPeriod((prev) => ({ ...prev, [field]: normalized }))
  }, [])

  const setSalesPeriodField = useCallback((field: keyof SalesPeriod, value: string) => {
    const normalized = sanitizeDateInput(value)
    setSalesPeriodPresetDays(null)
    setSalesPeriod((prev) => ({ ...prev, [field]: normalized }))
  }, [])

  useEffect(() => {
    if (salesPeriodPresetDays == null) return

    const syncRollingPeriod = () => {
      const next = getDefaultDateRange(salesPeriodPresetDays)
      setSalesPeriod((prev) => (isSameUiDateRange(prev, next) ? prev : next))
    }

    syncRollingPeriod()
    const id = window.setInterval(syncRollingPeriod, 60 * 1000)
    return () => window.clearInterval(id)
  }, [salesPeriodPresetDays])

  const activePeriod = isSales ? salesPeriod : demandPeriod

  const setActivePeriodField = useCallback((field: keyof UiDateRange, value: string) => {
    if (isSales) {
      setSalesPeriodField(field, value)
      return
    }
    setDemandPeriodField(field, value)
  }, [isSales, setDemandPeriodField, setSalesPeriodField])

  const applyActivePreset = useCallback((days: number) => {
    const next = getDefaultDateRange(days)
    if (isSales) {
      setSalesPeriodPresetDays(days)
      setSalesPeriod(next)
      return
    }
    setDemandPeriod(next)
  }, [isSales])

  const activePresetDays = useMemo(() => {
    for (const days of DEMAND_PERIOD_PRESETS) {
      const preset = getDefaultDateRange(days)
      if (preset.from === activePeriod.from && preset.to === activePeriod.to) return days
    }
    return null
  }, [activePeriod.from, activePeriod.to])

  const dateTriggerLabel = useMemo(() => {
    const from = toShortRuDate(activePeriod.from)
    const to = toShortRuDate(activePeriod.to)
    if (from && to) return `${from}—${to}`
    if (from) return `с ${from}`
    if (to) return `по ${to}`
    return 'Указать промежуток'
  }, [activePeriod.from, activePeriod.to])

  const dateTriggerTitle = useMemo(() => {
    const from = formatPeriodBoundary(activePeriod.from, 'startOfDay')
    const to = formatPeriodBoundary(activePeriod.to, 'endOfDay')
    if (from && to) return `${from} — ${to}`
    if (from) return `с ${from}`
    if (to) return `по ${to}`
    return 'Указать промежуток'
  }, [activePeriod.from, activePeriod.to])

  useEffect(() => {
    setDatePresetOpen(false)
  }, [pathname])

  useEffect(() => {
    if (!datePresetOpen) return

    const onPointerDown = (ev: MouseEvent) => {
      const host = dateRangeRef.current
      if (!host) return
      if (host.contains(ev.target as Node)) return
      setDatePresetOpen(false)
    }

    const onEscape = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') setDatePresetOpen(false)
    }

    window.addEventListener('mousedown', onPointerDown)
    window.addEventListener('keydown', onEscape)

    return () => {
      window.removeEventListener('mousedown', onPointerDown)
      window.removeEventListener('keydown', onEscape)
    }
  }, [datePresetOpen])

  async function refreshStoreName() {
    try {
      const resp = await window.api.loadSecrets()
      if (resp.ok) {
        const raw = (resp.secrets as any).storeName
        const cleaned = typeof raw === 'string' && raw.trim() ? raw.trim() : ''
        if (cleaned) {
          setStoreName(cleaned)
          try {
            localStorage.setItem(STORE_NAME_LS_KEY, cleaned)
          } catch {
            /* ignore */
          }
          document.title = `${baseTitle} 🤝 ${cleaned}`
          return
        }
      }
    } catch {
      // ignore
    }

    try {
      const raw = localStorage.getItem(STORE_NAME_LS_KEY) ?? ''
      const cleaned = raw.trim()
      if (cleaned) {
        setStoreName(cleaned)
        document.title = `${baseTitle} 🤝 ${cleaned}`
        return
      }
    } catch {
      // ignore
    }

    setStoreName('')
    document.title = baseTitle
  }

  useEffect(() => {
    refreshStoreName()
    const onStore = () => refreshStoreName()
    window.addEventListener('ozon:store-updated', onStore)
    return () => window.removeEventListener('ozon:store-updated', onStore)
  }, [])

  const refreshBootstrapUi = useCallback(async () => {
    try {
      const resp = await window.api.getBootstrapState()
      if (!resp.ok) throw new Error(resp.error ?? 'Не удалось определить состояние данных')

      const required = !!resp.requiresInitialSync
      const hasSecrets = !!resp.hasSecrets
      const storageRoot = String(resp.storageRoot ?? '')
      const skipInitialSync = !!resp.skipInitialSync

      setBootstrapUi((prev) => {
        const nextStep: BootstrapStep = !required
          ? 'done'
          : !hasSecrets
            ? 'needs-secrets'
            : !online
              ? 'waiting-online'
              : (prev.step === 'syncing' ? 'syncing' : prev.step === 'error' ? 'error' : 'idle')

        return {
          checked: true,
          required,
          hasSecrets,
          storageRoot,
          skipInitialSync,
          step: nextStep,
          error: nextStep === 'error' ? prev.error : null,
        }
      })

      if (!required) bootstrapAutoStartedRef.current = false
    } catch (e: any) {
      setBootstrapUi((prev) => ({
        ...prev,
        checked: true,
        required: true,
        skipInitialSync: false,
        step: 'error',
        error: e?.message ?? 'Не удалось подготовить данные',
      }))
    }
  }, [online])

  const refreshBootstrapProgress = useCallback(async () => {
    try {
      const resp = await window.api.getBootstrapProgress()
      if (!resp.ok) return
      setBootstrapProgress({
        active: !!resp.active,
        stageLabel: String(resp.stageLabel ?? ''),
        stageMessage: String(resp.stageMessage ?? ''),
        percent: Math.max(0, Math.min(100, Math.round(Number(resp.percent ?? 0) || 0))),
        completedStages: Math.max(0, Math.trunc(Number(resp.completedStages ?? 0) || 0)),
        totalStages: Math.max(1, Math.trunc(Number(resp.totalStages ?? 5) || 5)),
        currentLoaded: Math.max(0, Math.trunc(Number(resp.currentLoaded ?? 0) || 0)),
        currentTotal: typeof resp.currentTotal === 'number' && Number.isFinite(resp.currentTotal) ? Math.max(0, Math.trunc(resp.currentTotal)) : null,
        currentUnitLabel: String(resp.currentUnitLabel ?? 'этапов'),
        etaSeconds: typeof resp.etaSeconds === 'number' && Number.isFinite(resp.etaSeconds) ? Math.max(0, Math.round(resp.etaSeconds)) : null,
        error: typeof resp.error === 'string' && resp.error.trim() ? resp.error : null,
        timeline: Array.isArray(resp.timeline) ? resp.timeline.map((item) => ({
          key: String(item?.key ?? ''),
          label: String(item?.label ?? ''),
          status: item?.status === 'active' || item?.status === 'done' || item?.status === 'error' ? item.status : 'pending',
          startedAt: typeof item?.startedAt === 'string' ? item.startedAt : null,
          finishedAt: typeof item?.finishedAt === 'string' ? item.finishedAt : null,
          detail: typeof item?.detail === 'string' ? item.detail : null,
        })) : [],
      })
    } catch {
      // ignore
    }
  }, [])

  useEffect(() => {
    void refreshBootstrapUi()
    const onStore = () => { void refreshBootstrapUi() }
    window.addEventListener('ozon:store-updated', onStore)
    return () => window.removeEventListener('ozon:store-updated', onStore)
  }, [refreshBootstrapUi])

  useEffect(() => {
    if (!bootstrapUi.checked || !bootstrapUi.required) {
      setBootstrapProgress(INITIAL_BOOTSTRAP_PROGRESS_STATE)
      return
    }

    void refreshBootstrapProgress()
    const timer = window.setInterval(() => {
      void refreshBootstrapProgress()
    }, 700)

    return () => window.clearInterval(timer)
  }, [bootstrapUi.checked, bootstrapUi.required, refreshBootstrapProgress])

  useEffect(() => {
    if (!bootstrapUi.checked || !bootstrapUi.required || bootstrapUi.hasSecrets) return
    if (pathname !== '/settings') {
      navigate('/settings', { replace: true })
    }
  }, [bootstrapUi.checked, bootstrapUi.required, bootstrapUi.hasSecrets, navigate, pathname])

  useEffect(() => {
    if (!bootstrapUi.checked || !bootstrapUi.required) return

    if (!bootstrapUi.hasSecrets) {
      bootstrapAutoStartedRef.current = false
      if (bootstrapUi.step !== 'needs-secrets' || bootstrapUi.error) {
        setBootstrapUi((prev) => ({ ...prev, step: 'needs-secrets', error: null }))
      }
      return
    }

    if (bootstrapUi.skipInitialSync) {
      bootstrapAutoStartedRef.current = false
      if (bootstrapUi.step !== 'idle' || bootstrapUi.error) {
        setBootstrapUi((prev) => ({ ...prev, step: 'idle', error: null }))
      }
      return
    }

    if (!online) {
      bootstrapAutoStartedRef.current = false
      if (bootstrapUi.step !== 'waiting-online' || bootstrapUi.error) {
        setBootstrapUi((prev) => ({ ...prev, step: 'waiting-online', error: null }))
      }
      return
    }

    if (runningRef.current || bootstrapUi.step === 'syncing' || bootstrapUi.step === 'done') return
    if (bootstrapAutoStartedRef.current && bootstrapUi.step !== 'idle') return

    bootstrapAutoStartedRef.current = true
    let cancelled = false

    setBootstrapUi((prev) => ({ ...prev, step: 'syncing', error: null }))

    void (async () => {
      const resp = await syncNow('bootstrap')
      if (cancelled) return

      if (resp.ok) {
        bootstrapAutoStartedRef.current = false
        try {
          const state = await window.api.getBootstrapState()
          if (!cancelled && state.ok && !state.requiresInitialSync) {
            setBootstrapUi({
              checked: true,
              required: false,
              hasSecrets: !!state.hasSecrets,
              storageRoot: String(state.storageRoot ?? ''),
              skipInitialSync: !!state.skipInitialSync,
              step: 'done',
              error: null,
            })
          } else if (!cancelled) {
            setBootstrapUi((prev) => ({ ...prev, required: false, step: 'done', error: null }))
          }
        } catch {
          if (!cancelled) {
            setBootstrapUi((prev) => ({ ...prev, required: false, step: 'done', error: null }))
          }
        }
        return
      }

      bootstrapAutoStartedRef.current = false
      if (cancelled) return

      const error = resp.error === 'OFFLINE'
        ? 'Нет соединения с интернетом. Как только сеть появится, загрузка продолжится.'
        : resp.error === 'SYNC_ALREADY_RUNNING'
          ? null
          : (resp.error ?? 'Не удалось загрузить данные.')

      if (resp.error === 'OFFLINE') {
        setBootstrapUi((prev) => ({ ...prev, step: 'waiting-online', error: null }))
        return
      }

      if (resp.error === 'SYNC_ALREADY_RUNNING') return

      setBootstrapUi((prev) => ({ ...prev, step: 'error', error }))
    })()

    return () => {
      cancelled = true
    }
  }, [bootstrapUi.checked, bootstrapUi.required, bootstrapUi.hasSecrets, bootstrapUi.skipInitialSync, bootstrapUi.step, bootstrapUi.error, online, syncNow])

  useEffect(() => {
    let cancelled = false

    async function loadAdmin() {
      setAdminLoading(true)
      try {
        const resp = await window.api.getAdminSettings()
        if (cancelled) return
        if (!resp.ok) throw new Error(resp.error ?? 'Не удалось загрузить настройки Админ')

        const days = Math.max(1, Math.trunc(Number(resp.logRetentionDays) || 30))
        setAdminLogLifeSaved(days)
        setAdminLogLifeDraft((prev) => prev.trim() ? prev : String(days))
        setAdminNotice(null)
      } catch (e: any) {
        if (cancelled) return
        setAdminNotice({ kind: 'error', text: e?.message ?? 'Не удалось загрузить настройки Админ' })
      } finally {
        if (!cancelled) setAdminLoading(false)
      }
    }

    loadAdmin()
    return () => {
      cancelled = true
    }
  }, [])

  const dotState = useMemo(() => {
    if (!online) return 'offline'
    if (running) return 'running'
    if (lastError) return 'error'
    return 'ok'
  }, [online, running, lastError])



  useEffect(() => {
    if (!didOnlineBootstrapRef.current) {
      didOnlineBootstrapRef.current = true
      return
    }

    if (!online) return

    try {
      window.dispatchEvent(new Event('ozon:products-updated'))
      window.dispatchEvent(new Event('ozon:logs-updated'))
      window.dispatchEvent(new Event('ozon:store-updated'))
    } catch {
      // ignore
    }
  }, [online])

  const saveAdmin = useCallback(async () => {
    const parsed = parseLogLifeDays(adminLogLifeDraft)
    if (!parsed) {
      setAdminNotice({ kind: 'error', text: 'Поле «Жизнь лога» должно быть целым числом больше 0.' })
      return
    }

    setAdminSaving(true)
    setAdminNotice(null)

    try {
      const resp = await window.api.saveAdminSettings({ logRetentionDays: parsed })
      if (!resp.ok) throw new Error(resp.error ?? 'Не удалось сохранить настройки Админ')

      const saved = Math.max(1, Math.trunc(Number(resp.logRetentionDays) || parsed))
      setAdminLogLifeSaved(saved)
      setAdminLogLifeDraft(String(saved))
      window.dispatchEvent(new Event('ozon:logs-updated'))
    } catch (e: any) {
      setAdminNotice({ kind: 'error', text: e?.message ?? 'Не удалось сохранить настройки Админ' })
    } finally {
      setAdminSaving(false)
    }
  }, [adminLogLifeDraft])

  const adminParsed = parseLogLifeDays(adminLogLifeDraft)
  const adminDirty = adminParsed !== null ? adminParsed !== adminLogLifeSaved : adminLogLifeDraft.trim() !== String(adminLogLifeSaved)
  const visibleLastError = lastError && lastError !== 'Нет интернета' ? lastError : null
  const showBootstrapWelcome = bootstrapUi.checked && bootstrapUi.required && !bootstrapUi.skipInitialSync && bootstrapUi.step !== 'done' && bootstrapUi.step !== 'needs-secrets'
  const bootstrapTitle = bootstrapUi.step === 'waiting-online'
    ? 'Ждём интернет для первой загрузки'
    : bootstrapUi.step === 'error'
      ? 'Загрузка данных остановилась'
      : 'Подготавливаем данные'
  const bootstrapCompletedStages = Math.min(bootstrapProgress.totalStages, Math.max(0, bootstrapProgress.completedStages))
  const bootstrapRemainingStages = Math.max(0, bootstrapProgress.totalStages - bootstrapCompletedStages)
  const bootstrapEta = formatBootstrapEta(bootstrapProgress.etaSeconds)
  const bootstrapCurrentStageNumber = Math.min(
    bootstrapProgress.totalStages,
    Math.max(1, bootstrapProgress.completedStages + (bootstrapUi.step === 'syncing' || bootstrapUi.step === 'waiting-online' ? 1 : 0)),
  )
  const bootstrapTimelineItems = bootstrapProgress.timeline.filter((item) => item.status !== 'pending' || item.detail || item.startedAt || item.finishedAt)

  const titleLogoSrc = './brand/ozonator-title-logo.png'



  return (
    <div className="appShell">
      <div className="windowTitlebar">
        <div className="windowTitlebarInner">
          <img className="windowTitlebarLogo" src={titleLogoSrc} alt="Ozonator" draggable={false} />
          <div className="windowTitlebarText" aria-label="Магазин">
            {storeName ? (
              <>
                <span className="windowTitlebarHandshake" aria-hidden>🤝</span>
                <span className="windowTitlebarStore">{storeName}</span>
              </>
            ) : (
              <span className="windowTitlebarStorePlaceholder" aria-hidden> </span>
            )}
          </div>
        </div>
      </div>
      <div className="topbar">

        <div className="topbarInner">
          <div className="topbarLeft">
            <NavLink
              end
              to="/"
              className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
              title="Товары"
            >
              Товары
            </NavLink>

            {isSales ? (
              <div className="topbarDateChipHost topbarDateChipHostSales" ref={dateRangeRef} aria-label="Период продаж">
                <button
                  type="button"
                  className={`topbarDateTrigger${datePresetOpen ? ' open' : ''}`}
                  onClick={() => setDatePresetOpen((v) => !v)}
                  title={dateTriggerTitle}
                  aria-haspopup="dialog"
                  aria-expanded={datePresetOpen}
                >
                  <span className="topbarDateTriggerText">{dateTriggerLabel}</span>
                  <span className="topbarDateTriggerIcon" aria-hidden>📅</span>
                </button>

                {datePresetOpen && (
                  <div className="topbarDatePopover" role="dialog" aria-label="Период продаж">
                    <div className="topbarDatePopoverFields">
                      <label className="topbarDatePopoverField">
                        <span>С</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={activePeriod.from}
                          onChange={(e) => setActivePeriodField('from', e.target.value)}
                        />
                      </label>
                      <label className="topbarDatePopoverField">
                        <span>По</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={activePeriod.to}
                          onChange={(e) => setActivePeriodField('to', e.target.value)}
                        />
                      </label>
                    </div>

                    <div className="topbarDatePopoverPresets" role="menu" aria-label="Шаблоны периода">
                      {DEMAND_PERIOD_PRESETS.map((days) => (
                        <button
                          key={days}
                          type="button"
                          role="menuitem"
                          className={`topbarDatePresetBtn${activePresetDays === days ? ' active' : ''}`}
                          onClick={() => {
                            applyActivePreset(days)
                            setDatePresetOpen(false)
                          }}
                        >
                          {days}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <NavLink
                to="/sales"
                className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
                title="Продажи"
              >
                Продажи
              </NavLink>
            )}

            {isReturns ? (
              <div className="topbarDateChipHost topbarDateChipHostReturns" ref={dateRangeRef} aria-label="Период возвратов">
                <button
                  type="button"
                  className={`topbarDateTrigger${datePresetOpen ? ' open' : ''}`}
                  onClick={() => setDatePresetOpen((v) => !v)}
                  title={dateTriggerTitle}
                  aria-haspopup="dialog"
                  aria-expanded={datePresetOpen}
                >
                  <span className="topbarDateTriggerText">{dateTriggerLabel}</span>
                  <span className="topbarDateTriggerIcon" aria-hidden>📅</span>
                </button>

                {datePresetOpen && (
                  <div className="topbarDatePopover" role="dialog" aria-label="Период возвратов">
                    <div className="topbarDatePopoverFields">
                      <label className="topbarDatePopoverField">
                        <span>С</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={activePeriod.from}
                          onChange={(e) => setActivePeriodField('from', e.target.value)}
                        />
                      </label>
                      <label className="topbarDatePopoverField">
                        <span>По</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={activePeriod.to}
                          onChange={(e) => setActivePeriodField('to', e.target.value)}
                        />
                      </label>
                    </div>

                    <div className="topbarDatePopoverPresets" role="menu" aria-label="Шаблоны периода">
                      {DEMAND_PERIOD_PRESETS.map((days) => (
                        <button
                          key={days}
                          type="button"
                          role="menuitem"
                          className={`topbarDatePresetBtn${activePresetDays === days ? ' active' : ''}`}
                          onClick={() => {
                            applyActivePreset(days)
                            setDatePresetOpen(false)
                          }}
                        >
                          {days}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <NavLink
                to="/returns"
                className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
                title="Возвраты"
              >
                Возвраты
              </NavLink>
            )}

            {isDemandForecast ? (
              <div className="topbarDateChipHost topbarDateChipHostForecast" ref={dateRangeRef} aria-label="Период прогноза спроса">
                <button
                  type="button"
                  className={`topbarDateTrigger${datePresetOpen ? ' open' : ''}`}
                  onClick={() => setDatePresetOpen((v) => !v)}
                  title={dateTriggerTitle}
                  aria-haspopup="dialog"
                  aria-expanded={datePresetOpen}
                >
                  <span className="topbarDateTriggerText">{dateTriggerLabel}</span>
                  <span className="topbarDateTriggerIcon" aria-hidden>📅</span>
                </button>

                {datePresetOpen && (
                  <div className="topbarDatePopover" role="dialog" aria-label="Период прогноза спроса">
                    <div className="topbarDatePopoverFields">
                      <label className="topbarDatePopoverField">
                        <span>С</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={activePeriod.from}
                          onChange={(e) => setActivePeriodField('from', e.target.value)}
                        />
                      </label>
                      <label className="topbarDatePopoverField">
                        <span>По</span>
                        <input
                          type="date"
                          className="topbarDatePopoverInput"
                          value={activePeriod.to}
                          onChange={(e) => setActivePeriodField('to', e.target.value)}
                        />
                      </label>
                    </div>

                    <div className="topbarDatePopoverPresets" role="menu" aria-label="Шаблоны периода">
                      {DEMAND_PERIOD_PRESETS.map((days) => (
                        <button
                          key={days}
                          type="button"
                          role="menuitem"
                          className={`topbarDatePresetBtn${activePresetDays === days ? ' active' : ''}`}
                          onClick={() => {
                            applyActivePreset(days)
                            setDatePresetOpen(false)
                          }}
                        >
                          {days}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <NavLink
                to="/forecast-demand"
                className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
                title="Прогноз спроса"
              >
                Прогноз спроса
              </NavLink>
            )}

            <NavLink
              to="/stocks"
              className={({ isActive }) => `navChip${isActive ? ' active' : ''}`}
              title="Остатки"
            >
              Остатки
            </NavLink>

            {isProductsLike && (
              <div className="topbarSearch">
                <div className="searchWrap">
                  <input
                    className="searchInput search"
                    value={productsQuery}
                    onChange={(e) => setProductsQuery(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Escape') {
                        e.preventDefault()
                        e.stopPropagation()
                        setProductsQuery('')
                      }
                    }}
                    placeholder="Поиск по таблице…"
                  />
                  {productsQuery && (
                    <button
                      type="button"
                      className="searchClearBtn"
                      title="Очистить"
                      aria-label="Очистить"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => setProductsQuery('')}
                    >
                      ×
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>

          <div className="topbarRight">
            <NavLink className="iconLink" to="/logs" title="Лог">
              🗒️
            </NavLink>

            <NavLink className="iconLink" to="/settings" title="Настройки">
              ⚙️
            </NavLink>

            {isAdmin && (
              <button
                type="button"
                className={`topbarSaveBtn${adminDirty ? ' isDirty' : ''}`}
                onClick={saveAdmin}
                disabled={adminLoading || adminSaving}
                title={adminSaving ? 'Сохранение…' : 'Сохранить настройки Админ'}
              >
                {adminSaving ? 'Сохранение…' : 'Сохранить'}
              </button>
            )}

            <NavLink className="iconLink" to="/admin" title="Админ">
              🛡️
            </NavLink>

            <button
              className={`iconBtn syncBtn ${running ? 'running' : ''}`}
              title={online ? (running ? 'Синхронизация…' : 'Синхронизировать сейчас') : 'Оффлайн'}
              onClick={() => syncNow('manual')}
              disabled={!online || running}
            >
              <span className={`syncBtnDot ${dotState}`} aria-hidden>
                {running ? <span className="syncSpinner" /> : <span className="syncCheck" />}
              </span>
            </button>
          </div>
        </div>
      </div>

      <div className="pageArea">
        <div className={isProductsLike ? 'container containerWide bootstrapWelcomeHost' : 'container bootstrapWelcomeHost'}>
          {showBootstrapWelcome && (
            <div className="bootstrapWelcomeOverlay">
              <div className="bootstrapWelcomeCard bootstrapProgressCard">
                <div className="bootstrapWelcomeBadge">Первая загрузка данных</div>
                <h2 className="bootstrapWelcomeTitle">{bootstrapTitle}</h2>

                <div className="bootstrapWelcomeStatus bootstrapProgressStatus">
                  <span className={`bootstrapWelcomeDot ${bootstrapUi.step}`} aria-hidden />
                  <div className="bootstrapProgressStatusText">
                    <strong>
                      {bootstrapUi.step === 'syncing' && (bootstrapProgress.stageMessage || 'Загрузка идёт. Окно можно не закрывать.')}
                      {bootstrapUi.step === 'waiting-online' && 'Нет сети. Как только интернет появится, загрузка продолжится.'}
                      {bootstrapUi.step === 'error' && (bootstrapUi.error ?? bootstrapProgress.error ?? 'Загрузка остановилась с ошибкой.')}
                    </strong>
                    <span>
                      {bootstrapProgress.stageLabel
                        ? `Этап ${bootstrapCurrentStageNumber} из ${bootstrapProgress.totalStages}: ${bootstrapProgress.stageLabel}`
                        : 'Подготавливаем загрузку'}
                    </span>
                  </div>
                </div>

                <div className="bootstrapProgressBar" aria-hidden>
                  <div className="bootstrapProgressBarFill" style={{ width: `${bootstrapProgress.percent}%` }} />
                </div>

                <div className="bootstrapProgressMetrics">
                  <div className="bootstrapProgressMetric">
                    <span className="bootstrapProgressMetricLabel">Загружено</span>
                    <strong>{bootstrapCompletedStages} из {bootstrapProgress.totalStages} этапов</strong>
                  </div>
                  <div className="bootstrapProgressMetric">
                    <span className="bootstrapProgressMetricLabel">Осталось</span>
                    <strong>{bootstrapRemainingStages} этапов</strong>
                  </div>
                  <div className="bootstrapProgressMetric">
                    <span className="bootstrapProgressMetricLabel">Готовность</span>
                    <strong>{bootstrapProgress.percent}%</strong>
                  </div>
                  <div className="bootstrapProgressMetric">
                    <span className="bootstrapProgressMetricLabel">Оценка времени</span>
                    <strong>{bootstrapEta}</strong>
                  </div>
                </div>

                <div className="bootstrapProgressTimeline">
                  <div className="bootstrapProgressTimelineTitle">Таймлайн загрузки</div>
                  <div className="bootstrapProgressTimelineList">
                    {bootstrapTimelineItems.map((item) => (
                      <div key={item.key} className={`bootstrapProgressTimelineItem ${item.status}`}>
                        <span className="bootstrapProgressTimelineMarker" aria-hidden />
                        <div className="bootstrapProgressTimelineBody">
                          <div className="bootstrapProgressTimelineHead">
                            <strong>{item.label}</strong>
                            <span>
                              {item.startedAt && item.finishedAt
                                ? `${formatBootstrapTime(item.startedAt)} → ${formatBootstrapTime(item.finishedAt)}`
                                : item.startedAt
                                  ? formatBootstrapTime(item.startedAt)
                                  : 'ожидание'}
                            </span>
                          </div>
                          {(item.detail || item.status === 'active') && (
                            <div className="bootstrapProgressTimelineDetail">
                              {item.detail || bootstrapProgress.stageMessage || 'Выполняется…'}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="bootstrapWelcomeActions">
                  {bootstrapUi.step === 'error' && (
                    <button
                      type="button"
                      onClick={() => {
                        bootstrapAutoStartedRef.current = false
                        setBootstrapUi((prev) => ({ ...prev, step: 'idle', error: null }))
                      }}
                    >
                      Повторить загрузку
                    </button>
                  )}
                </div>
              </div>
            </div>
          )}

          {visibleLastError && <div className="notice error">{visibleLastError}</div>}

          {isProducts && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="products" viewKey="products" dataset="products" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isSales && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key={`sales:${salesPeriod.from || "-"}:${salesPeriod.to || "-"}:${salesRefreshTick}`} viewKey="sales" dataset="sales" query={productsQuery} period={salesPeriod} onStats={onProductStats} />
            </div>
          )}

          {isReturns && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="returns" viewKey="returns" dataset="returns" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isStocks && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="stocks" viewKey="stocks" dataset="stocks" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isDemandForecast && (
            <div style={{ height: '100%' }}>
              <ProductsPageMemo key="forecast-demand" viewKey="forecast-demand" dataset="products" query={productsQuery} onStats={onProductStats} />
            </div>
          )}

          {isLogs && (
            <div style={{ height: '100%' }}>
              <LogsPage />
            </div>
          )}

          {isAdmin && (
            <div style={{ height: '100%' }}>
              <AdminPage
                loading={adminLoading}
                saving={adminSaving}
                logLifeDaysValue={adminLogLifeDraft}
                onChangeLogLifeDays={(v) => {
                  setAdminLogLifeDraft(v)
                  if (adminNotice) setAdminNotice(null)
                }}
                notice={adminNotice}
                currentSavedDays={adminLogLifeSaved}
              />
            </div>
          )}

          {isSettings && (
            <div style={{ height: '100%' }}>
              <SettingsPage />
            </div>
          )}

          {productsTotal /* noop */ && false}
          {productsFiltered /* noop */ && false}
        </div>
      </div>
    </div>
  )
}

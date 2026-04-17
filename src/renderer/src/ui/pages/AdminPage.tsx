import React from 'react'

type Props = {
  loading: boolean
  saving: boolean
  logLifeDaysValue: string
  currentSavedDays: number
  onChangeLogLifeDays: (value: string) => void
  notice: { kind: 'success' | 'error'; text: string } | null
}

export default function AdminPage(props: Props) {
  const {
    loading,
    saving,
    logLifeDaysValue,
    onChangeLogLifeDays,
    notice,
  } = props

  return (
    <div className="adminWrap">
      <div className="card adminCard">
        {notice?.kind === 'error' && <div className="notice error">{notice.text}</div>}

        {loading ? (
          <div className="muted">Загрузка настроек…</div>
        ) : (
          <div className="adminGrid">
            <label className="adminField adminFieldInline">
              <span className="adminFieldLabel">Жизнь лога</span>
              <input
                type="text"
                inputMode="numeric"
                pattern="[0-9]*"
                maxLength={3}
                className="searchInput adminNumberInput"
                value={logLifeDaysValue}
                onChange={(e) => onChangeLogLifeDays(e.target.value.replace(/\D+/g, '').slice(0, 3))}
                placeholder="10"
                disabled={saving}
                aria-label="Жизнь лога"
              />
            </label>
          </div>
        )}
      </div>
    </div>
  )
}

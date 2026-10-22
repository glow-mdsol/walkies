import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceArea,
  ReferenceLine,
  Cell,
} from 'recharts'
import {
  mmolToDisplay,
  mmolDeltaToDisplay,
  formatGlucose,
  formatGlucoseDelta,
} from '../utils/glucoseUnits'

const CATEGORY_COLORS = {
  walking: '#10b981',
  running: '#f59e0b',
  cycling: '#3b82f6',
  highly_active: '#ef4444',
  other: '#8b5cf6',
  sedentary: '#94a3b8',
  sleeping: '#cbd5e1',
}

const CATEGORY_LABELS = {
  walking: 'Walking / Hiking',
  running: 'Running',
  cycling: 'Cycling',
  highly_active: 'Highly Active',
  other: 'Other Movement',
  sedentary: 'Sedentary',
  sleeping: 'Sleeping',
}

function todayIso() {
  return new Date().toISOString().slice(0, 10)
}

function shiftDateIso(dateStr, deltaDays) {
  const dt = new Date(`${dateStr}T12:00:00Z`)
  dt.setUTCDate(dt.getUTCDate() + deltaDays)
  return dt.toISOString().slice(0, 10)
}

export default function DailyAggregateView({
  apiFetch,
  userSettings,
  onSelectWalk,
  shareToken = null,
  sharedInitialData = null,
}) {
  const [viewMode, setViewMode] = useState(
    sharedInitialData?.share_type === 'range' ? 'range' : 'daily'
  )
  const [selectedDate, setSelectedDate] = useState(
    sharedInitialData?.date || sharedInitialData?.end_date || todayIso()
  )
  const [rangeDays, setRangeDays] = useState(7)
  const [dailyData, setDailyData] = useState(
    sharedInitialData?.share_type === 'daily' ? sharedInitialData : null
  )
  const [rangeData, setRangeData] = useState(
    sharedInitialData?.share_type === 'range' ? sharedInitialData : null
  )
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  // Share modal state
  const [showShareModal, setShowShareModal] = useState(false)
  const [shareLinks, setShareLinks] = useState([])
  const [creatingShare, setCreatingShare] = useState(false)

  const effectiveSettings = useMemo(() => {
    return (
      dailyData?.user_settings ||
      rangeData?.user_settings ||
      userSettings ||
      dailyData?.regimen ||
      rangeData?.regimen ||
      {}
    )
  }, [dailyData, rangeData, userSettings])

  const unit = effectiveSettings?.glucose_unit || dailyData?.regimen?.glucose_unit || 'mmol/L'
  const userType = effectiveSettings?.user_type || dailyData?.regimen?.user_type || 'diabetic_closed_loop'
  const insulinType = effectiveSettings?.insulin_type || dailyData?.regimen?.insulin_type || 'ClosedLoopPump'
  const usesInsulin =
    insulinType !== 'None' &&
    !['diabetic_lifestyle', 'non_diabetic_cgm', 'non_diabetic'].includes(userType)
  const isClosedLoop =
    userType === 'diabetic_closed_loop' || insulinType === 'ClosedLoopPump'

  const targetLowDisplay = mmolToDisplay(
    dailyData?.regimen?.target_low_mmol ?? effectiveSettings?.target_low_mmol ?? 4.0,
    unit
  )
  const targetHighDisplay = mmolToDisplay(
    dailyData?.regimen?.target_high_mmol ?? effectiveSettings?.target_high_mmol ?? 8.0,
    unit
  )
  const hypoDisplay = mmolToDisplay(
    dailyData?.regimen?.hypo_threshold_mmol ?? effectiveSettings?.hypo_threshold_mmol ?? 3.9,
    unit
  )

  const loadDailyData = useCallback(async (dateStr) => {
    if (shareToken) return
    if (!apiFetch) return
    setLoading(true)
    setError(null)
    try {
      const res = await apiFetch(`/api/daily/${encodeURIComponent(dateStr)}/analysis`)
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Failed to load daily correlation analysis')
      setDailyData(payload)
    } catch (err) {
      setError(err.message || 'Could not load daily analysis')
    } finally {
      setLoading(false)
    }
  }, [apiFetch, shareToken])

  const loadRangeData = useCallback(async (endDateStr, daysCount) => {
    if (shareToken) return
    if (!apiFetch) return
    setLoading(true)
    setError(null)
    try {
      const startDateStr = shiftDateIso(endDateStr, -(daysCount - 1))
      const res = await apiFetch(
        `/api/daily/range?start_date=${encodeURIComponent(startDateStr)}&end_date=${encodeURIComponent(endDateStr)}`
      )
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Failed to load multi-day summary')
      setRangeData(payload)
    } catch (err) {
      setError(err.message || 'Could not load multi-day summary')
    } finally {
      setLoading(false)
    }
  }, [apiFetch, shareToken])

  useEffect(() => {
    if (shareToken) return
    if (viewMode === 'daily') {
      loadDailyData(selectedDate)
    } else {
      loadRangeData(selectedDate, rangeDays)
    }
  }, [viewMode, selectedDate, rangeDays, loadDailyData, loadRangeData, shareToken])

  const fetchShareLinks = useCallback(async () => {
    if (!apiFetch || shareToken) return
    try {
      const startD = viewMode === 'daily' ? selectedDate : shiftDateIso(selectedDate, -(rangeDays - 1))
      const endD = selectedDate
      const res = await apiFetch(
        `/api/daily/share-links?start_date=${encodeURIComponent(startD)}&end_date=${encodeURIComponent(endD)}&share_type=${viewMode}`
      )
      if (res.ok) {
        const data = await res.json()
        setShareLinks(data.links || [])
      }
    } catch {
      // ignore
    }
  }, [apiFetch, shareToken, viewMode, selectedDate, rangeDays])

  const handleCreateShareLink = async () => {
    if (!apiFetch) return
    setCreatingShare(true)
    try {
      const startD = viewMode === 'daily' ? selectedDate : shiftDateIso(selectedDate, -(rangeDays - 1))
      const endD = selectedDate
      const res = await apiFetch('/api/daily/share-links', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          share_type: viewMode,
          start_date: startD,
          end_date: endD,
          expires_in_days: 30,
        }),
      })
      if (res.ok) {
        await fetchShareLinks()
      }
    } finally {
      setCreatingShare(false)
    }
  }

  const handleRevokeShareLink = async (token) => {
    if (!apiFetch) return
    const res = await apiFetch(`/api/share-links/${encodeURIComponent(token)}`, {
      method: 'DELETE',
    })
    if (res.ok) {
      await fetchShareLinks()
    }
  }

  const chartEpochs = useMemo(() => {
    if (!dailyData?.epochs) return []
    return dailyData.epochs.map((ep) => ({
      ...ep,
      bg_display: mmolToDisplay(ep.avg_bg_mmol, unit),
      bg_delta_display: mmolDeltaToDisplay(ep.bg_delta_mmol, unit),
      fill: CATEGORY_COLORS[ep.activity_category] || '#94a3b8',
    }))
  }, [dailyData, unit])

  const rangeChartDays = useMemo(() => {
    if (!rangeData?.days) return []
    return rangeData.days.map((d) => ({
      ...d,
      avg_bg_display: mmolToDisplay(d.avg_bg_mmol, unit),
    }))
  }, [rangeData, unit])

  const summary = dailyData?.summary || {}
  const agg = rangeData?.aggregate || {}

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem' }}>
      {/* Top Toolbar */}
      <section
        className="card"
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: '1rem',
          padding: '1rem 1.25rem',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
          {!shareToken && (
            <>
              <button
                className={viewMode === 'daily' ? 'btn-primary' : 'btn-secondary'}
                onClick={() => setViewMode('daily')}
                style={{ padding: '0.45rem 0.9rem', fontSize: '0.85rem' }}
              >
                24-Hour Daily View
              </button>
              <button
                className={viewMode === 'range' ? 'btn-primary' : 'btn-secondary'}
                onClick={() => setViewMode('range')}
                style={{ padding: '0.45rem 0.9rem', fontSize: '0.85rem' }}
              >
                Multi-Day Trends
              </button>
            </>
          )}
          {shareToken && (
            <span className="metric-pill" style={{ background: 'var(--surface-tint)' }}>
              <span className="metric-pill-label">Shared Read-Only View</span>
              <span className="metric-pill-value">
                {viewMode === 'daily'
                  ? dailyData?.date
                  : `${rangeData?.start_date} → ${rangeData?.end_date}`}
              </span>
            </span>
          )}
        </div>

        {!shareToken && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
            {viewMode === 'range' && (
              <div style={{ display: 'flex', gap: '0.35rem', marginRight: '0.5rem' }}>
                {[7, 14, 30].map((d) => (
                  <button
                    key={d}
                    className={rangeDays === d ? 'btn-primary' : 'btn-secondary'}
                    onClick={() => setRangeDays(d)}
                    style={{ padding: '0.35rem 0.65rem', fontSize: '0.8rem' }}
                  >
                    {d}d
                  </button>
                ))}
              </div>
            )}
            <button
              className="btn-secondary"
              onClick={() => setSelectedDate((prev) => shiftDateIso(prev, -1))}
              style={{ padding: '0.4rem 0.7rem' }}
              title="Previous Day"
            >
              ◀
            </button>
            <input
              type="date"
              value={selectedDate}
              max={todayIso()}
              onChange={(e) => e.target.value && setSelectedDate(e.target.value)}
              style={{
                padding: '0.4rem 0.65rem',
                border: '1px solid var(--border)',
                borderRadius: 'var(--radius)',
                background: 'var(--surface)',
                color: 'var(--text)',
                fontSize: '0.9rem',
              }}
            />
            <button
              className="btn-secondary"
              onClick={() => setSelectedDate((prev) => shiftDateIso(prev, 1))}
              disabled={selectedDate >= todayIso()}
              style={{ padding: '0.4rem 0.7rem' }}
              title="Next Day"
            >
              ▶
            </button>
            <button
              className="btn-secondary"
              onClick={() => {
                setShowShareModal((s) => !s)
                if (!showShareModal) fetchShareLinks()
              }}
              style={{ padding: '0.4rem 0.85rem', fontSize: '0.85rem' }}
            >
              🔗 Share
            </button>
          </div>
        )}
      </section>

      {/* Public Share Link Management Panel */}
      {showShareModal && !shareToken && (
        <section className="card" style={{ padding: '1rem 1.25rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
            <h3 style={{ margin: 0, fontSize: '1rem' }}>
              Share {viewMode === 'daily' ? `Daily Summary (${selectedDate})` : `${rangeDays}-Day Aggregate Summary`}
            </h3>
            <button
              className="btn-primary"
              onClick={handleCreateShareLink}
              disabled={creatingShare}
              style={{ padding: '0.4rem 0.9rem', fontSize: '0.85rem' }}
            >
              {creatingShare ? 'Creating...' : '+ Create Share Link'}
            </button>
          </div>
          {shareLinks.length === 0 ? (
            <p className="hint" style={{ margin: 0 }}>
              No active public share links for this view. Create one to share a read-only link.
            </p>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {shareLinks.map((lnk) => {
                const shareUrl = `${window.location.origin}?share=${encodeURIComponent(lnk.token)}`
                return (
                  <div
                    key={lnk.token}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      gap: '0.75rem',
                      padding: '0.5rem 0.75rem',
                      background: 'var(--surface-tint)',
                      borderRadius: 'var(--radius)',
                      fontSize: '0.85rem',
                    }}
                  >
                    <code style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{shareUrl}</code>
                    <div style={{ display: 'flex', gap: '0.4rem' }}>
                      <button
                        className="btn-secondary"
                        onClick={() => navigator.clipboard?.writeText(shareUrl)}
                        style={{ padding: '0.25rem 0.6rem', fontSize: '0.8rem' }}
                      >
                        Copy
                      </button>
                      <button
                        className="btn-danger"
                        onClick={() => handleRevokeShareLink(lnk.token)}
                        style={{ padding: '0.25rem 0.6rem', fontSize: '0.8rem' }}
                      >
                        Revoke
                      </button>
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </section>
      )}

      {error && <div className="error-banner">{error}</div>}
      {loading && (
        <section className="card" style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
          Computing epochal movement & glycemic correlation...
        </section>
      )}

      {/* ==================== 24-HOUR DAILY VIEW ==================== */}
      {!loading && viewMode === 'daily' && dailyData && (
        <>
          {/* Summary Metric Cards */}
          <section className="card" style={{ padding: '1rem 1.25rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
              <h2 style={{ margin: 0, fontSize: '1.15rem' }}>
                Daily Activity & Glycemic Summary — {dailyData.date}
              </h2>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                Display Unit: <strong>{unit}</strong>
              </span>
            </div>
            <div className="metric-pills" style={{ display: 'flex', flexWrap: 'wrap', gap: '0.6rem' }}>
              <span className="metric-pill">
                <span className="metric-pill-label">Total Steps</span>
                <span className="metric-pill-value">{(summary.total_steps ?? 0).toLocaleString()}</span>
              </span>
              <span className="metric-pill">
                <span className="metric-pill-label">Active Mins</span>
                <span className="metric-pill-value">{summary.active_minutes ?? 0} m</span>
              </span>
              {summary.resting_hr != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Resting HR</span>
                  <span className="metric-pill-value">{Math.round(summary.resting_hr)} bpm</span>
                </span>
              )}
              {summary.tir_pct != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Time in Range</span>
                  <span className="metric-pill-value">{summary.tir_pct.toFixed(1)}%</span>
                </span>
              )}
              {summary.tbr_pct != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Time Below Range</span>
                  <span className="metric-pill-value">{summary.tbr_pct.toFixed(1)}%</span>
                </span>
              )}
              {summary.avg_bg_mmol != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Mean Glucose</span>
                  <span className="metric-pill-value">{formatGlucose(summary.avg_bg_mmol, unit)}</span>
                </span>
              )}
              {summary.bg_cv_pct != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Glycemic CV</span>
                  <span className="metric-pill-value">{summary.bg_cv_pct.toFixed(1)}%</span>
                </span>
              )}
              <span className="metric-pill">
                <span className="metric-pill-label">Sustained Hypos (≥15m)</span>
                <span className="metric-pill-value">{summary.hypos_count ?? 0}</span>
              </span>
              {usesInsulin && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Total Bolus</span>
                  <span className="metric-pill-value">{(summary.bolus_total_units ?? 0).toFixed(1)} U</span>
                </span>
              )}
              {summary.carb_total_grams > 0 && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Total Carbs</span>
                  <span className="metric-pill-value">{Math.round(summary.carb_total_grams)} g</span>
                </span>
              )}
              <span className="metric-pill">
                <span className="metric-pill-label">Sensor Coverage</span>
                <span className="metric-pill-value">{(summary.sensor_coverage_pct ?? 0).toFixed(1)}%</span>
              </span>
            </div>
          </section>

          {/* Sensor Gaps Banner (>15m) */}
          {dailyData.sensor_gaps?.length > 0 && (
            <section
              className="card"
              style={{
                padding: '0.8rem 1.15rem',
                borderLeft: '4px solid #64748b',
                background: 'var(--surface-tint)',
              }}
            >
              <strong style={{ fontSize: '0.9rem' }}>
                CGM Sensor Gaps Detected ({dailyData.sensor_gaps.length} gap{dailyData.sensor_gaps.length > 1 ? 's' : ''} &gt;15 min):
              </strong>
              <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginLeft: '0.5rem' }}>
                {dailyData.sensor_gaps
                  .map((g) => `${g.start_utc.slice(11, 16)}–${g.end_utc.slice(11, 16)} UTC (${g.duration_mins}m)`)
                  .join(', ')}{' '}
                — excluded from glucose interpolation.
              </span>
            </section>
          )}

          {/* 15-Minute Epoch & Glucose Correlation Chart */}
          <section className="card" style={{ padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '1rem' }}>
              15-Minute Activity Epochs vs. Continuous Glucose ({unit}) {usesInsulin ? '& Active Insulin (IOB)' : ''}
            </h3>
            {chartEpochs.length === 0 && (!dailyData.cgm_series || dailyData.cgm_series.length === 0) ? (
              <p className="empty" style={{ margin: '1.5rem 0' }}>
                No 15-minute Garmin epochs or CGM telemetry recorded for {dailyData.date}. Sync Garmin or import CGM data to populate this timeline.
              </p>
            ) : (
              <div style={{ width: '100%', height: 340 }}>
                <ResponsiveContainer>
                  <ComposedChart data={chartEpochs} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" opacity={0.25} />
                    <XAxis dataKey="local_time" tick={{ fontSize: 11 }} />
                    <YAxis
                      yAxisId="bg"
                      orientation="left"
                      domain={['auto', 'auto']}
                      tick={{ fontSize: 11 }}
                      label={{ value: `BG (${unit})`, angle: -90, position: 'insideLeft', fontSize: 11 }}
                    />
                    <YAxis
                      yAxisId="steps"
                      orientation="right"
                      tick={{ fontSize: 11 }}
                      label={{ value: 'Steps / 15m', angle: 90, position: 'insideRight', fontSize: 11 }}
                    />
                    <Tooltip
                      formatter={(val, name, props) => {
                        if (name === 'Avg BG') return [`${val} ${unit}`, name]
                        if (name === 'Steps (15m)') {
                          const cat = CATEGORY_LABELS[props?.payload?.activity_category] || props?.payload?.activity_category
                          return [`${val} steps (${cat})`, name]
                        }
                        if (name === 'IOB') return [`${val} U`, name]
                        return [val, name]
                      }}
                    />
                    {targetLowDisplay != null && targetHighDisplay != null && (
                      <ReferenceArea
                        yAxisId="bg"
                        y1={targetLowDisplay}
                        y2={targetHighDisplay}
                        fill="#10b981"
                        fillOpacity={0.08}
                      />
                    )}
                    {hypoDisplay != null && (
                      <ReferenceLine
                        yAxisId="bg"
                        y={hypoDisplay}
                        stroke="#ef4444"
                        strokeDasharray="4 4"
                      />
                    )}
                    <Bar yAxisId="steps" dataKey="steps" name="Steps (15m)" opacity={0.45}>
                      {chartEpochs.map((entry, idx) => (
                        <Cell key={`cell-${idx}`} fill={entry.fill} />
                      ))}
                    </Bar>
                    <Line
                      yAxisId="bg"
                      type="monotone"
                      dataKey="bg_display"
                      name="Avg BG"
                      stroke="#2563eb"
                      strokeWidth={2.5}
                      dot={{ r: 2 }}
                      connectNulls={false}
                    />
                    {usesInsulin && (
                      <Line
                        yAxisId="bg"
                        type="monotone"
                        dataKey="iob_units"
                        name="IOB"
                        stroke="#f59e0b"
                        strokeWidth={1.5}
                        strokeDasharray="3 3"
                        dot={false}
                      />
                    )}
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            )}
          </section>

          {/* Activity Category Glycemic Breakdown Table */}
          <section className="card" style={{ padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '1rem' }}>
              Glycemic Response by Activity Category (with 1–2h Post-Activity Lag)
            </h3>
            {dailyData.activity_category_breakdown?.length > 0 ? (
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.88rem' }}>
                  <thead>
                    <tr style={{ borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                      <th style={{ padding: '0.5rem' }}>Category</th>
                      <th style={{ padding: '0.5rem' }}>Duration</th>
                      <th style={{ padding: '0.5rem' }}>Steps</th>
                      <th style={{ padding: '0.5rem' }}>Mean BG ({unit})</th>
                      <th style={{ padding: '0.5rem' }}>1h Post-Activity Δ</th>
                      <th style={{ padding: '0.5rem' }}>Time in Range</th>
                      <th style={{ padding: '0.5rem' }}>Hypos (During / 2h Post)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dailyData.activity_category_breakdown.map((row) => (
                      <tr key={row.category} style={{ borderBottom: '1px solid var(--border-muted)' }}>
                        <td style={{ padding: '0.5rem', fontWeight: 600 }}>
                          <span
                            style={{
                              display: 'inline-block',
                              width: 10,
                              height: 10,
                              borderRadius: '50%',
                              background: CATEGORY_COLORS[row.category] || '#94a3b8',
                              marginRight: '0.45rem',
                            }}
                          />
                          {CATEGORY_LABELS[row.category] || row.category}
                        </td>
                        <td style={{ padding: '0.5rem' }}>{row.duration_mins} min</td>
                        <td style={{ padding: '0.5rem' }}>{(row.steps ?? 0).toLocaleString()}</td>
                        <td style={{ padding: '0.5rem' }}>
                          {row.avg_bg_mmol != null ? formatGlucose(row.avg_bg_mmol, unit) : '—'}
                        </td>
                        <td style={{ padding: '0.5rem' }}>
                          {row.mean_bg_change_1h_post_mmol != null
                            ? formatGlucoseDelta(row.mean_bg_change_1h_post_mmol, unit)
                            : '—'}
                        </td>
                        <td style={{ padding: '0.5rem' }}>
                          {row.tir_pct != null ? `${row.tir_pct.toFixed(1)}%` : '—'}
                        </td>
                        <td style={{ padding: '0.5rem' }}>{row.hypo_events_during_or_2h_post ?? 0}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="hint" style={{ margin: 0 }}>
                No categorized movement epochs recorded for this date.
              </p>
            )}
          </section>

          {/* Closed-Loop Automation Events (Hidden for MDI / Non-Insulin Regimens) */}
          {isClosedLoop && dailyData.therapy_events?.pump_states?.length > 0 && (
            <section className="card" style={{ padding: '1rem 1.25rem' }}>
              <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '1rem' }}>
                Automated Pump / SmartGuard / Control-IQ Events
              </h3>
              <div className="metric-pills">
                {dailyData.therapy_events.pump_states.map((ps, idx) => (
                  <span key={idx} className="metric-pill">
                    <span className="metric-pill-label">{ps.timestamp_utc.slice(11, 16)} UTC</span>
                    <span className="metric-pill-value">{ps.state}</span>
                  </span>
                ))}
              </div>
            </section>
          )}

          {/* Linked Recorded GPS Walks */}
          {dailyData.linked_walks?.length > 0 && (
            <section className="card" style={{ padding: '1rem 1.25rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                <h3 style={{ margin: 0, fontSize: '1rem' }}>
                  Linked GPS Walks ({dailyData.linked_walks.length})
                </h3>
                <span className="hint" style={{ fontSize: '0.8rem' }}>
                  ✓ Steps deduplicated with 15-min Garmin wellness epochs
                </span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {dailyData.linked_walks.map((w) => (
                  <div
                    key={w.id}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      padding: '0.6rem 0.85rem',
                      background: 'var(--surface-tint)',
                      borderRadius: 'var(--radius)',
                    }}
                  >
                    <div>
                      <strong>{w.name || 'Recorded Walk'}</strong>
                      {w.distance_km != null && (
                        <span style={{ marginLeft: '0.75rem', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                          {Number(w.distance_km).toFixed(2)} km
                        </span>
                      )}
                      {w.tir_pct != null && (
                        <span style={{ marginLeft: '0.75rem', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                          TIR: {Number(w.tir_pct).toFixed(0)}%
                        </span>
                      )}
                    </div>
                    {onSelectWalk && !shareToken && (
                      <button
                        className="btn-secondary"
                        onClick={() => onSelectWalk(w.id)}
                        style={{ padding: '0.35rem 0.75rem', fontSize: '0.82rem' }}
                      >
                        Inspect GPS Walk →
                      </button>
                    )}
                  </div>
                ))}
              </div>
            </section>
          )}
        </>
      )}

      {/* ==================== MULTI-DAY RANGE VIEW ==================== */}
      {!loading && viewMode === 'range' && rangeData && (
        <>
          <section className="card" style={{ padding: '1rem 1.25rem' }}>
            <h2 style={{ margin: '0 0 0.75rem 0', fontSize: '1.15rem' }}>
              {rangeData.days_count}-Day Aggregate Correlation ({rangeData.start_date} → {rangeData.end_date})
            </h2>
            <div className="metric-pills" style={{ display: 'flex', flexWrap: 'wrap', gap: '0.6rem' }}>
              <span className="metric-pill">
                <span className="metric-pill-label">Total Steps</span>
                <span className="metric-pill-value">{(agg.total_steps ?? 0).toLocaleString()}</span>
              </span>
              <span className="metric-pill">
                <span className="metric-pill-label">Avg Daily Steps</span>
                <span className="metric-pill-value">{(agg.avg_daily_steps ?? 0).toLocaleString()}</span>
              </span>
              {agg.tir_pct != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Overall TIR</span>
                  <span className="metric-pill-value">{agg.tir_pct.toFixed(1)}%</span>
                </span>
              )}
              {agg.active_days_tir_pct != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Active Days TIR</span>
                  <span className="metric-pill-value">{agg.active_days_tir_pct.toFixed(1)}%</span>
                </span>
              )}
              {agg.sedentary_days_tir_pct != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Sedentary Days TIR</span>
                  <span className="metric-pill-value">{agg.sedentary_days_tir_pct.toFixed(1)}%</span>
                </span>
              )}
              {agg.avg_bg_mmol != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">Mean Glucose</span>
                  <span className="metric-pill-value">{formatGlucose(agg.avg_bg_mmol, unit)}</span>
                </span>
              )}
              {agg.bg_drop_per_1000_steps_mmol != null && (
                <span className="metric-pill">
                  <span className="metric-pill-label">BG Δ / 1,000 Active Steps</span>
                  <span className="metric-pill-value">
                    {formatGlucoseDelta(agg.bg_drop_per_1000_steps_mmol, unit)}
                  </span>
                </span>
              )}
            </div>
          </section>

          <section className="card" style={{ padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '1rem' }}>
              Daily Steps vs. Time in Range (%)
            </h3>
            <div style={{ width: '100%', height: 280 }}>
              <ResponsiveContainer>
                <ComposedChart data={rangeChartDays} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" opacity={0.25} />
                  <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="tir" orientation="left" domain={[0, 100]} tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="steps" orientation="right" tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar yAxisId="steps" dataKey="total_steps" name="Daily Steps" fill="#10b981" opacity={0.45} />
                  <Line yAxisId="tir" type="monotone" dataKey="tir_pct" name="Time in Range (%)" stroke="#2563eb" strokeWidth={2.5} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </section>

          <section className="card" style={{ padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '1rem' }}>Daily Breakdown</h3>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.88rem' }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                    <th style={{ padding: '0.5rem' }}>Date</th>
                    <th style={{ padding: '0.5rem' }}>Steps</th>
                    <th style={{ padding: '0.5rem' }}>Active Mins</th>
                    <th style={{ padding: '0.5rem' }}>Mean BG ({unit})</th>
                    <th style={{ padding: '0.5rem' }}>TIR (%)</th>
                    <th style={{ padding: '0.5rem' }}>Hypos</th>
                    <th style={{ padding: '0.5rem' }}>Walks</th>
                  </tr>
                </thead>
                <tbody>
                  {rangeData.days?.map((d) => (
                    <tr
                      key={d.date}
                      style={{ borderBottom: '1px solid var(--border-muted)', cursor: shareToken ? 'default' : 'pointer' }}
                      onClick={() => {
                        if (!shareToken) {
                          setSelectedDate(d.date)
                          setViewMode('daily')
                        }
                      }}
                    >
                      <td style={{ padding: '0.5rem', fontWeight: 600 }}>{d.date}</td>
                      <td style={{ padding: '0.5rem' }}>{(d.total_steps ?? 0).toLocaleString()}</td>
                      <td style={{ padding: '0.5rem' }}>{d.active_minutes ?? 0} m</td>
                      <td style={{ padding: '0.5rem' }}>
                        {d.avg_bg_mmol != null ? formatGlucose(d.avg_bg_mmol, unit) : '—'}
                      </td>
                      <td style={{ padding: '0.5rem' }}>
                        {d.tir_pct != null ? `${d.tir_pct.toFixed(1)}%` : '—'}
                      </td>
                      <td style={{ padding: '0.5rem' }}>{d.hypos_count ?? 0}</td>
                      <td style={{ padding: '0.5rem' }}>{d.linked_walks_count ?? 0}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </div>
  )
}

import { useState } from 'react'

export const INSULIN_PROFILE_KEY = 'walkies.insulinProfile.v1'

export const INSULIN_PROFILE_DEFAULTS = {
  name: 'NovoRapid',
  onsetMins: 15,
  earlyActionMins: 30,
  peakMinHours: 1.0,
  peakMaxHours: 2.0,
  durationMinHours: 3.0,
  durationMaxHours: 5.0,
}

export function loadInsulinProfile() {
  if (typeof window === 'undefined') return { ...INSULIN_PROFILE_DEFAULTS }
  try {
    const raw = window.localStorage.getItem(INSULIN_PROFILE_KEY)
    if (!raw) return { ...INSULIN_PROFILE_DEFAULTS }
    const p = JSON.parse(raw)
    return {
      name: typeof p.name === 'string' && p.name.trim() ? p.name.trim() : INSULIN_PROFILE_DEFAULTS.name,
      onsetMins: Number(p.onsetMins) > 0 ? Number(p.onsetMins) : INSULIN_PROFILE_DEFAULTS.onsetMins,
      earlyActionMins: Number(p.earlyActionMins) > 0 ? Number(p.earlyActionMins) : INSULIN_PROFILE_DEFAULTS.earlyActionMins,
      peakMinHours: Number(p.peakMinHours) > 0 ? Number(p.peakMinHours) : INSULIN_PROFILE_DEFAULTS.peakMinHours,
      peakMaxHours: Number(p.peakMaxHours) > 0 ? Number(p.peakMaxHours) : INSULIN_PROFILE_DEFAULTS.peakMaxHours,
      durationMinHours: Number(p.durationMinHours) > 0 ? Number(p.durationMinHours) : INSULIN_PROFILE_DEFAULTS.durationMinHours,
      durationMaxHours: Number(p.durationMaxHours) > 0 ? Number(p.durationMaxHours) : INSULIN_PROFILE_DEFAULTS.durationMaxHours,
    }
  } catch {
    return { ...INSULIN_PROFILE_DEFAULTS }
  }
}

/**
 * Build an IOB (insulin-on-board) fraction function from an insulin profile.
 * Returns a function iob(t_effort_seconds) → fraction [0, 1] of bolus still active.
 *
 * Activity curve: piece-wise linear
 *   0 → onset:         no activity
 *   onset → earlyEnd:  ramp 0 → 0.5   (early onset phase)
 *   earlyEnd → peak:   ramp 0.5 → 1.0 (main action phase)
 *   peak → duration:   ramp 1.0 → 0   (tail / decline)
 */
export function buildIobFn(profile) {
  const onset_s = profile.onsetMins * 60
  const earlyEnd_s = onset_s + profile.earlyActionMins * 60
  const peak_s = ((profile.peakMinHours + profile.peakMaxHours) / 2) * 3600
  const duration_s = ((profile.durationMinHours + profile.durationMaxHours) / 2) * 3600

  function activity(t) {
    if (t <= onset_s || t >= duration_s) return 0
    const earlySpan = earlyEnd_s - onset_s
    const riseSpan = peak_s - earlyEnd_s
    const fallSpan = duration_s - peak_s
    if (earlySpan <= 0 || fallSpan <= 0) {
      // Fallback: simple triangle peak
      if (peak_s <= onset_s) return 0
      if (t <= peak_s) return (t - onset_s) / (peak_s - onset_s)
      return (duration_s - t) / (duration_s - peak_s)
    }
    if (t <= earlyEnd_s) return 0.5 * (t - onset_s) / earlySpan
    if (riseSpan > 0 && t <= peak_s) return 0.5 + 0.5 * (t - earlyEnd_s) / riseSpan
    return (duration_s - t) / fallSpan
  }

  // Precompute suffix integral (trapezoid rule) for IOB fraction
  const N = 600
  const dt = duration_s / N
  const acts = Array.from({ length: N + 1 }, (_, i) => activity(i * dt))
  const suffix = new Float64Array(N + 2)
  for (let i = N - 1; i >= 0; i--) {
    suffix[i] = suffix[i + 1] + (acts[i] + acts[i + 1]) * 0.5 * dt
  }
  const totalArea = suffix[0]
  if (totalArea < 1e-9) return () => 0

  return function iob(t_s) {
    if (t_s <= 0) return 1
    if (t_s >= duration_s) return 0
    const idxF = t_s / dt
    const idx = Math.min(N - 1, Math.floor(idxF))
    const frac = idxF - idx
    const partialArea = frac * (acts[idx] + acts[idx + 1]) * 0.5 * dt
    return (suffix[idx] - partialArea) / totalArea
  }
}

function num(v, dec) {
  return Number.isFinite(v) ? v.toFixed(dec) : '–'
}

function FieldRow({ label, hint, children }) {
  return (
    <div className="setup-field-row">
      <div className="setup-field-label">
        <span>{label}</span>
        {hint && <span className="setup-field-hint">{hint}</span>}
      </div>
      <div className="setup-field-inputs">{children}</div>
    </div>
  )
}

export default function InsulinSetup({ onBack }) {
  const [form, setForm] = useState(() => loadInsulinProfile())
  const [saved, setSaved] = useState(false)

  const update = (key, rawVal) => {
    const val = key === 'name' ? rawVal : Number(rawVal)
    setSaved(false)
    setForm((prev) => {
      const next = { ...prev, [key]: val }
      // Keep range relationships sane in real-time
      if (key === 'peakMinHours' && next.peakMinHours >= next.peakMaxHours) {
        next.peakMaxHours = Math.round((next.peakMinHours + 0.5) * 10) / 10
      }
      if (key === 'peakMaxHours' && next.peakMaxHours <= next.peakMinHours) {
        next.peakMinHours = Math.round((next.peakMaxHours - 0.5) * 10) / 10
      }
      if (key === 'durationMinHours' && next.durationMinHours >= next.durationMaxHours) {
        next.durationMaxHours = Math.round((next.durationMinHours + 0.5) * 10) / 10
      }
      if (key === 'durationMaxHours' && next.durationMaxHours <= next.durationMinHours) {
        next.durationMinHours = Math.round((next.durationMaxHours - 0.5) * 10) / 10
      }
      return next
    })
  }

  const handleSave = () => {
    window.localStorage.setItem(INSULIN_PROFILE_KEY, JSON.stringify(form))
    setSaved(true)
  }

  const handleReset = () => {
    setForm({ ...INSULIN_PROFILE_DEFAULTS })
    setSaved(false)
  }

  // Preview: duration mid
  const durationMid = (form.durationMinHours + form.durationMaxHours) / 2
  const peakMid = (form.peakMinHours + form.peakMaxHours) / 2
  const onsetEnd = form.onsetMins / 60 + form.earlyActionMins / 60

  return (
    <div className="insulin-setup">
      <div className="insulin-setup-header">
        <button className="btn-back" onClick={onBack}>← Back</button>
        <h2>Insulin Profile</h2>
      </div>

      <p className="insulin-setup-description">
        Configure the pharmacokinetic profile of your rapid-acting insulin.
        This is used to model active insulin on board (IOB) for bolus decay calculations.
        You only need to set this once.
      </p>

      <div className="card insulin-setup-card">
        <FieldRow label="Insulin name" hint="e.g. NovoRapid, Humalog, Fiasp">
          <input
            type="text"
            className="setup-text-input"
            value={form.name}
            maxLength={40}
            onChange={(e) => update('name', e.target.value)}
          />
        </FieldRow>

        <FieldRow label="Onset time" hint="Minutes before insulin begins acting">
          <div className="setup-slider-row">
            <input
              type="range"
              min={1}
              max={60}
              step={1}
              value={form.onsetMins}
              onChange={(e) => update('onsetMins', e.target.value)}
            />
            <span className="setup-value">{form.onsetMins} min</span>
          </div>
        </FieldRow>

        <FieldRow label="Early action time" hint="Minutes from onset to main action phase">
          <div className="setup-slider-row">
            <input
              type="range"
              min={10}
              max={90}
              step={5}
              value={form.earlyActionMins}
              onChange={(e) => update('earlyActionMins', e.target.value)}
            />
            <span className="setup-value">{form.earlyActionMins} min</span>
          </div>
        </FieldRow>

        <FieldRow
          label="Peak action"
          hint="Range of hours when insulin effect is at maximum"
        >
          <div className="setup-range-row">
            <label className="setup-range-label">From</label>
            <input
              type="range"
              min={0.5}
              max={4}
              step={0.25}
              value={form.peakMinHours}
              onChange={(e) => update('peakMinHours', e.target.value)}
            />
            <span className="setup-value">{num(form.peakMinHours, 2)} h</span>
            <label className="setup-range-label">to</label>
            <input
              type="range"
              min={0.75}
              max={5}
              step={0.25}
              value={form.peakMaxHours}
              onChange={(e) => update('peakMaxHours', e.target.value)}
            />
            <span className="setup-value">{num(form.peakMaxHours, 2)} h</span>
          </div>
        </FieldRow>

        <FieldRow
          label="Total duration"
          hint="Range of hours for full insulin action"
        >
          <div className="setup-range-row">
            <label className="setup-range-label">From</label>
            <input
              type="range"
              min={1}
              max={8}
              step={0.5}
              value={form.durationMinHours}
              onChange={(e) => update('durationMinHours', e.target.value)}
            />
            <span className="setup-value">{num(form.durationMinHours, 1)} h</span>
            <label className="setup-range-label">to</label>
            <input
              type="range"
              min={1.5}
              max={10}
              step={0.5}
              value={form.durationMaxHours}
              onChange={(e) => update('durationMaxHours', e.target.value)}
            />
            <span className="setup-value">{num(form.durationMaxHours, 1)} h</span>
          </div>
        </FieldRow>

        <div className="setup-preview">
          <span className="setup-preview-label">Profile summary:</span>
          <span>
            {form.name} · onset {form.onsetMins} min · early action {form.earlyActionMins} min ·
            peak {num(peakMid, 2)} h · duration {num(durationMid, 1)} h ·
            action starts at {num(onsetEnd, 2)} h
          </span>
        </div>

        <div className="setup-actions">
          <button className="btn-primary" onClick={handleSave}>Save profile</button>
          <button className="btn-ghost" onClick={handleReset}>Reset to defaults</button>
          {saved && <span className="msg-ok">Profile saved.</span>}
        </div>
      </div>
    </div>
  )
}

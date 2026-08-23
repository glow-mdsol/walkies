import { useState, useEffect } from 'react'

export const DEFAULT_USER_SETTINGS = {
  user_type: 'non_diabetic',
  insulin_type: 'None',
  pump_brand_model: '',
  basal_rates: { default: 0.8, exercise: 0.5 },
  carb_ratios: { default: 5 },
  insulin_sensitivity_factor: 2.0,
  correction_threshold: 8.0,
  exercise_correction_factor: 1.5,
  iob_calculation_method: 'exponential',
  insulin_profile: {
    name: 'NovoRapid',
    onsetMins: 15,
    earlyActionMins: 30,
    peakMinHours: 1.0,
    peakMaxHours: 2.0,
    durationMinHours: 3.0,
    durationMaxHours: 5.0,
  }
}

export const INSULIN_TEMPLATES = [
  {
    name: 'NovoRapid (Rapid)',
    profile: {
      name: 'NovoRapid',
      onsetMins: 15,
      earlyActionMins: 30,
      peakMinHours: 1.0,
      peakMaxHours: 2.0,
      durationMinHours: 3.0,
      durationMaxHours: 5.0,
    }
  },
  {
    name: 'Humalog (Rapid)',
    profile: {
      name: 'Humalog',
      onsetMins: 15,
      earlyActionMins: 30,
      peakMinHours: 1.0,
      peakMaxHours: 2.0,
      durationMinHours: 3.0,
      durationMaxHours: 5.0,
    }
  },
  {
    name: 'Fiasp (Ultra-Rapid)',
    profile: {
      name: 'Fiasp',
      onsetMins: 5,
      earlyActionMins: 20,
      peakMinHours: 0.5,
      peakMaxHours: 1.5,
      durationMinHours: 3.0,
      durationMaxHours: 5.0,
    }
  },
  {
    name: 'Apidra (Rapid)',
    profile: {
      name: 'Apidra',
      onsetMins: 15,
      earlyActionMins: 30,
      peakMinHours: 1.0,
      peakMaxHours: 2.0,
      durationMinHours: 3.0,
      durationMaxHours: 5.0,
    }
  },
  {
    name: 'Actrapid (Short-Acting / Regular)',
    profile: {
      name: 'Actrapid',
      onsetMins: 30,
      earlyActionMins: 60,
      peakMinHours: 2.0,
      peakMaxHours: 4.0,
      durationMinHours: 5.0,
      durationMaxHours: 8.0,
    }
  },
  {
    name: 'Humulin S (Short-Acting / Regular)',
    profile: {
      name: 'Humulin S',
      onsetMins: 30,
      earlyActionMins: 60,
      peakMinHours: 2.0,
      peakMaxHours: 4.0,
      durationMinHours: 5.0,
      durationMaxHours: 8.0,
    }
  },
  {
    name: 'Custom (Enter manually)',
    profile: {
      name: 'Custom',
      onsetMins: 15,
      earlyActionMins: 30,
      peakMinHours: 1.0,
      peakMaxHours: 2.0,
      durationMinHours: 3.0,
      durationMaxHours: 5.0,
    }
  }
]

const getMatchingTemplateName = (profile) => {
  if (!profile) return 'Custom'
  const match = INSULIN_TEMPLATES.find(t => 
    t.profile.name === profile.name &&
    t.profile.onsetMins === profile.onsetMins &&
    t.profile.earlyActionMins === profile.earlyActionMins &&
    t.profile.peakMinHours === profile.peakMinHours &&
    t.profile.peakMaxHours === profile.peakMaxHours &&
    t.profile.durationMinHours === profile.durationMinHours &&
    t.profile.durationMaxHours === profile.durationMaxHours
  )
  return match ? match.profile.name : 'Custom'
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

export default function UserSettings({ userSettings, onSave, isRegistration, onBack, onDeleteAccount }) {
  const [form, setForm] = useState(() => {
    if (userSettings && userSettings.updated_at) {
      let userType = userSettings.user_type
      if (!userType) {
        if (userSettings.insulin_type === 'Pump') userType = 'diabetic_pump'
        else if (userSettings.insulin_type === 'MDI') userType = 'diabetic_mdi'
        else userType = 'non_diabetic'
      }
      // Pre-populate with backend data, filling missing defaults
      return {
        user_type: userType,
        insulin_type: userSettings.insulin_type || 'None',
        pump_brand_model: userSettings.pump_brand_model || '',
        basal_rates: userSettings.basal_rates || { ...DEFAULT_USER_SETTINGS.basal_rates },
        carb_ratios: userSettings.carb_ratios || { ...DEFAULT_USER_SETTINGS.carb_ratios },
        insulin_sensitivity_factor: userSettings.insulin_sensitivity_factor ?? DEFAULT_USER_SETTINGS.insulin_sensitivity_factor,
        correction_threshold: userSettings.correction_threshold ?? DEFAULT_USER_SETTINGS.correction_threshold,
        exercise_correction_factor: userSettings.exercise_correction_factor ?? DEFAULT_USER_SETTINGS.exercise_correction_factor,
        iob_calculation_method: userSettings.iob_calculation_method || DEFAULT_USER_SETTINGS.iob_calculation_method,
        insulin_profile: userSettings.insulin_profile || { ...DEFAULT_USER_SETTINGS.insulin_profile }
      }
    }
    return { ...DEFAULT_USER_SETTINGS }
  })
  
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [saved, setSaved] = useState(false)

  const [deletingAccount, setDeletingAccount] = useState(false)
  const [deleteError, setDeleteError] = useState(null)

  const handleDeleteAccount = async () => {
    if (!window.confirm("Are you sure you want to permanently delete your account and all walk telemetry data? This action is irreversible.")) {
      return
    }
    setDeletingAccount(true)
    setDeleteError(null)
    try {
      await onDeleteAccount()
    } catch (err) {
      setDeleteError(err.message || 'Failed to delete account')
      setDeletingAccount(false)
    }
  }

  const handleTemplateChange = (e) => {
    const name = e.target.value
    const template = INSULIN_TEMPLATES.find(t => t.profile.name === name)
    if (template) {
      updateRoot('insulin_profile', { ...template.profile })
    }
  }

  const updateRoot = (key, val) => {
    setSaved(false)
    setForm(prev => ({ ...prev, [key]: val }))
  }

  const updateNested = (parentKey, childKey, rawVal) => {
    const val = childKey === 'name' ? rawVal : Number(rawVal)
    setSaved(false)
    setForm(prev => {
      const parentObj = { ...(prev[parentKey] || {}) }
      parentObj[childKey] = val
      
      // Keep range relationships sane in real-time for insulin profile
      if (parentKey === 'insulin_profile') {
        if (childKey === 'peakMinHours' && parentObj.peakMinHours >= parentObj.peakMaxHours) {
          parentObj.peakMaxHours = Math.round((parentObj.peakMinHours + 0.5) * 10) / 10
        }
        if (childKey === 'peakMaxHours' && parentObj.peakMaxHours <= parentObj.peakMinHours) {
          parentObj.peakMinHours = Math.round((parentObj.peakMaxHours - 0.5) * 10) / 10
        }
        if (childKey === 'durationMinHours' && parentObj.durationMinHours >= parentObj.durationMaxHours) {
          parentObj.durationMaxHours = Math.round((parentObj.durationMinHours + 0.5) * 10) / 10
        }
        if (childKey === 'durationMaxHours' && parentObj.durationMaxHours <= parentObj.durationMinHours) {
          parentObj.durationMinHours = Math.round((parentObj.durationMaxHours - 0.5) * 10) / 10
        }
      }
      return { ...prev, [parentKey]: parentObj }
    })
  }

  const handleSave = async () => {
    setSaving(true)
    setError(null)
    setSaved(false)
    try {
      await onSave(form)
      setSaved(true)
    } catch (err) {
      setError(err.message || 'Failed to save settings')
    } finally {
      setSaving(false)
    }
  }

  // Pharmacokinetic preview metrics
  const profile = form.insulin_profile || DEFAULT_USER_SETTINGS.insulin_profile
  const peakMid = ((profile.peakMinHours || 0) + (profile.peakMaxHours || 0)) / 2
  const durationMid = ((profile.durationMinHours || 0) + (profile.durationMaxHours || 0)) / 2
  const onsetEnd = (profile.onsetMins || 0) / 60 + (profile.earlyActionMins || 0) / 60

  return (
    <div className="insulin-setup">
      <div className="insulin-setup-header">
        {onBack && !isRegistration && (
          <button className="btn-back" onClick={onBack} style={{ marginRight: '0.5rem' }}>← Back</button>
        )}
        <h2>{isRegistration ? 'Complete Your Profile Registration' : 'UserSettings'}</h2>
      </div>

      <p className="insulin-setup-description">
        {isRegistration
          ? 'Before using Trek GM, please complete your user profile. This enables personalization of BGM metrics and safety analysis.'
          : 'Update your clinical treatment configurations. These settings drive insulin stress, active IOB decay, and target range calculations.'}
      </p>

      {error && <div className="error-banner">{error}</div>}

      <div className="card insulin-setup-card">
        {/* User Type / Diabetic Status */}
        <FieldRow label="User Profile Type" hint="Select your biometric and therapy profile">
          <select
            className="setup-text-input"
            value={form.user_type}
            onChange={(e) => {
              const ut = e.target.value
              let it = 'None'
              if (ut === 'diabetic_pump') it = 'Pump'
              else if (ut === 'diabetic_mdi') it = 'MDI'
              setSaved(false)
              setForm(prev => ({ ...prev, user_type: ut, insulin_type: it }))
            }}
            style={{ maxWidth: '320px', padding: '0.45rem' }}
          >
            <option value="diabetic_pump">Diabetic with CGM (Pump Therapy)</option>
            <option value="diabetic_mdi">Diabetic with CGM (Basal/Bolus)</option>
            <option value="non_diabetic_cgm">Non-diabetic with CGM</option>
            <option value="non_diabetic">Non-diabetic without CGM</option>
          </select>
        </FieldRow>

        {form.insulin_type === 'None' && (
          <div className="setup-preview" style={{ background: '#f0fdf4', borderColor: '#bbf7d0', color: '#166534' }}>
            <span className="setup-preview-label">Non-Diabetic Configuration:</span>
            <span>No insulin or pump-specific parameters are required. Click "Save Profile" to continue.</span>
          </div>
        )}

        {form.insulin_type !== 'None' && (
          <>
            {/* Pump Brand & Model (Pump-only) */}
            {form.insulin_type === 'Pump' && (
              <>
                <FieldRow label="Pump Brand & Model" hint="e.g. Medtronic 780G, Tandem t:slim X2">
                  <input
                    type="text"
                    className="setup-text-input"
                    value={form.pump_brand_model}
                    onChange={(e) => updateRoot('pump_brand_model', e.target.value)}
                    placeholder="e.g. Medtronic 780G"
                  />
                </FieldRow>

                <FieldRow label="Default Basal Rate" hint="Default basal rate in Units/hour">
                  <div className="setup-slider-row">
                    <input
                      type="number"
                      className="setup-text-input"
                      style={{ width: '100px' }}
                      step="0.05"
                      min="0"
                      value={form.basal_rates.default}
                      onChange={(e) => updateNested('basal_rates', 'default', e.target.value)}
                    />
                    <span className="setup-value">U/h</span>
                  </div>
                </FieldRow>

                <FieldRow label="Exercise Basal Rate" hint="Exercise-adjusted basal rate in Units/hour">
                  <div className="setup-slider-row">
                    <input
                      type="number"
                      className="setup-text-input"
                      style={{ width: '100px' }}
                      step="0.05"
                      min="0"
                      value={form.basal_rates.exercise}
                      onChange={(e) => updateNested('basal_rates', 'exercise', e.target.value)}
                    />
                    <span className="setup-value">U/h</span>
                  </div>
                </FieldRow>
              </>
            )}

            {/* General Diabetic Settings */}
            <FieldRow label="Carb Ratio" hint="Grams of carbs covered by 1 Unit of insulin">
              <div className="setup-slider-row">
                <input
                  type="number"
                  className="setup-text-input"
                  style={{ width: '100px' }}
                  step="0.5"
                  min="0.1"
                  value={form.carb_ratios.default}
                  onChange={(e) => updateNested('carb_ratios', 'default', e.target.value)}
                />
                <span className="setup-value">g/U</span>
              </div>
            </FieldRow>

            <FieldRow label="Insulin Sensitivity Factor (ISF)" hint="BG drop expected from 1 Unit of insulin">
              <div className="setup-slider-row">
                <input
                  type="number"
                  className="setup-text-input"
                  style={{ width: '100px' }}
                  step="0.1"
                  min="0.1"
                  value={form.insulin_sensitivity_factor}
                  onChange={(e) => updateRoot('insulin_sensitivity_factor', e.target.value)}
                />
                <span className="setup-value">mmol/L per U</span>
              </div>
            </FieldRow>

            <FieldRow label="Correction Target Threshold" hint="Target blood glucose value to correct down to">
              <div className="setup-slider-row">
                <input
                  type="number"
                  className="setup-text-input"
                  style={{ width: '100px' }}
                  step="0.1"
                  min="3.0"
                  value={form.correction_threshold}
                  onChange={(e) => updateRoot('correction_threshold', e.target.value)}
                />
                <span className="setup-value">mmol/L</span>
              </div>
            </FieldRow>

            <FieldRow label="Exercise Correction Factor" hint="Multiplier to reduce insulin action during exercise">
              <div className="setup-slider-row">
                <input
                  type="range"
                  min="1.0"
                  max="2.5"
                  step="0.1"
                  value={form.exercise_correction_factor}
                  onChange={(e) => updateRoot('exercise_correction_factor', e.target.value)}
                />
                <span className="setup-value">{num(form.exercise_correction_factor, 1)}x</span>
              </div>
            </FieldRow>

            <FieldRow label="Insulin-On-Board Method" hint="Pharmacokinetic decay calculation model">
              <select
                className="setup-text-input"
                value={form.iob_calculation_method}
                onChange={(e) => updateRoot('iob_calculation_method', e.target.value)}
                style={{ maxWidth: '280px', padding: '0.45rem' }}
              >
                <option value="exponential">Exponential (DIA Peak-aligned)</option>
                <option value="linear">Linear Decay (Straight Line)</option>
              </select>
            </FieldRow>

            {/* Rapid Acting Insulin PK Setup */}
            <h3 style={{ fontSize: '0.85rem', textTransform: 'uppercase', color: 'var(--text-faint)', borderBottom: '1px solid var(--border-muted)', paddingBottom: '0.3rem', marginTop: '1rem', marginBottom: '0.75rem' }}>
              Rapid-Acting Insulin Profile
            </h3>

            <FieldRow label="Select Insulin Template" hint="Select a predefined profile or custom">
              <select
                className="setup-text-input"
                value={getMatchingTemplateName(form.insulin_profile)}
                onChange={handleTemplateChange}
                style={{ maxWidth: '280px', padding: '0.45rem' }}
              >
                {INSULIN_TEMPLATES.map(t => (
                  <option key={t.profile.name} value={t.profile.name}>
                    {t.name}
                  </option>
                ))}
              </select>
            </FieldRow>

            <FieldRow label="Insulin Name" hint="e.g. NovoRapid, Humalog, Fiasp">
              <input
                type="text"
                className="setup-text-input"
                value={profile.name}
                maxLength={40}
                onChange={(e) => updateNested('insulin_profile', 'name', e.target.value)}
              />
            </FieldRow>

            <FieldRow label="Onset time" hint="Minutes before insulin begins acting">
              <div className="setup-slider-row">
                <input
                  type="range"
                  min={1}
                  max={60}
                  step={1}
                  value={profile.onsetMins}
                  onChange={(e) => updateNested('insulin_profile', 'onsetMins', e.target.value)}
                />
                <span className="setup-value">{profile.onsetMins} min</span>
              </div>
            </FieldRow>

            <FieldRow label="Early action time" hint="Minutes from onset to main action phase">
              <div className="setup-slider-row">
                <input
                  type="range"
                  min={10}
                  max={90}
                  step={5}
                  value={profile.earlyActionMins}
                  onChange={(e) => updateNested('insulin_profile', 'earlyActionMins', e.target.value)}
                />
                <span className="setup-value">{profile.earlyActionMins} min</span>
              </div>
            </FieldRow>

            <FieldRow label="Peak action window" hint="Hours when insulin effect is at maximum">
              <div className="setup-range-row">
                <span className="setup-range-label">From</span>
                <input
                  type="range"
                  min={0.5}
                  max={4}
                  step={0.25}
                  value={profile.peakMinHours}
                  onChange={(e) => updateNested('insulin_profile', 'peakMinHours', e.target.value)}
                />
                <span className="setup-value">{num(profile.peakMinHours, 2)} h</span>
                <span className="setup-range-label">to</span>
                <input
                  type="range"
                  min={0.75}
                  max={5}
                  step={0.25}
                  value={profile.peakMaxHours}
                  onChange={(e) => updateNested('insulin_profile', 'peakMaxHours', e.target.value)}
                />
                <span className="setup-value">{num(profile.peakMaxHours, 2)} h</span>
              </div>
            </FieldRow>

            <FieldRow label="Total duration range" hint="Hours for full insulin action clearance">
              <div className="setup-range-row">
                <span className="setup-range-label">From</span>
                <input
                  type="range"
                  min={1}
                  max={8}
                  step={0.5}
                  value={profile.durationMinHours}
                  onChange={(e) => updateNested('insulin_profile', 'durationMinHours', e.target.value)}
                />
                <span className="setup-value">{num(profile.durationMinHours, 1)} h</span>
                <span className="setup-range-label">to</span>
                <input
                  type="range"
                  min={1.5}
                  max={10}
                  step={0.5}
                  value={profile.durationMaxHours}
                  onChange={(e) => updateNested('insulin_profile', 'durationMaxHours', e.target.value)}
                />
                <span className="setup-value">{num(profile.durationMaxHours, 1)} h</span>
              </div>
            </FieldRow>

            <div className="setup-preview">
              <span className="setup-preview-label">Active PK Profile:</span>
              <span>
                {profile.name} · onset {profile.onsetMins} min · early action {profile.earlyActionMins} min ·
                peak {num(peakMid, 2)} h · duration {num(durationMid, 1)} h ·
                action starts at {num(onsetEnd, 2)} h
              </span>
            </div>
          </>
        )}

        {/* Form Action buttons */}
        <div className="setup-actions">
          <button className="btn-primary" onClick={handleSave} disabled={saving}>
            {saving ? 'Saving...' : 'Save Profile'}
          </button>
          {saved && <span className="msg-ok">Profile saved successfully.</span>}
        </div>

        {/* Danger Zone */}
        {!isRegistration && (
          <div style={{ marginTop: '3rem', paddingTop: '2rem', borderTop: '1px solid rgba(190, 47, 47, 0.25)' }}>
            <h3 style={{ color: '#be2f2f', fontSize: '1.05rem', fontWeight: 700, marginBottom: '0.5rem', textTransform: 'none', letterSpacing: 'normal' }}>Danger Zone</h3>
            <p className="hint" style={{ marginBottom: '1.2rem', color: 'var(--text-muted)' }}>
              Permanently delete your account, clinical configurations, and all uploaded walking/CGM telemetry data. This action is irreversible.
            </p>
            <button
              className="btn-danger"
              type="button"
              onClick={handleDeleteAccount}
              disabled={deletingAccount}
            >
              {deletingAccount ? 'Deleting Account...' : 'Delete My Account & Data'}
            </button>
            {deleteError && <p className="msg-error" style={{ marginTop: '0.75rem', color: '#be2f2f' }}>{deleteError}</p>}
          </div>
        )}
      </div>
    </div>
  )
}

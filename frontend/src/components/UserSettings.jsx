import { useState, useEffect } from 'react'
import SystemLogsSection from './SystemLogsSection'

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
  },
  garmin_email: null,
  carelink_email: null
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

export default function UserSettings({ userSettings, onSave, isRegistration, onBack, onDeleteAccount, apiFetch }) {
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
        insulin_profile: userSettings.insulin_profile || { ...DEFAULT_USER_SETTINGS.insulin_profile },
        garmin_email: userSettings.garmin_email || null
      }
    }
    return { ...DEFAULT_USER_SETTINGS }
  })
  
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [saved, setSaved] = useState(false)

  const [deletingAccount, setDeletingAccount] = useState(false)
  const [deleteError, setDeleteError] = useState(null)
  const [activeTab, setActiveTab] = useState('clinical')

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

  const [garminEmailInput, setGarminEmailInput] = useState('')
  const [garminPasswordInput, setGarminPasswordInput] = useState('')
  const [garminMfaCode, setGarminMfaCode] = useState('')
  const [garminStatus, setGarminStatus] = useState(() => form.garmin_email ? 'connected' : 'idle')
  const [garminError, setGarminError] = useState(null)

  const [carelinkFile, setCarelinkFile] = useState(null)
  const [carelinkStatus, setCarelinkStatus] = useState(() => form.carelink_email ? 'connected' : 'idle')
  const [carelinkError, setCarelinkError] = useState(null)
  const [uploadingCarelink, setUploadingCarelink] = useState(false)

  useEffect(() => {
    if (form.garmin_email) {
      setGarminStatus('connected')
    } else {
      setGarminStatus('idle')
    }
  }, [form.garmin_email])

  useEffect(() => {
    if (form.carelink_email) {
      setCarelinkStatus('connected')
    } else {
      setCarelinkStatus('idle')
    }
  }, [form.carelink_email])

  const handleCarelinkUpload = async (e) => {
    e.preventDefault()
    if (!carelinkFile) return
    setUploadingCarelink(true)
    setCarelinkError(null)
    const formData = new FormData()
    formData.append('file', carelinkFile)
    try {
      const res = await apiFetch('/api/carelink/upload-token', {
        method: 'POST',
        body: formData,
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Upload failed')

      setCarelinkStatus('connected')
      updateRoot('carelink_email', payload.username)
      alert(`Successfully connected to CareLink account: ${payload.username}`)
    } catch (err) {
      setCarelinkStatus('error')
      setCarelinkError(err.message)
    } finally {
      setUploadingCarelink(false)
    }
  }

  const handleCarelinkDisconnect = async () => {
    if (!window.confirm('Are you sure you want to disconnect CareLink? This deletes your session tokens.')) return
    try {
      const updatedSettings = { ...form, carelink_email: null }
      const res = await apiFetch('/api/user/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatedSettings),
      })
      if (!res.ok) throw new Error('Failed to disconnect settings')
      setCarelinkStatus('idle')
      setCarelinkFile(null)
      updateRoot('carelink_email', null)
      alert('Disconnected from CareLink.')
    } catch (err) {
      alert(err.message)
    }
  }

  const handleGarminConnect = async (e) => {
    e.preventDefault()
    if (!garminEmailInput || !garminPasswordInput) return
    setGarminStatus('connecting')
    setGarminError(null)
    try {
      const res = await apiFetch('/api/garmin/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: garminEmailInput, password: garminPasswordInput })
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Connection failed')

      if (payload.status === 'mfa_required') {
        setGarminStatus('mfa_required')
      } else {
        setGarminStatus('connected')
        updateRoot('garmin_email', garminEmailInput)
        alert('Connected to Garmin Connect successfully!')
      }
    } catch (err) {
      setGarminStatus('error')
      setGarminError(err.message)
    }
  }

  const handleGarminMfaSubmit = async (e) => {
    e.preventDefault()
    if (!garminMfaCode) return
    setGarminStatus('connecting')
    setGarminError(null)
    try {
      const res = await apiFetch('/api/garmin/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: garminEmailInput, mfa_code: garminMfaCode })
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'MFA validation failed')

      setGarminStatus('connected')
      updateRoot('garmin_email', garminEmailInput)
      alert('Connected to Garmin Connect successfully!')
    } catch (err) {
      setGarminStatus('mfa_required')
      setGarminError(err.message)
    }
  }

  const handleGarminDisconnect = async () => {
    if (!window.confirm('Are you sure you want to disconnect your Garmin account? This deletes cached tokens.')) return
    try {
      const updatedSettings = { ...form, garmin_email: null }
      const res = await apiFetch('/api/user/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatedSettings),
      })
      if (!res.ok) throw new Error('Failed to disconnect settings')
      setGarminStatus('idle')
      setGarminEmailInput('')
      setGarminPasswordInput('')
      setGarminMfaCode('')
      updateRoot('garmin_email', null)
      alert('Disconnected from Garmin Connect.')
    } catch (err) {
      alert(err.message)
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

      <div className="home-tabs" style={{ marginBottom: '1.25rem', justifyContent: 'flex-start', borderBottom: '1px solid var(--border-muted)', paddingBottom: '1px' }}>
        <button
          className={`home-tab${activeTab === 'clinical' ? ' home-tab--active' : ''}`}
          onClick={() => setActiveTab('clinical')}
          type="button"
          style={{ borderBottom: 'none' }}
        >
          Clinical Settings
        </button>
        <button
          className={`home-tab${activeTab === 'garmin' ? ' home-tab--active' : ''}`}
          onClick={() => setActiveTab('garmin')}
          type="button"
          style={{ borderBottom: 'none' }}
        >
          Garmin Sync
        </button>
        <button
          className={`home-tab${activeTab === 'carelink' ? ' home-tab--active' : ''}`}
          onClick={() => setActiveTab('carelink')}
          type="button"
          style={{ borderBottom: 'none' }}
        >
          CareLink Sync
        </button>
        <button
          className={`home-tab${activeTab === 'logs' ? ' home-tab--active' : ''}`}
          onClick={() => setActiveTab('logs')}
          type="button"
          style={{ borderBottom: 'none' }}
        >
          System Logs
        </button>
      </div>

      <div className="card insulin-setup-card">
        {activeTab === 'clinical' && (
          <>
            {/* User Type / Diabetic Status */}
            {/* ... rest of clinical fields ... */}
            {/* (Unmodified code target for clinical settings form inputs) */}
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

        {/* Garmin Connect Sync Section */}
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
          </>
        )}

        {activeTab === 'garmin' && (
          <div style={{ padding: '0.5rem 0' }}>
            <p className="hint" style={{ marginBottom: '1.5rem' }}>
              Link your Garmin account to fetch walking & hiking activities automatically. Credentials are only used in-memory during sync setup and never stored in the database.
            </p>

            {garminStatus === 'connected' ? (
              <div className="setup-preview" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', margin: '0.5rem 0 1.5rem', padding: '0.85rem 1.1rem' }}>
                <div>
                  <span className="setup-preview-label" style={{ color: 'var(--primary)' }}>Connected</span>
                  <span style={{ fontSize: '0.9rem' }}>Account: <strong>{form.garmin_email}</strong></span>
                </div>
                <button className="btn-secondary" type="button" onClick={handleGarminDisconnect} style={{ color: '#be2f2f', borderColor: 'rgba(190, 47, 47, 0.4)', padding: '0.45rem 1rem' }}>
                  Disconnect Account
                </button>
              </div>
            ) : (
              <div style={{ marginBottom: '1.5rem' }}>
                {garminStatus === 'mfa_required' ? (
                  <form onSubmit={handleGarminMfaSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '0.8rem', maxWidth: '380px' }}>
                    <div style={{ padding: '0.65rem 0.85rem', background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 'var(--radius)', color: '#1e40af', fontSize: '0.85rem' }}>
                      <strong>Two-Factor Authentication:</strong> A verification code was sent by Garmin to your email or authenticator. Please enter it below.
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                      <label style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Verification Code</label>
                      <input
                        type="text"
                        className="setup-text-input"
                        placeholder="e.g. 123456"
                        value={garminMfaCode}
                        onChange={(e) => setGarminMfaCode(e.target.value)}
                        autoFocus
                        required
                      />
                    </div>
                    <div style={{ display: 'flex', gap: '0.5rem', marginTop: '0.25rem' }}>
                      <button className="btn-primary" type="submit" disabled={garminStatus === 'connecting'} style={{ padding: '0.5rem 1rem' }}>
                        {garminStatus === 'connecting' ? 'Verifying Code...' : 'Submit Verification Code'}
                      </button>
                      <button
                        className="btn-secondary"
                        type="button"
                        onClick={() => {
                          setGarminStatus('idle')
                          setGarminError(null)
                          setGarminMfaCode('')
                        }}
                        style={{ padding: '0.5rem 1rem' }}
                      >
                        ← Back to Login
                      </button>
                    </div>
                    {garminError && (
                      <div style={{ marginTop: '0.5rem', padding: '0.65rem 0.85rem', background: '#fef2f2', border: '1px solid #fca5a5', borderRadius: 'var(--radius)', color: '#991b1b', fontSize: '0.85rem' }}>
                        {garminError}
                      </div>
                    )}
                  </form>
                ) : (
                  <form onSubmit={handleGarminConnect} style={{ display: 'flex', flexDirection: 'column', gap: '1rem', maxWidth: '380px' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                      <label style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Garmin Email Address</label>
                      <input
                        type="email"
                        className="setup-text-input"
                        placeholder="e.g. you@example.com"
                        value={garminEmailInput}
                        onChange={(e) => {
                          setGarminEmailInput(e.target.value)
                          if (garminError) setGarminError(null)
                        }}
                        required
                      />
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                      <label style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Garmin Password</label>
                      <input
                        type="password"
                        className="setup-text-input"
                        placeholder="Enter password"
                        value={garminPasswordInput}
                        onChange={(e) => {
                          setGarminPasswordInput(e.target.value)
                          if (garminError) setGarminError(null)
                        }}
                        required
                      />
                    </div>
                    <button className="btn-primary" type="submit" disabled={garminStatus === 'connecting'} style={{ alignSelf: 'flex-start', padding: '0.55rem 1.2rem' }}>
                      {garminStatus === 'connecting' ? 'Connecting to Garmin...' : 'Connect Garmin Account'}
                    </button>

                    {garminError && (
                      <div style={{
                        marginTop: '0.25rem',
                        padding: '0.75rem 1rem',
                        background: '#fef2f2',
                        border: '1px solid #fca5a5',
                        borderRadius: 'var(--radius)',
                        color: '#991b1b',
                        fontSize: '0.88rem',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.5rem'
                      }}>
                        <span style={{ fontSize: '1.1rem' }}>⚠️</span>
                        <span>{garminError}</span>
                      </div>
                    )}
                  </form>
                )}
              </div>
            )}
          </div>
        )}

        {activeTab === 'carelink' && (
          <div style={{ padding: '0.5rem 0' }}>
            <p className="hint" style={{ marginBottom: '1.5rem' }}>
              Link your Medtronic CareLink account by uploading your <code>logindata.json</code> token file. This file can be generated locally using the CLI login script.
            </p>

            {carelinkStatus === 'connected' ? (
              <div className="setup-preview" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', margin: '0.5rem 0 1.5rem', padding: '0.85rem 1.1rem' }}>
                <div>
                  <span className="setup-preview-label" style={{ color: 'var(--primary)' }}>Connected</span>
                  <span style={{ fontSize: '0.9rem' }}>Account: <strong>{form.carelink_email}</strong></span>
                </div>
                <button className="btn-secondary" type="button" onClick={handleCarelinkDisconnect} style={{ color: '#be2f2f', borderColor: 'rgba(190, 47, 47, 0.4)', padding: '0.45rem 1rem' }}>
                  Disconnect Account
                </button>
              </div>
            ) : (
              <div style={{ marginBottom: '1.5rem' }}>
                <form onSubmit={handleCarelinkUpload} style={{ display: 'flex', flexDirection: 'column', gap: '1rem', maxWidth: '400px' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                    <label style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Upload logindata.json</label>
                    <input
                      type="file"
                      accept=".json"
                      onChange={(e) => setCarelinkFile(e.target.files[0])}
                      style={{ padding: '0.35rem 0' }}
                      required
                    />
                  </div>
                  <button className="btn-primary" type="submit" disabled={uploadingCarelink} style={{ alignSelf: 'flex-start', padding: '0.55rem 1.2rem' }}>
                    {uploadingCarelink ? 'Connecting to CareLink...' : 'Upload & Connect'}
                  </button>
                </form>

                {carelinkStatus === 'error' && carelinkError && (
                  <p className="msg-error" style={{ marginTop: '0.75rem' }}>
                    {carelinkError}
                  </p>
                )}

                <section style={{ marginTop: '2rem', borderTop: '1px dashed var(--border-muted)', paddingTop: '1.5rem' }}>
                  <h4 style={{ fontSize: '0.9rem', color: 'var(--text-normal)', marginBottom: '0.5rem' }}>How to generate logindata.json:</h4>
                  <ol style={{ fontSize: '0.85rem', color: 'var(--text-muted)', paddingLeft: '1.2rem', display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
                    <li>Clone the community API script locally: <code>git clone https://github.com/ondrej1024/carelink-python-client.git</code></li>
                    <li>Install its python prerequisites: <code>pip install selenium-wire requests curlify</code></li>
                    <li>Run <code>python carelink_carepartner_api_login.py</code> (or add <code>--us</code> for US accounts).</li>
                    <li>Follow the browser automation window to sign in and solve any reCAPTCHA.</li>
                    <li>This creates <code>logindata.json</code> in that folder. Select and upload it here!</li>
                  </ol>
                </section>
              </div>
            )}
          </div>
        )}

        {activeTab === 'logs' && (
          <div style={{ padding: '0.25rem 0' }}>
            <SystemLogsSection apiFetch={apiFetch} />
          </div>
        )}
      </div>
    </div>
  )
}

import { useState } from 'react'

const STATUS = {
  full:    { label: 'Fully supported',   bg: 'var(--primary-subtle)', color: 'var(--primary)' },
  partial: { label: 'Partly supported',  bg: 'rgba(209,106,0,0.10)',  color: '#d16a00' },
  planned: { label: 'Planned',           bg: 'rgba(190,47,47,0.08)',  color: '#be2f2f' },
}

const PRIMARY_PERSONAS = [
  {
    id: 'pump',
    status: 'full',
    role: 'Diabetic — Pump / Closed-Loop',
    quote: '"I want to see how my closed-loop pump behaves when I climb hills, and evaluate if it successfully prevents hypos during and after my walks."',
    needs: [
      'Overlay SmartGuard / auto-suspend events on glucose + HR charts',
      'IOB estimation and insulin-stress-effect amplification at walk start',
      'Post-walk and overnight windows for late-onset exercise lag hypo',
    ],
  },
  {
    id: 'mdi',
    status: 'partial',
    role: 'Diabetic — Basal/Bolus (MDI)',
    quote: '"I don\'t wear a pump, but I monitor my glucose constantly. I need to know how my pre-walk meal injection affects my glucose trend while hiking."',
    needs: [
      'Log or import pen doses and align them to walk onset',
      'Glucose drop-rate analytics and rescue-carb timing',
      'Configurable fast-acting insulin decay profiles (NovoRapid, Fiasp…)',
    ],
  },
  {
    id: 'cgm',
    status: 'planned',
    role: 'Non-Diabetic — with CGM',
    quote: '"I wear a glucose sensor for endurance training and metabolic health. I want to optimise my fueling strategy and see when my energy levels dip."',
    needs: [
      'Match glucose spikes and dips to carbohydrate intake timing',
      'Metabolic efficiency analysis across pace, terrain, and pre-walk meals',
      'Clean dashboard — no insulin charts, no clinical hypo/hyper alerts',
    ],
  },
  {
    id: 'trad',
    status: 'partial',
    role: 'Non-Diabetic — Traditional Walker',
    quote: '"I just want to track my walks, look at my route on a map, analyse my heart rate zones, and see how stress affects my performance."',
    needs: [
      'Heart rate zones, pace, and altitude charted against distance',
      'Stress and decoupling analysis — observed HR vs expected HR',
      'Interactive GPS map coloured by HR zone with distance markers',
    ],
  },
]

const SECONDARY_PERSONAS = [
  {
    id: 'hcp',
    role: 'Healthcare Provider / Endocrinologist',
    quote: '"I want consolidated, context-rich exercise reports during clinic visits to help adjust daily basal rates and exercise parameters."',
    needs: [
      'Aggregated TIR, hypo count, and hyper count for each walk window',
      'Shareable standalone HTML exports — no account required to view',
    ],
  },
  {
    id: 'carer',
    role: 'Caregiver / Partner',
    quote: '"I want peace of mind when they\'re out on solo hikes, and to know what to expect when they recover overnight."',
    needs: [
      'Mobile-friendly read-only share links — no login needed',
      'Nocturnal hypo risk summary and suggested overnight alarm window',
    ],
  },
]

function StatusPill({ status }) {
  const s = STATUS[status]
  return (
    <span style={{
      display: 'inline-block',
      fontSize: '0.72rem',
      fontWeight: 600,
      letterSpacing: '0.03em',
      textTransform: 'uppercase',
      padding: '0.2rem 0.55rem',
      borderRadius: '20px',
      background: s.bg,
      color: s.color,
      whiteSpace: 'nowrap',
      flexShrink: 0,
    }}>
      {s.label}
    </span>
  )
}

function PersonaCard({ persona, secondary = false }) {
  return (
    <div style={{
      background: 'var(--surface)',
      border: '1px solid var(--border)',
      borderRadius: 'var(--radius-lg)',
      boxShadow: 'var(--shadow-sm)',
      padding: secondary ? '1rem 1.15rem' : '1.1rem 1.25rem',
      display: 'flex',
      flexDirection: 'column',
      gap: '0.6rem',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '0.75rem' }}>
        <span style={{
          fontSize: secondary ? '0.85rem' : '0.9rem',
          fontWeight: 700,
          color: 'var(--text)',
          lineHeight: 1.3,
        }}>
          {persona.role}
        </span>
        {persona.status && <StatusPill status={persona.status} />}
      </div>
      <p style={{
        fontSize: '0.83rem',
        color: 'var(--text-muted)',
        fontStyle: 'italic',
        lineHeight: 1.5,
        margin: 0,
      }}>
        {persona.quote}
      </p>
      <ul style={{
        paddingLeft: '1.1rem',
        margin: 0,
        display: 'flex',
        flexDirection: 'column',
        gap: '0.2rem',
      }}>
        {persona.needs.map((need, i) => (
          <li key={i} style={{ fontSize: '0.8rem', color: 'var(--text-muted)', lineHeight: 1.45 }}>
            {need}
          </li>
        ))}
      </ul>
    </div>
  )
}

export default function PersonasSection() {
  const [open, setOpen] = useState(false)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'center' }}>
        <button
          onClick={() => setOpen(o => !o)}
          style={{
            background: 'none',
            border: '1px solid var(--border)',
            borderRadius: 'var(--radius)',
            padding: '0.45rem 1rem',
            fontSize: '0.83rem',
            fontWeight: 500,
            color: 'var(--text-muted)',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '0.4rem',
            transition: 'color 0.15s, border-color 0.15s',
          }}
          onMouseEnter={e => { e.currentTarget.style.color = 'var(--text)'; e.currentTarget.style.borderColor = 'var(--text-muted)' }}
          onMouseLeave={e => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border)' }}
        >
          {open ? '▲' : '▼'} Who is this for?
        </button>
      </div>
      {open && <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
      <div>
        <p style={{
          fontSize: '0.75rem',
          fontWeight: 700,
          letterSpacing: '0.08em',
          textTransform: 'uppercase',
          color: 'var(--primary)',
          marginBottom: '0.5rem',
        }}>
          Who it's for
        </p>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
          gap: '0.75rem',
        }}>
          {PRIMARY_PERSONAS.map(p => (
            <PersonaCard key={p.id} persona={p} />
          ))}
        </div>
      </div>

      <div>
        <p style={{
          fontSize: '0.75rem',
          fontWeight: 700,
          letterSpacing: '0.08em',
          textTransform: 'uppercase',
          color: 'var(--text-faint)',
          marginBottom: '0.5rem',
        }}>
          Also designed for
        </p>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
          gap: '0.75rem',
        }}>
          {SECONDARY_PERSONAS.map(p => (
            <PersonaCard key={p.id} persona={p} secondary />
          ))}
        </div>
      </div>
      </div>}
    </div>
  )
}

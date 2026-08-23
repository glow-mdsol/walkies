"""Tests for the evidence-based exercise risk and recovery analytics."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import (
    _sensor_gap_detection,
    _extended_recovery_phases,
    _exercise_risk_profile,
)

_EPOCH = datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc)


def _pts(offsets_s: list[int]) -> list[dict]:
    """Build minimal activity_points with timestamps at given second offsets."""
    return [
        {
            'timestamp': int((_EPOCH + timedelta(seconds=s)).timestamp()),
            'timestamp_iso': (_EPOCH + timedelta(seconds=s)).isoformat(),
            'hr': 120,
            'distance_m': float(i * 100),
            'lat': None,
            'lon': None,
        }
        for i, s in enumerate(offsets_s)
    ]


def _bg(offset_min: int, value: float) -> dict:
    ts = (_EPOCH + timedelta(minutes=offset_min)).isoformat()
    return {'timestamp': ts, 'bg': value}


# --- sensor gap detection ---

def test_sensor_gap_no_gaps():
    pts = _pts([0, 3600])
    bg = [_bg(i * 5, 7.0) for i in range(13)]  # readings every 5 min
    assert _sensor_gap_detection(pts, bg) == []


def test_sensor_gap_detects_gap():
    pts = _pts([0, 3600])
    bg = [_bg(0, 7.0), _bg(5, 7.0), _bg(30, 7.0)]  # 25-min gap
    gaps = _sensor_gap_detection(pts, bg)
    assert len(gaps) == 1
    assert gaps[0]['duration_min'] == 25.0


def test_sensor_gap_ignores_pre_walk():
    # Walk starts at +30 min; readings every 5 min during the walk only.
    # The 30-min gap between the last pre-walk reading (t=0) and the first
    # in-walk reading (t=31) must NOT be counted.
    pts = _pts([60 * 30, 60 * 90])
    bg = (
        [_bg(-20, 7.0), _bg(0, 7.0)]  # pre-walk
        + [_bg(31 + i * 5, 7.0) for i in range(13)]  # every 5 min in walk
    )
    gaps = _sensor_gap_detection(pts, bg)
    assert gaps == []


def test_sensor_gap_empty_bg():
    pts = _pts([0, 3600])
    assert _sensor_gap_detection(pts, []) == []


# --- extended recovery phases ---

def test_recovery_phases_structure():
    pts = _pts([0, 3600])
    bg_all = [_bg(i * 5, 7.0) for i in range(-12, 200)]  # covers pre and post
    phases = _extended_recovery_phases(pts, bg_all)
    assert len(phases) == 3
    keys = [p['key'] for p in phases]
    assert keys == ['post_0_2h', 'post_2_6h', 'post_6_12h']


def test_recovery_phases_counts_hypos():
    pts = _pts([0, 60])  # walk is 1 min
    # Reading at 30 min post (in 0-2h window) that is hypo
    bg_all = [_bg(31, 3.5), _bg(40, 3.7), _bg(50, 5.0)]
    phases = _extended_recovery_phases(pts, bg_all)
    post_0_2h = next(p for p in phases if p['key'] == 'post_0_2h')
    assert post_0_2h['hypos'] == 2
    assert post_0_2h['min_bg'] == 3.5


def test_recovery_phases_empty_when_no_bg():
    pts = _pts([0, 3600])
    phases = _extended_recovery_phases(pts, [])
    assert all(p['count'] == 0 for p in phases)


# --- exercise risk profile ---

def _basic_metrics(avg_hr=120.0, slope=-1.0, duration_h=1.5):
    return {
        'avg_hr': avg_hr,
        'bg_slope_during_h': slope,
        'duration_h': duration_h,
    }


def test_risk_profile_prewalk_optimal():
    pts = _pts([0, 3600])
    bg_all = [_bg(-5, 8.5), _bg(30, 7.0)]  # 8.5 at start → optimal
    result = _exercise_risk_profile(pts, bg_all, _basic_metrics(), [])
    assert result['prewalk']['assessment'] == 'optimal'


def test_risk_profile_prewalk_low():
    pts = _pts([0, 3600])
    bg_all = [_bg(-5, 5.5)]
    result = _exercise_risk_profile(pts, bg_all, _basic_metrics(), [])
    assert result['prewalk']['assessment'] == 'low'


def test_risk_profile_intensity_stratification():
    pts = _pts([0, 3600])
    bg_all = [_bg(0, 8.0)]

    light   = _exercise_risk_profile(pts, bg_all, _basic_metrics(avg_hr=95), [])
    mod     = _exercise_risk_profile(pts, bg_all, _basic_metrics(avg_hr=120), [])
    vig     = _exercise_risk_profile(pts, bg_all, _basic_metrics(avg_hr=145), [])

    assert light['during']['intensity'] == 'light'
    assert light['nextWalk']['bolus_reduction_pct'] < mod['nextWalk']['bolus_reduction_pct']
    assert mod['nextWalk']['bolus_reduction_pct'] < vig['nextWalk']['bolus_reduction_pct']


def test_risk_profile_drop_flag_rapid():
    pts = _pts([0, 3600])
    bg_all = [_bg(-5, 8.0), _bg(60, 5.0)]
    result = _exercise_risk_profile(pts, bg_all, _basic_metrics(slope=-2.5), [])
    assert result['during']['drop_flag'] == 'rapid'


def test_risk_profile_hypo_projection():
    pts = _pts([0, 3600])
    bg_all = [_bg(-5, 8.0), _bg(60, 5.5)]  # bg_at_end ≈ 5.5, slope -1 → ~96 min to 3.9
    result = _exercise_risk_profile(pts, bg_all, _basic_metrics(slope=-1.0), [])
    assert result['during']['projected_hypo_min'] is not None
    assert result['during']['projected_hypo_min'] > 0


def test_risk_profile_overnight_risk_from_confirmed_hypo():
    pts = _pts([0, 60])
    bg_all = []
    recovery = [
        {'key': 'post_0_2h', 'label': '0–2 h', 'count': 3, 'min_bg': 4.2, 'mean_bg': 5.0, 'hypos': 0, 'slope_per_hour': -0.5},
        {'key': 'post_2_6h', 'label': '2–6 h', 'count': 2, 'min_bg': 3.5, 'mean_bg': 4.0, 'hypos': 1, 'slope_per_hour': -1.0},
        {'key': 'post_6_12h', 'label': '6–12 h', 'count': 0, 'min_bg': None, 'mean_bg': None, 'hypos': 0, 'slope_per_hour': None},
    ]
    result = _exercise_risk_profile(pts, bg_all, _basic_metrics(), recovery)
    assert result['recovery']['overnight_risk'] == 'confirmed'


def test_risk_profile_empty_when_no_activity():
    assert _exercise_risk_profile([], [], {}, []) == {}

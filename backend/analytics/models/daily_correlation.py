"""Deterministic 24-hour and multi-day epochal movement & glycemic correlation model.

Grounded in peer-reviewed clinical consensus:
- Battelino et al. (2019), ATTD International Consensus on Time in Range (Diabetes Care 42(8):1593-1603)
- Moser et al. (2020), EASD/ISPAD Position Statement on CGM in Exercise (Diabetologia 63(12):2501-2520)
- Riddell et al. (2017), Exercise Management in Type 1 Diabetes (Lancet Diabetes Endocrinol 5(5):377-390)
"""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

SENSOR_GAP_THRESHOLD_SECS = 15 * 60  # >15 minutes (900 seconds) marks a CGM sensor gap
HYPO_MIN_DURATION_SECS = 15 * 60     # ATTD consensus: >=15 minutes below threshold


def _parse_utc(ts_str: str) -> datetime:
    dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_local_hhmm(utc_dt: datetime, tz_offset_mins: int = 0) -> str:
    local_dt = utc_dt + timedelta(minutes=int(tz_offset_mins or 0))
    return local_dt.strftime("%H:%M")


def detect_sensor_gaps(
    cgm_points: list[dict],
    gap_threshold_secs: float = SENSOR_GAP_THRESHOLD_SECS,
) -> list[dict]:
    """Identify CGM data gaps strictly greater than 15 minutes (900 seconds)."""
    if len(cgm_points) < 2:
        return []

    sorted_pts = sorted(cgm_points, key=lambda p: p["ts_unix"])
    gaps: list[dict] = []
    for idx in range(1, len(sorted_pts)):
        prev_pt = sorted_pts[idx - 1]
        curr_pt = sorted_pts[idx]
        delta_s = curr_pt["ts_unix"] - prev_pt["ts_unix"]
        if delta_s > gap_threshold_secs:
            gaps.append({
                "start_utc": prev_pt["timestamp_utc"],
                "end_utc": curr_pt["timestamp_utc"],
                "start_unix": prev_pt["ts_unix"],
                "end_unix": curr_pt["ts_unix"],
                "duration_mins": round(delta_s / 60.0, 1),
            })
    return gaps


def detect_sustained_hypo_episodes(
    cgm_points: list[dict],
    hypo_threshold_mmol: float = 3.9,
    min_duration_secs: float = HYPO_MIN_DURATION_SECS,
    gap_threshold_secs: float = SENSOR_GAP_THRESHOLD_SECS,
) -> list[dict]:
    """Detect sustained hypoglycemia episodes (>=15 continuous minutes <= threshold).

    Per ATTD International Consensus (Battelino et al., 2019), a clinical CGM
    hypoglycemic event requires at least 15 minutes below threshold (either >=900s
    span or >=3 consecutive 5-minute CGM readings without a sensor gap >15m).
    """
    if not cgm_points:
        return []

    sorted_pts = sorted(cgm_points, key=lambda p: p["ts_unix"])
    episodes: list[dict] = []
    current_run: list[dict] = []

    def _finalize_run(run: list[dict]) -> None:
        if not run:
            return
        span_secs = run[-1]["ts_unix"] - run[0]["ts_unix"]
        # Either time span >= 15 mins (900s) OR >= 3 consecutive 5-min samples (~15m exposure)
        if span_secs >= min_duration_secs - 30 or len(run) >= 3:
            episodes.append({
                "start_utc": run[0]["timestamp_utc"],
                "end_utc": run[-1]["timestamp_utc"],
                "start_unix": run[0]["ts_unix"],
                "end_unix": run[-1]["ts_unix"],
                "duration_mins": round(max(span_secs, len(run) * 300.0) / 60.0, 1),
                "nadir_bg_mmol": round(min(p["bg_mmol"] for p in run), 2),
                "sample_count": len(run),
            })

    for pt in sorted_pts:
        bg = pt.get("bg_mmol")
        if bg is None:
            continue
        if bg <= hypo_threshold_mmol:
            if current_run and (pt["ts_unix"] - current_run[-1]["ts_unix"]) > gap_threshold_secs:
                _finalize_run(current_run)
                current_run = [pt]
            else:
                current_run.append(pt)
        else:
            if current_run:
                _finalize_run(current_run)
                current_run = []

    if current_run:
        _finalize_run(current_run)

    return episodes


def _compute_iob_at_unix(
    target_unix: float,
    bolus_points: list[dict],
    dia_hours: float = 4.0,
) -> float:
    """Compute rapid-acting Insulin-On-Board (IOB) at target_unix using a smooth Walsh/exponential curve."""
    dia_secs = max(1.0, float(dia_hours) * 3600.0)
    total_iob = 0.0
    for b in bolus_points:
        dt_secs = target_unix - b["ts_unix"]
        if 0.0 <= dt_secs < dia_secs:
            frac = dt_secs / dia_secs
            # Smooth cubic S-curve decay matching rapid-acting insulin action (peak ~75m, zero at DIA)
            remaining = 1.0 - (3.0 * (frac ** 2) - 2.0 * (frac ** 3))
            total_iob += float(b["units"]) * max(0.0, remaining)
    return round(total_iob, 2)


def _lookup_bg_near_unix(
    target_unix: float,
    cgm_points: list[dict],
    sensor_gaps: list[dict],
    max_tolerance_secs: float = 450.0,
) -> float | None:
    """Lookup or linearly interpolate BG at target_unix unless inside a >15m sensor gap."""
    if not cgm_points:
        return None

    for gap in sensor_gaps:
        if gap["start_unix"] < target_unix < gap["end_unix"]:
            return None

    nearest = min(cgm_points, key=lambda p: abs(p["ts_unix"] - target_unix))
    if abs(nearest["ts_unix"] - target_unix) <= max_tolerance_secs:
        return round(float(nearest["bg_mmol"]), 2)
    return None


def compute_daily_correlation(
    local_date: str,
    epochs: list[dict],
    glycemic_events: list[dict],
    wellness_summary: dict | None = None,
    user_settings: dict | None = None,
    linked_walks: list[dict] | None = None,
) -> dict:
    """Compute the full 24-hour epochal movement and glycemic correlation payload for local_date."""
    settings = user_settings or {}
    user_type = settings.get("user_type") or "diabetic_closed_loop"
    insulin_type = settings.get("insulin_type") or "ClosedLoopPump"
    glucose_unit = settings.get("glucose_unit") or "mmol/L"
    target_low = float(settings.get("target_low_mmol") or 4.0)
    target_high = float(settings.get("target_high_mmol") or 8.0)
    hypo_thresh = float(settings.get("hypo_threshold_mmol") or 3.9)
    hyper_thresh = float(settings.get("hyper_threshold_mmol") or 12.9)

    dia_hours = 4.0
    insulin_profile = settings.get("insulin_profile")
    if isinstance(insulin_profile, dict) and insulin_profile.get("durationMaxHours"):
        try:
            dia_hours = float(insulin_profile["durationMaxHours"])
        except (TypeError, ValueError):
            dia_hours = 4.0

    # Partition glycemic events
    all_cgm: list[dict] = []
    day_cgm: list[dict] = []
    all_boluses: list[dict] = []
    day_boluses: list[dict] = []
    day_carbs: list[dict] = []
    day_pump_states: list[dict] = []

    for ev in glycemic_events:
        ev_type = ev.get("event_type")
        ts_utc = ev.get("timestamp_utc")
        if not ts_utc:
            continue
        try:
            dt_utc = _parse_utc(ts_utc)
            ts_unix = dt_utc.timestamp()
        except ValueError:
            continue

        ev_date = ev.get("local_date") or dt_utc.strftime("%Y-%m-%d")
        tz_off = int(ev.get("tz_offset_mins") or 0)

        if ev_type == "cgm" and ev.get("bg_mmol") is not None:
            bg_val = float(ev["bg_mmol"])
            if 1.0 <= bg_val <= 35.0:
                pt = {
                    "timestamp_utc": dt_utc.isoformat(),
                    "ts_unix": ts_unix,
                    "local_date": ev_date,
                    "local_time": _format_local_hhmm(dt_utc, tz_off),
                    "bg_mmol": round(bg_val, 2),
                    "trend": ev.get("trend_arrow"),
                }
                all_cgm.append(pt)
                if ev_date == local_date:
                    day_cgm.append(pt)
        elif ev_type == "bolus" and ev.get("bolus_units") is not None:
            units = float(ev["bolus_units"])
            if units > 0:
                b_pt = {
                    "timestamp_utc": dt_utc.isoformat(),
                    "ts_unix": ts_unix,
                    "local_date": ev_date,
                    "units": round(units, 2),
                }
                all_boluses.append(b_pt)
                if ev_date == local_date:
                    day_boluses.append({"timestamp_utc": dt_utc.isoformat(), "units": round(units, 2)})
        elif ev_type == "carb" and ev.get("carb_grams") is not None:
            grams = float(ev["carb_grams"])
            if grams > 0 and ev_date == local_date:
                day_carbs.append({"timestamp_utc": dt_utc.isoformat(), "grams": round(grams, 1)})
        elif ev_type == "pump_state" and ev.get("pump_state") and ev_date == local_date:
            day_pump_states.append({"timestamp_utc": dt_utc.isoformat(), "state": ev["pump_state"]})

    all_cgm.sort(key=lambda p: p["ts_unix"])
    day_cgm.sort(key=lambda p: p["ts_unix"])
    all_boluses.sort(key=lambda p: p["ts_unix"])

    # Sensor gaps (>15 mins)
    day_gaps_internal = detect_sensor_gaps(day_cgm)
    all_gaps_internal = detect_sensor_gaps(all_cgm)
    sensor_gaps = [
        {
            "start_utc": g["start_utc"],
            "end_utc": g["end_utc"],
            "duration_mins": g["duration_mins"],
        }
        for g in day_gaps_internal
    ]

    # Sustained hypo episodes (>=15 mins <= hypo_threshold_mmol)
    day_hypo_episodes = detect_sustained_hypo_episodes(day_cgm, hypo_threshold_mmol=hypo_thresh)
    all_hypo_episodes = detect_sustained_hypo_episodes(all_cgm, hypo_threshold_mmol=hypo_thresh)

    # 24-hour CGM summary stats
    bg_values = [p["bg_mmol"] for p in day_cgm]
    if bg_values:
        n_bg = len(bg_values)
        in_range = sum(1 for v in bg_values if target_low <= v <= target_high)
        below_range = sum(1 for v in bg_values if v < target_low)
        above_range = sum(1 for v in bg_values if v > target_high)
        tir_pct = round((in_range / n_bg) * 100.0, 1)
        tbr_pct = round((below_range / n_bg) * 100.0, 1)
        tar_pct = round((above_range / n_bg) * 100.0, 1)
        avg_bg_mmol = round(sum(bg_values) / n_bg, 2)
        if n_bg >= 2 and avg_bg_mmol > 0:
            bg_cv_pct = round((statistics.stdev(bg_values) / avg_bg_mmol) * 100.0, 1)
        else:
            bg_cv_pct = 0.0
        # 288 5-min samples per 24h day, minus any >15m gap time
        gap_mins = sum(g["duration_mins"] for g in day_gaps_internal)
        coverage_from_samples = min(100.0, (n_bg / 288.0) * 100.0)
        coverage_from_gaps = max(0.0, ((1440.0 - gap_mins) / 1440.0) * 100.0) if n_bg >= 20 else coverage_from_samples
        sensor_coverage_pct = round(min(coverage_from_samples, coverage_from_gaps), 1)
    else:
        tir_pct = None
        tbr_pct = None
        tar_pct = None
        avg_bg_mmol = None
        bg_cv_pct = None
        sensor_coverage_pct = 0.0

    # Align 15-minute epochs with CGM and IOB
    uses_insulin = insulin_type not in ("None", None) and user_type not in (
        "diabetic_lifestyle",
        "non_diabetic_cgm",
        "non_diabetic",
    )

    correlated_epochs: list[dict] = []
    sorted_epochs = sorted(epochs, key=lambda e: e["epoch_start_utc"])

    for ep in sorted_epochs:
        try:
            s_dt = _parse_utc(ep["epoch_start_utc"])
            e_dt = _parse_utc(ep["epoch_end_utc"])
        except ValueError:
            continue
        s_unix = s_dt.timestamp()
        e_unix = e_dt.timestamp()
        tz_off = int(ep.get("tz_offset_mins") or 0)

        # Check if a >15m gap spans this epoch
        has_gap = any(
            g["start_unix"] <= s_unix and g["end_unix"] >= e_unix
            for g in all_gaps_internal
        )

        ep_bgs = [
            p["bg_mmol"]
            for p in all_cgm
            if (s_unix - 150.0) <= p["ts_unix"] <= (e_unix + 150.0)
        ] if not has_gap else []

        ep_avg_bg = round(sum(ep_bgs) / len(ep_bgs), 2) if ep_bgs else None

        start_bg = _lookup_bg_near_unix(s_unix, all_cgm, all_gaps_internal)
        end_bg = _lookup_bg_near_unix(e_unix, all_cgm, all_gaps_internal)
        if start_bg is not None and end_bg is not None and not has_gap:
            bg_delta = round(end_bg - start_bg, 2)
        elif len(ep_bgs) >= 2 and not has_gap:
            bg_delta = round(ep_bgs[-1] - ep_bgs[0], 2)
        else:
            bg_delta = None

        # Also compute 1-hour post-epoch BG change for activity category aggregation
        bg_1h_post = _lookup_bg_near_unix(e_unix + 3600.0, all_cgm, all_gaps_internal)
        change_1h_post = (
            round(bg_1h_post - end_bg, 2)
            if (bg_1h_post is not None and end_bg is not None)
            else None
        )

        iob_val = (
            _compute_iob_at_unix(s_unix, all_boluses, dia_hours=dia_hours)
            if uses_insulin
            else None
        )

        correlated_epochs.append({
            "epoch_start_utc": s_dt.isoformat(),
            "epoch_end_utc": e_dt.isoformat(),
            "start_unix": s_unix,
            "end_unix": e_unix,
            "local_time": _format_local_hhmm(s_dt, tz_off),
            "steps": int(ep.get("steps") or 0),
            "activity_category": ep.get("activity_category") or "sedentary",
            "intensity_level": ep.get("intensity_level") or "sedentary",
            "distance_m": ep.get("distance_m"),
            "active_kcal": ep.get("active_kcal"),
            "mean_hr": ep.get("mean_hr"),
            "max_hr": ep.get("max_hr"),
            "avg_bg_mmol": ep_avg_bg,
            "bg_delta_mmol": bg_delta,
            "bg_change_1h_post_mmol": change_1h_post,
            "iob_units": iob_val,
            "linked_walk_id": ep.get("linked_walk_id"),
        })

    # Activity Category Breakdown
    category_groups: dict[str, list[dict]] = {}
    for cep in correlated_epochs:
        cat = cep["activity_category"]
        category_groups.setdefault(cat, []).append(cep)

    category_breakdown: list[dict] = []
    for cat, cat_epochs in category_groups.items():
        duration_mins = len(cat_epochs) * 15
        cat_steps = sum(e["steps"] for e in cat_epochs)
        cat_bgs = [e["avg_bg_mmol"] for e in cat_epochs if e["avg_bg_mmol"] is not None]
        cat_avg_bg = round(sum(cat_bgs) / len(cat_bgs), 2) if cat_bgs else None

        post_1h_changes = [
            e["bg_change_1h_post_mmol"]
            for e in cat_epochs
            if e["bg_change_1h_post_mmol"] is not None
        ]
        mean_1h_post = (
            round(sum(post_1h_changes) / len(post_1h_changes), 2)
            if post_1h_changes
            else None
        )

        if cat_bgs:
            cat_in_range = sum(1 for v in cat_bgs if target_low <= v <= target_high)
            cat_tir = round((cat_in_range / len(cat_bgs)) * 100.0, 1)
        else:
            cat_tir = None

        # Count sustained hypo episodes occurring during or within 2 hours (7200s) post category epochs
        # For sedentary/sleeping, only count hypos occurring strictly during those epochs so post-exercise
        # delayed hypos are attributed to the preceding exercise rather than subsequent rest.
        post_window_secs = 0.0 if cat in ("sedentary", "sleeping") else 7200.0
        matched_hypo_starts: set[float] = set()
        for hypo in all_hypo_episodes:
            for e in cat_epochs:
                if hypo["start_unix"] <= (e["end_unix"] + post_window_secs) and hypo["end_unix"] >= e["start_unix"]:
                    matched_hypo_starts.add(hypo["start_unix"])
                    break

        category_breakdown.append({
            "category": cat,
            "duration_mins": duration_mins,
            "steps": cat_steps,
            "avg_bg_mmol": cat_avg_bg,
            "mean_bg_change_1h_post_mmol": mean_1h_post,
            "tir_pct": cat_tir,
            "hypo_events_during_or_2h_post": len(matched_hypo_starts),
        })

    # Sort category breakdown so active movement types appear first, then sedentary/sleeping
    _cat_order = {
        "walking": 0,
        "running": 1,
        "cycling": 2,
        "highly_active": 3,
        "other": 4,
        "sedentary": 5,
        "sleeping": 6,
    }
    category_breakdown.sort(key=lambda row: (_cat_order.get(row["category"], 99), -row["steps"]))

    # Total steps & active minutes (never double-counting recorded walks)
    epoch_steps_total = sum(e["steps"] for e in correlated_epochs)
    summary_steps = (wellness_summary or {}).get("total_steps")
    if summary_steps is not None and int(summary_steps) >= epoch_steps_total:
        total_steps = int(summary_steps)
    else:
        total_steps = epoch_steps_total

    active_epoch_mins = sum(
        15
        for e in correlated_epochs
        if e["activity_category"] not in ("sedentary", "sleeping")
        or e["intensity_level"] in ("moderate", "vigorous")
    )
    if wellness_summary and (
        wellness_summary.get("moderate_intensity_mins") is not None
        or wellness_summary.get("vigorous_intensity_mins") is not None
    ):
        mod_m = int(wellness_summary.get("moderate_intensity_mins") or 0)
        vig_m = int(wellness_summary.get("vigorous_intensity_mins") or 0)
        active_minutes = max(active_epoch_mins, mod_m + vig_m)
    else:
        active_minutes = active_epoch_mins

    resting_hr = (wellness_summary or {}).get("resting_hr")
    bolus_total = round(sum(b["units"] for b in day_boluses), 2) if day_boluses else 0.0
    carb_total = round(sum(c["grams"] for c in day_carbs), 1) if day_carbs else 0.0

    # Strip internal unix helpers from serialized epochs
    public_epochs = [
        {
            "epoch_start_utc": e["epoch_start_utc"],
            "epoch_end_utc": e["epoch_end_utc"],
            "local_time": e["local_time"],
            "steps": e["steps"],
            "activity_category": e["activity_category"],
            "intensity_level": e["intensity_level"],
            "distance_m": e["distance_m"],
            "active_kcal": e["active_kcal"],
            "mean_hr": e["mean_hr"],
            "max_hr": e["max_hr"],
            "avg_bg_mmol": e["avg_bg_mmol"],
            "bg_delta_mmol": e["bg_delta_mmol"],
            "iob_units": e["iob_units"],
            "linked_walk_id": e["linked_walk_id"],
        }
        for e in correlated_epochs
    ]

    public_cgm = [
        {
            "timestamp_utc": p["timestamp_utc"],
            "local_time": p["local_time"],
            "bg_mmol": p["bg_mmol"],
            "trend": p["trend"],
        }
        for p in day_cgm
    ]

    return {
        "date": local_date,
        "regimen": {
            "user_type": user_type,
            "insulin_type": insulin_type,
            "glucose_unit": glucose_unit,
            "target_low_mmol": target_low,
            "target_high_mmol": target_high,
            "hypo_threshold_mmol": hypo_thresh,
            "hyper_threshold_mmol": hyper_thresh,
        },
        "summary": {
            "total_steps": total_steps,
            "active_minutes": active_minutes,
            "resting_hr": resting_hr,
            "tir_pct": tir_pct,
            "tbr_pct": tbr_pct,
            "tar_pct": tar_pct,
            "avg_bg_mmol": avg_bg_mmol,
            "bg_cv_pct": bg_cv_pct,
            "hypos_count": len(day_hypo_episodes),
            "bolus_total_units": bolus_total,
            "carb_total_grams": carb_total,
            "sensor_coverage_pct": sensor_coverage_pct,
        },
        "epochs": public_epochs,
        "cgm_series": public_cgm,
        "sensor_gaps": sensor_gaps,
        "hypo_episodes": [
            {
                "start_utc": h["start_utc"],
                "end_utc": h["end_utc"],
                "duration_mins": h["duration_mins"],
                "nadir_bg_mmol": h["nadir_bg_mmol"],
            }
            for h in day_hypo_episodes
        ],
        "therapy_events": {
            "boluses": day_boluses,
            "carbs": day_carbs,
            "pump_states": day_pump_states,
        },
        "activity_category_breakdown": category_breakdown,
        "linked_walks": linked_walks or [],
    }


def compute_daily_range_summary(
    start_date: str,
    end_date: str,
    day_payloads: list[dict],
    user_settings: dict | None = None,
) -> dict:
    """Aggregate daily correlation payloads across [start_date, end_date]."""
    settings = user_settings or {}
    sorted_days = sorted(day_payloads, key=lambda d: d.get("date") or "")

    total_steps = 0
    tir_vals: list[float] = []
    tbr_vals: list[float] = []
    tar_vals: list[float] = []
    avg_bg_vals: list[float] = []
    active_day_tirs: list[float] = []
    sedentary_day_tirs: list[float] = []

    active_bg_deltas: list[float] = []
    active_steps_for_delta = 0

    cat_Rollup: dict[str, dict[str, Any]] = {}
    day_rows: list[dict] = []

    for d_payload in sorted_days:
        d_str = d_payload.get("date")
        summary = d_payload.get("summary") or {}
        steps = int(summary.get("total_steps") or 0)
        act_mins = int(summary.get("active_minutes") or 0)
        tir = summary.get("tir_pct")
        tbr = summary.get("tbr_pct")
        tar = summary.get("tar_pct")
        avg_bg = summary.get("avg_bg_mmol")
        hypos = int(summary.get("hypos_count") or 0)
        linked_walks = d_payload.get("linked_walks") or []

        total_steps += steps
        if tir is not None:
            tir_vals.append(float(tir))
            is_active_day = act_mins >= 30 or steps >= 6000 or len(linked_walks) > 0
            if is_active_day:
                active_day_tirs.append(float(tir))
            else:
                sedentary_day_tirs.append(float(tir))
        if tbr is not None:
            tbr_vals.append(float(tbr))
        if tar is not None:
            tar_vals.append(float(tar))
        if avg_bg is not None:
            avg_bg_vals.append(float(avg_bg))

        for ep in d_payload.get("epochs") or []:
            if ep.get("activity_category") not in ("sedentary", "sleeping") and ep.get("steps", 0) >= 100:
                if ep.get("bg_delta_mmol") is not None:
                    active_bg_deltas.append(float(ep["bg_delta_mmol"]))
                    active_steps_for_delta += int(ep["steps"])

        for cat_row in d_payload.get("activity_category_breakdown") or []:
            cat = cat_row["category"]
            bucket = cat_Rollup.setdefault(
                cat,
                {
                    "category": cat,
                    "total_mins": 0,
                    "total_steps": 0,
                    "tir_vals": [],
                    "bg_deltas": [],
                    "hypos": 0,
                },
            )
            bucket["total_mins"] += int(cat_row.get("duration_mins") or 0)
            bucket["total_steps"] += int(cat_row.get("steps") or 0)
            if cat_row.get("tir_pct") is not None:
                bucket["tir_vals"].append(float(cat_row["tir_pct"]))
            if cat_row.get("mean_bg_change_1h_post_mmol") is not None:
                bucket["bg_deltas"].append(float(cat_row["mean_bg_change_1h_post_mmol"]))
            bucket["hypos"] += int(cat_row.get("hypo_events_during_or_2h_post") or 0)

        day_rows.append({
            "date": d_str,
            "total_steps": steps,
            "active_minutes": act_mins,
            "tir_pct": tir,
            "tbr_pct": tbr,
            "tar_pct": tar,
            "avg_bg_mmol": avg_bg,
            "bg_cv_pct": summary.get("bg_cv_pct"),
            "hypos_count": hypos,
            "bolus_total_units": summary.get("bolus_total_units"),
            "carb_total_grams": summary.get("carb_total_grams"),
            "sensor_coverage_pct": summary.get("sensor_coverage_pct"),
            "linked_walks_count": len(linked_walks),
        })

    days_count = len(sorted_days)
    avg_daily_steps = int(round(total_steps / days_count)) if days_count > 0 else 0

    if active_steps_for_delta > 0 and active_bg_deltas:
        bg_drop_per_1000 = round((sum(active_bg_deltas) / active_steps_for_delta) * 1000.0, 2)
    else:
        bg_drop_per_1000 = None

    category_totals: list[dict] = []
    for cat, bucket in cat_Rollup.items():
        category_totals.append({
            "category": cat,
            "total_mins": bucket["total_mins"],
            "total_steps": bucket["total_steps"],
            "tir_pct": round(sum(bucket["tir_vals"]) / len(bucket["tir_vals"]), 1) if bucket["tir_vals"] else None,
            "mean_bg_delta_mmol": round(sum(bucket["bg_deltas"]) / len(bucket["bg_deltas"]), 2) if bucket["bg_deltas"] else None,
            "hypo_events_during_or_2h_post": bucket["hypos"],
        })
    category_totals.sort(key=lambda r: -r["total_steps"])

    return {
        "start_date": start_date,
        "end_date": end_date,
        "days_count": days_count,
        "regimen": {
            "user_type": settings.get("user_type") or "diabetic_closed_loop",
            "insulin_type": settings.get("insulin_type") or "ClosedLoopPump",
            "glucose_unit": settings.get("glucose_unit") or "mmol/L",
            "target_low_mmol": float(settings.get("target_low_mmol") or 4.0),
            "target_high_mmol": float(settings.get("target_high_mmol") or 8.0),
            "hypo_threshold_mmol": float(settings.get("hypo_threshold_mmol") or 3.9),
            "hyper_threshold_mmol": float(settings.get("hyper_threshold_mmol") or 12.9),
        },
        "aggregate": {
            "total_steps": total_steps,
            "avg_daily_steps": avg_daily_steps,
            "tir_pct": round(sum(tir_vals) / len(tir_vals), 1) if tir_vals else None,
            "tbr_pct": round(sum(tbr_vals) / len(tbr_vals), 1) if tbr_vals else None,
            "tar_pct": round(sum(tar_vals) / len(tar_vals), 1) if tar_vals else None,
            "avg_bg_mmol": round(sum(avg_bg_vals) / len(avg_bg_vals), 2) if avg_bg_vals else None,
            "bg_drop_per_1000_steps_mmol": bg_drop_per_1000,
            "active_days_tir_pct": round(sum(active_day_tirs) / len(active_day_tirs), 1) if active_day_tirs else None,
            "sedentary_days_tir_pct": round(sum(sedentary_day_tirs) / len(sedentary_day_tirs), 1) if sedentary_day_tirs else None,
        },
        "activity_category_totals": category_totals,
        "days": day_rows,
    }

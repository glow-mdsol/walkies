# Requirements from the literature

_Walkies · Evidence Review · 8 papers · July 2026_

Distilled from the source PDFs in this directory. Three tiers:
- **Validated** — already implemented; evidence confirms the approach is correct
- **New** — not yet in the app
- **Enhancement** — exists but incomplete

---

## Evidence-validated features

### V-01 · Time in range with clinical thresholds

**Evidence:** Riddell 2017 consensus — target BG 7–10 mmol/L before exercise; TIR 3.9–10 mmol/L for general population; in-range 4.0–8.0 aligns with modified exercise target. Zaharieva 2020 Table 3 — same thresholds for closed-loop strategies.

| What the app does | Verdict |
|---|---|
| Shows TIR% for the 4.0–8.0 mmol/L band. Flags hypo ≤3.9 and hyper ≥12.9. | Thresholds match the consensus. The 4.0–8.0 band is appropriate for active monitoring during exercise. |

---

### V-02 · Glucose drop rate and projected hypo time

**Evidence:** Riddell 2017 — hypo incidence ~44% during prolonged (≥60 min) continuous moderate-intensity aerobic exercise when no strategies are in place. Scott 2021 — check BG every 15 min immediately post-exercise due to elevated hypo risk.

| What the app does | Verdict |
|---|---|
| Computes mmol/L/h drop rate during the walk; projects time to 3.9 mmol/L at current trajectory; flags rapid (red) vs elevated (amber) drops. | Directly addresses the primary exercise-associated risk. Projection logic is clinically grounded. |

---

### V-03 · Pre-walk BG target assessment (7–10 mmol/L)

**Evidence:** Boom 2021 — glycaemic target set at 120 mg/dL (6.7 mmol/L), can be increased to 150 mg/dL (8.3 mmol/L) for up to 12 h. Zaharieva 2020 — closed-loop: start exercise with BG 7.0–10.0 mmol/L. Riddell 2017 — optimal pre-exercise range 7–10 mmol/L.

| What the app does | Verdict |
|---|---|
| Flags walk-start BG as optimal (7–10), low, or high; colours the risk profile accordingly. | Consistent with all three sources. The 7–10 mmol/L range is the right target for pump users in AUTO mode. |

---

### V-04 · Phase-by-phase BG slope (before / during / after)

**Evidence:** Scott 2021 — distinguishes peri-exercise (during), early recovery (0–2 h), and late recovery (2–12 h) windows as distinct glucose management phases with different risks and interventions.

| What the app does | Verdict |
|---|---|
| Computes BG slope in mmol/L/h for before, during, and post-walk phases and charts them as a bar comparison. | Phase decomposition matches the recovery paper's framework exactly. |

---

### V-05 · 6–12 h post-exercise recovery monitor

**Evidence:** Scott 2021 — basal insulin can be reduced 20–50% for 6–12 h post-exercise; nocturnal hypo risk is elevated throughout this window. Zaharieva 2020 — "basal insulin can be reduced by 20% overnight for 6 hours to combat nocturnal hypoglycemia."

| What the app does | Verdict |
|---|---|
| Shows windowed BG stats (min, mean, hypo count, slope) for early and late recovery phases from Carelink data. | Window selection and hypo flagging are well-grounded. The 6–12 h duration matches both sources. |

---

### V-06 · Early-recovery carb trigger (BG < 8.0 mmol/L)

**Evidence:** Scott 2021 — "Initiate carbohydrate feeding when the glucose concentration is less than 8·0 mmol/L (144 mg/dL)" in the first 2 h after exercise. Recommends 15–20 g fast-acting carbs.

| What the app does | Verdict |
|---|---|
| Flags "early-recovery carb trigger met" when BG drops below 8.0 mmol/L within 2 h of walk end; shows the Scott 2021 citation inline. | Exact threshold match to the source recommendation. |

---

### V-07 · Next walk: basal reduction 50–80% starting 90 min before

**Evidence:** Riddell 2017 — "Reduce basal insulin by 50–80% up to 90 min before the start of exercise until the exercise stops. Or consider pump suspension at the start of exercise (suspend insulin for no longer than 60 min)."

| What the app does | Verdict |
|---|---|
| Shows a computed basal_reduction_pct and timing in Next Walk Preparation, derived from walk intensity and duration. | Range and timing window match Riddell Table 2 exactly. |

---

### V-08 · Next walk: bolus reduction by exercise intensity

**Evidence:** Riddell 2017 Table 3 — "Reduce bolus insulin by 25–75% at the meal before exercise depending on the intensity of activity (25% for light, 50% for moderate, 75% for high intensity)."

| What the app does | Verdict |
|---|---|
| Shows bolus_reduction_pct in Next Walk Preparation, classified from the walk's HR zone distribution. | Percentages align with the Riddell table. Intensity classification from HR zones is a reasonable proxy. |

---

### V-09 · IOB decay model with effort-scaled acceleration

**Evidence:** Boom 2021 — "People with type 1 diabetes have to consider insulin on board after the last bolus insulin dose, and have to adjust the basal rate for a certain length of time and to a certain extent when exercising." Zaharieva 2020 — IOB is a primary risk factor at exercise onset for closed-loop users.

| What the app does | Verdict |
|---|---|
| Computes HR-effort-scaled IOB decay from bolus history; overlays active bolus curve on the glucose+insulin chart with SmartGuard suspension dampening. | The effort-acceleration model is novel but well-motivated. The SmartGuard suspend factor is an appropriate 780G-specific refinement. |

---

### V-10 · HR zone vs glucose stability breakdown

**Evidence:** Ski study 2017 — "Heart rate–based exercise detection has been shown to improve AP performance during exercise with decreased time <70 mg/dL." IHE paper 2024 — HR intensity (zone) predicts glycaemic response during exercise.

| What the app does | Verdict |
|---|---|
| Shows BG SD and BG slope per HR zone; surfaces dominant zone in the insight summary. | Directly addresses the HR–glucose coupling that both the ski study and IHE paper highlight. |

---

## New features

### N-01 · IOB at walk-start risk flag

**Evidence — Boom 2021 · Zaharieva 2020:** "People with type 1 diabetes have to consider insulin on board after the last bolus insulin dose." Zaharieva Table 3 — IOB is an explicit closed-loop pre-exercise risk factor. High IOB at exercise onset amplifies glucose drop risk independent of the basal setting.

**Gap:** The IOB decay model shows active insulin _during_ the walk, but the app never surfaces the IOB value _at the moment the walk started_ as a standalone risk signal.

**Implementation:** Compute estimated IOB at walk-start timestamp from bolus history. Show in the risk profile pre-walk row. Flag as elevated (amber) if > `1.0 U`, high (red) if > `2.0 U`.

_Status: implemented 2026-07-10_

---

### N-02 · First 30-min glucose drop rate

**Evidence — Riddell 2017 · IHE paper 2024:** Riddell 2017 — 44% hypo incidence during ≥60 min continuous moderate aerobic exercise. The early descent rate is the primary predictor of whether the episode reaches hypo. IHE 2024 — glucose drop attenuated by 1.47 mmol/L with intermittent high-intensity vs continuous moderate, measured in the first exercise phase.

**Gap:** The app shows a single per-hour drop rate averaged across the whole walk. The first 30-minute window has the highest predictive value and should be broken out.

**Implementation:** In the risk profile during-walk row, add a sub-row: "First 30 min: `X mmol/L/h` vs rest of walk: `Y mmol/L/h`". Flag if first-30-min rate exceeds `2 mmol/L/h`.

_Status: implemented 2026-07-11_

---

### N-03 · Intermittent high-intensity burst detection

**Evidence — IHE paper 2024:** "The exercise-induced decrease in capillary glycemia was attenuated by 1.47 mmol/L for IHE vs CME (P < 0.05)." Short bursts at 150% PWC170 interspersed in moderate-intensity exercise offer better protection against glycaemic drop than continuous moderate work of identical total load.

**Gap:** The app shows time in HR zones but doesn't detect the IHE pattern (HR spikes into zone 4–5 within a predominantly zone 2–3 walk) or annotate its glucose-protective effect.

**Implementation:** Detect HR excursions above `85% estimated HRmax` lasting 10–30 s within a predominantly zone 2–3 walk. Count bursts; show "X IHE bursts detected — associated with reduced glucose drop." Annotate on glucose chart with small markers.

_Status: implemented 2026-07-11 (burst count + risk profile row; chart annotation pending)_

---

### N-04 · Post-exercise hyperglycaemia detection and cool-down guidance

**Evidence — Scott 2021:** "If blood glucose concentrations are only slightly elevated (ie, 8–12 mmol/L) at the end of an exercise bout, it might be appropriate to do a low-intensity aerobic cool-down with the aim of gradually reducing blood glucose." Post-exercise hyperglycaemia can occur after aerobic exercise of moderate intensity due to pump suspension, reduced delivery, or catecholamine response.

**Gap:** The app flags BG < 8.0 as the carb trigger but never flags BG 8–12 at walk end as mild post-exercise hyperglycaemia or suggests a cool-down.

**Implementation:** In recovery monitor: if final-walk BG is `8–12 mmol/L`, show an amber note — "Mild post-exercise hyperglycaemia. Consider a 10–15 min low-intensity cool-down before stopping to reduce BG gradually." Cite Scott 2021.

_Status: implemented 2026-07-11_

---

### N-05 · 780G temp target recommendation (closed-loop specific)

**Evidence — Boom 2021 · Zaharieva 2020:** Boom 2021 — "The glycaemic target is set at 120 mg/dL and can be increased to 150 mg/dL for up to 12 h." Zaharieva 2020 — closed-loop exercise strategies differ from open-loop; raising the target set-point is the primary lever for pump users in AUTO mode, not manual basal reduction.

**Gap:** Next Walk Preparation gives generic basal/bolus advice applicable to MDI or open-loop. Geoffrey's 780G in AUTO mode — the actual lever is the temp target, not a manual basal edit.

**Implementation:** Add a 780G-specific panel in Next Walk Prep: "Set temp target to `8.3 mmol/L (150 mg/dL)` starting `90 min` before walk, for up to `12 h`. This replaces manual basal adjustment when in AUTO mode."

_Status: implemented 2026-07-10_

---

### N-06 · Nocturnal risk window end time (computed clock time)

**Evidence — Scott 2021 · Zaharieva 2020:** Scott 2021 — basal reduction recommended for 6–12 h post-exercise. Zaharieva 2020 — "basal insulin can be reduced by 20% overnight for 6 hours to combat nocturnal hypoglycemia." Both papers treat this as a time-bounded elevated-risk window, not a vague advisory.

**Gap:** Recovery monitor shows "nocturnal hypo risk persists 6–12h" as static prose. It never computes the actual clock time when the window closes for this specific walk.

**Implementation:** From walk end timestamp, compute and display: "Elevated risk window: `[end time]` → `[end + 12 h]`" (e.g. 3:45 pm → 3:45 am). Show as a header chip in the Recovery Monitor card.

_Status: implemented 2026-07-11_

---

## Enhancements to existing features

### E-01 · Walk-start closed-loop contraindication check

**Evidence — Zaharieva 2020 Table 3:** Closed-loop contraindications before exercise: significant hyperglycaemia (>15.0 mmol/L), ketones (≥1.5 mmol/L), recent severe hypoglycaemia (loss of consciousness / seizure / inability to self-treat). These are hard stops, not just risk flags.

**Current behaviour:** Pre-walk BG is shown and rated against the 7–10 mmol/L optimal range. BG above 10 shows as "above target" but there's no distinct flag for the `15.0 mmol/L` closed-loop contraindication threshold.

**Enhancement:** Add a red "CL contraindication" state in the pre-walk BG row when start BG > `15.0 mmol/L`: "Do not start walk until BG is reduced. Closed-loop systems should not be used for exercise above this threshold."

_Status: implemented 2026-07-11_

---

### E-02 · Riddell intensity-reduction table alongside computed recommendation

**Evidence — Riddell 2017 Table 3:** Light exercise: reduce pre-meal bolus 25%. Moderate: 50%. High intensity: 75%. Bedtime snack of 0.4 g carbohydrate/kg body weight recommended after low-to-moderate activity to prevent nocturnal hypo.

**Current behaviour:** Next Walk Prep shows a computed bolus_reduction_pct but gives no context for why that number was chosen or where it sits relative to the source recommendation.

**Enhancement:** Show the three-row Riddell table (Light 25% / Moderate 50% / High 75%) with the relevant row highlighted based on the walk's classified intensity. Add the bedtime-snack guidance for low-to-moderate walks.

_Status: implemented 2026-07-11 (weight_kg not stored — bedtime snack shows formula 0.4 g/kg without computed grams)_

---

### E-03 · Recovery carb quality note (multi-transportable carbs)

**Evidence — Scott 2021:** "The use of multiple transportable carbohydrates (eg, fructose) in combination with glucose after exercise will promote a faster liver glycogen restoration." This is relevant when carbs are already being taken — the type of carb matters for recovery speed, not just the quantity.

**Current behaviour:** Next Walk Prep recommends "15–20 g fast carbs" post-walk. Recovery monitor advises carbs when BG < 8.0. Neither gives guidance on carb type.

**Enhancement:** Append to the carb guidance: "Mixed glucose + fructose sources (sports drinks, fruit) restore liver glycogen faster than pure glucose for the same gram count." One sentence, inline with existing text.

_Status: implemented 2026-07-11_

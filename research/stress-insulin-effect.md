# Stress performance on insulin effect

## Goal
Estimate how exercise stress may alter effective bolus insulin action during a walk, using available Garmin FIT + CareLink + weather signals.

This is an analysis aid and trend signal, not a dosing recommendation.

## Literature snapshot (starting set)

The following are useful anchors for model assumptions and validation design:

1. Riddell et al., Diabetes Care (2023), T1DEXI real-world exercise glycemic effects, PMID: 36795053.
2. McClure et al., Can J Diabetes (2023), systematic review/meta-analysis for high-intensity interval exercise glycemic response in T1D, PMID: 36549943.
3. Lee et al., J Diabetes Complications (2020), high-intensity interval exercise and hypoglycemia minimization in adults with T1D, PMID: 31918984.
4. Moser et al., Nutrients (2016), CGM behavior during continuous vs HIIT in T1D, PMID: 27517956.
5. Fabris et al., Diabetes Care (2020), smart bolus calculator using real-time insulin sensitivity around exercise, PMID: 32144167.
6. Jimenez et al., J Sport Rehabil (2009), insulin sensitivity response after resistive exercise in T1D, PMID: 20108856.

## Why these papers matter for this app

1. Exercise modality changes glycemic direction and rate.
2. Intensity can attenuate or reverse immediate glucose drop in some sessions.
3. Aerobic load with insulin on board often increases hypoglycemia risk.
4. Real-time insulin sensitivity adaptation is feasible in closed-loop support logic.

## Available proxy signals in Walkies data

From FIT:
1. Heart rate (current intensity and sustained load).
2. Grade proxy from altitude + distance.
3. Distance/time structure for overlap windows.

From CareLink:
1. Bolus timing and units.
2. Glucose trajectory for outcome linkage.

From weather:
1. Apparent temperature as additional stress load.

## Implemented heuristic in backend

Walkies now computes a non-diagnostic insulin stress effect summary per walk:

1. Build an IOB proxy from each bolus with linear decay over 4 hours.
2. For each activity segment with non-trivial IOB:
   - HR load from ~95 bpm to a 175 bpm reference.
   - Uphill load from positive grade.
   - Heat load from apparent temperature above 18 C.
3. Build stress index:

$$
\text{stress\_index} = \mathrm{clip}\left(1 + 0.45\,\text{hr\_load} + 0.20\,\text{uphill\_load} + 0.10\,\text{heat\_load},\ 0.8,\ 1.9\right)
$$

4. Weight by IOB (saturating at 3 U) and aggregate a walk-level multiplier:

$$
\text{insulin\_stress\_multiplier} = \frac{\sum (\Delta t\cdot w_{IOB}\cdot \text{stress\_index})}{\sum (\Delta t\cdot w_{IOB})}
$$

Outputs:
1. stress_multiplier (x)
2. band (Low/Moderate/High/Very High)
3. overlap_minutes (minutes with active IOB proxy)
4. weighted_overlap_minutes
5. peak_iob_units_proxy
6. peak_stress_index

## Validation plan

1. Per-walk calibration:
   - Compare multiplier vs during-walk BG slope and hypo events.
2. Cross-walk calibration:
   - Regress BG drop per hour on multiplier, start BG, and bolus units.
3. Segment-level validation:
   - Evaluate whether high index segments predict 20-40 min subsequent BG decline.
4. Sensitivity analysis:
   - Sweep HR, grade, heat coefficients and choose stable settings by out-of-sample error.

## Next model upgrades

1. Replace linear IOB proxy with selected insulin profile kinetics.
2. Include lagged effects (30-120 min) explicitly.
3. Distinguish aerobic steady-state vs anaerobic intervals via HR acceleration and pace variance.
4. Add confidence score based on BG sampling density and IOB overlap coverage.

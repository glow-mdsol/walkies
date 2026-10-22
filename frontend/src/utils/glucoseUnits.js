/**
 * Canonical glucose unit conversion and formatting utilities.
 * Internal storage and backend analytics use canonical mmol/L.
 * 1 mmol/L = 18.0182 mg/dL
 */

export const MGDL_PER_MMOL = 18.0182;

export function normalizeGlucoseUnit(unit) {
  if (!unit) return 'mmol/L';
  const u = String(unit).trim().toLowerCase();
  if (u === 'mg/dl' || u === 'mgdl') return 'mg/dL';
  return 'mmol/L';
}

export function mmolToDisplay(valMmol, unit = 'mmol/L') {
  if (valMmol === null || valMmol === undefined || !Number.isFinite(Number(valMmol))) {
    return null;
  }
  const num = Number(valMmol);
  const normUnit = normalizeGlucoseUnit(unit);
  if (normUnit === 'mg/dL') {
    return Math.round(num * MGDL_PER_MMOL);
  }
  return Number(num.toFixed(1));
}

export function mmolDeltaToDisplay(deltaMmol, unit = 'mmol/L') {
  if (deltaMmol === null || deltaMmol === undefined || !Number.isFinite(Number(deltaMmol))) {
    return null;
  }
  const num = Number(deltaMmol);
  const normUnit = normalizeGlucoseUnit(unit);
  if (normUnit === 'mg/dL') {
    return Math.round(num * MGDL_PER_MMOL);
  }
  return Number(num.toFixed(2));
}

export function displayToMmol(displayVal, unit = 'mmol/L') {
  if (displayVal === null || displayVal === undefined || displayVal === '' || !Number.isFinite(Number(displayVal))) {
    return null;
  }
  const num = Number(displayVal);
  const normUnit = normalizeGlucoseUnit(unit);
  if (normUnit === 'mg/dL') {
    return Number((num / MGDL_PER_MMOL).toFixed(2));
  }
  return Number(num.toFixed(2));
}

export function formatGlucose(valMmol, unit = 'mmol/L', includeUnit = false) {
  const converted = mmolToDisplay(valMmol, unit);
  if (converted === null) return '–';
  const normUnit = normalizeGlucoseUnit(unit);
  const formatted = normUnit === 'mg/dL' ? String(Math.round(converted)) : Number(converted).toFixed(1);
  return includeUnit ? `${formatted} ${normUnit}` : formatted;
}

export function formatGlucoseDelta(deltaMmol, unit = 'mmol/L', includeUnit = false) {
  const converted = mmolDeltaToDisplay(deltaMmol, unit);
  if (converted === null) return '–';
  const normUnit = normalizeGlucoseUnit(unit);
  const sign = converted > 0 ? '+' : '';
  const formatted = normUnit === 'mg/dL' ? `${sign}${Math.round(converted)}` : `${sign}${Number(converted).toFixed(1)}`;
  return includeUnit ? `${formatted} ${normUnit}` : formatted;
}

export function formatGlucoseSlope(slopeMmolPerHour, unit = 'mmol/L', includeUnit = true) {
  const converted = mmolDeltaToDisplay(slopeMmolPerHour, unit);
  if (converted === null) return '–';
  const normUnit = normalizeGlucoseUnit(unit);
  const sign = converted > 0 ? '+' : '';
  const formatted = normUnit === 'mg/dL' ? `${sign}${Math.round(converted)}` : `${sign}${Number(converted).toFixed(1)}`;
  return includeUnit ? `${formatted} ${normUnit}/h` : formatted;
}

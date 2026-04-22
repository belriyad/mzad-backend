"""
car_valuator.py v5.0 — Segmented log-price model with trim + km_age_product
  Classifier accuracy : 94.74%
  Budget  (≤120k QAR): R²=0.81  RMSE=12,578  MAPE=23.3%
  Premium (>120k QAR): R²=0.62  RMSE=62,698  MAPE=15.7%
"""
from __future__ import annotations
import json
from pathlib import Path
import joblib
import numpy as np
import pandas as pd

_DIR          = Path(__file__).parent
_CLF_PATH     = _DIR / "car_segment_clf.pkl"
_BUDGET_PATH  = _DIR / "car_valuator_budget.pkl"
_PREMIUM_PATH = _DIR / "car_valuator_premium.pkl"
_META_PATH    = _DIR / "model_meta.json"

_clf     = None
_budget  = None
_premium = None
_meta    = None

# Post-prediction fence: cars with very high age×km product are
# almost certainly not worth premium prices regardless of make/class.
_KM_AGE_PREMIUM_FENCE = 2_500_000   # e.g. 17yr × 150k km
_KM_ABSOLUTE_MAX      = 300_000     # beyond this km, always budget


def _load():
    global _clf, _budget, _premium, _meta
    if _clf is None:
        _clf     = joblib.load(_CLF_PATH)
        _budget  = joblib.load(_BUDGET_PATH)
        _premium = joblib.load(_PREMIUM_PATH)
        with open(_META_PATH) as f:
            _meta = json.load(f)


def estimate_price(features: dict) -> dict:
    """
    Estimate car price from any subset of known features.

    Args:
        features: dict with any of:
            make, class_name, trim, fuel_type, gear_type, car_type,
            city, warranty_status, seller_type   (strings)
            manufacture_year, km, cylinder_count (numbers)

    Returns:
        {
            "estimated_price_qar": float,
            "confidence_range": [low_qar, high_qar],
            "segment": "budget" | "premium",
            "model_version": str,
            "r2": float,
            "mape": float,
        }
    """
    _load()
    numeric   = _meta["features_numeric"]
    categoric = _meta["features_categorical"]

    # ── Build input row ──────────────────────────────────────────────────────
    row: dict = {}

    yr = features.get("manufacture_year")
    try:
        car_age = 2026 - int(yr) if yr is not None else None
    except (TypeError, ValueError):
        car_age = None
    row["car_age"] = car_age

    km_val = None
    for col in ["km", "cylinder_count"]:
        v = features.get(col)
        try:
            row[col] = float(v) if v is not None else None
        except (TypeError, ValueError):
            row[col] = None
        if col == "km":
            km_val = row[col]

    # Derived interaction feature
    if car_age is not None and km_val is not None:
        row["km_age_product"] = float(km_val) * float(car_age)
    else:
        row["km_age_product"] = None

    for col in categoric:
        v = features.get(col)
        row[col] = str(v).strip() if v and str(v).strip() else None

    df = pd.DataFrame([row], columns=numeric + categoric)
    df.columns = numeric + categoric  # ensure named columns for LightGBM

    # ── Classify segment ─────────────────────────────────────────────────────
    is_premium = int(_clf.predict(df)[0])

    # Post-prediction fence: override to budget for worn-out cars
    km_age = row.get("km_age_product")
    if is_premium and (
        (km_age is not None and km_age > _KM_AGE_PREMIUM_FENCE) or
        (km_val is not None and km_val > _KM_ABSOLUTE_MAX)
    ):
        is_premium = 0

    segment  = "premium" if is_premium else "budget"
    pipeline = _premium if is_premium else _budget
    seg_meta = _meta["segments"][segment]

    # ── Predict (log-space, convert back) ────────────────────────────────────
    log_pred  = float(pipeline.predict(df)[0])
    predicted = float(np.expm1(log_pred))
    predicted = max(1000.0, predicted)

    # Confidence range: ±(MAPE/2 × predicted).
    # MAPE is the mean absolute percentage error, so ±half gives a tight
    # "likely" band proportional to price — much tighter than ±MAE.
    mape_pct   = seg_meta["mape"]
    half_range = predicted * (mape_pct / 100.0) * 0.5
    return {
        "estimated_price_qar": round(predicted, 0),
        "confidence_range":    [round(max(0.0, predicted - half_range), 0),
                                 round(predicted + half_range, 0)],
        "segment":             segment,
        "model_version":       _meta["model_version"],
        "r2":                  round(seg_meta["r2"], 4),
        "mape":                round(seg_meta["mape"], 2),
    }


if __name__ == "__main__":
    tests = [
        # Trim differentiation
        {"make": "Toyota",   "class_name": "Land Cruiser", "trim": "GXR", "manufacture_year": 2020, "km": 80000},
        {"make": "Toyota",   "class_name": "Land Cruiser", "trim": "VXR", "manufacture_year": 2020, "km": 80000},
        {"make": "Nissan",   "class_name": "Patrol",       "trim": "SE",        "manufacture_year": 2019, "km": 60000},
        {"make": "Nissan",   "class_name": "Patrol",       "trim": "Platinum",  "manufacture_year": 2019, "km": 60000},
        # Old high-km (should be budget, not premium)
        {"make": "Mercedes", "class_name": "E-Class",      "manufacture_year": 2005, "km": 260000},
        {"make": "GMC",      "class_name": "Denali NY",    "manufacture_year": 2009, "km": 260000},
        {"make": "Land Rover","class_name": "Discovery",   "manufacture_year": 2002, "km": 347000},
        # Normal cases
        {"make": "Toyota",   "class_name": "Camry",        "manufacture_year": 2021, "km": 50000},
        {"make": "Honda",    "class_name": "Civic",        "manufacture_year": 2019, "km": 90000},
        {"make": "Mercedes", "class_name": "E-Class",      "manufacture_year": 2022, "km": 25000},
    ]
    print(f"{'Make':<12} {'Model':<14} {'Trim':<12} {'Yr':<5} {'KM':>8}  {'Estimate':>12}  {'Seg'}")
    print("-" * 80)
    for t in tests:
        r = estimate_price(t)
        print(f"{t['make']:<12} {t['class_name']:<14} {t.get('trim',''):<12} "
              f"{t['manufacture_year']:<5} {t.get('km',0):>8,}  "
              f"{int(r['estimated_price_qar']):>12,}  {r['segment']}")

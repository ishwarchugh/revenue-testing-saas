from __future__ import annotations

from typing import Literal

import datetime as dt
import math
import numpy as np
import pandas as pd
from uuid import uuid4


RiskLevel = Literal["significant", "higher", "lower"]
ControlRiskLevel = Literal["higher", "lower"]
SAPLevel = Literal["none", "minimal", "conservative", "persuasive"]
ConfidenceLevel = Literal[80, 85, 90, 95]


def _mus_ria_pct(
    inherent_risk: RiskLevel,
    control_risk: ControlRiskLevel,
    sap_level: SAPLevel,
) -> float:
    """
    Risk of Incorrect Acceptance (RIA) expressed as a percentage.

    Base RIA is determined by inherent + control risk, then adjusted upward
    (i.e., higher RIA) based on SAP level.
    """
    ir = str(inherent_risk).strip().lower()
    cr = str(control_risk).strip().lower()
    sap = str(sap_level).strip().lower()

    if ir not in {"significant", "higher", "lower"}:
        raise ValueError(
            "Invalid inherent_risk. Expected one of: 'significant', 'higher', 'lower'."
        )
    if cr not in {"higher", "lower"}:
        raise ValueError("Invalid control_risk. Expected one of: 'higher', 'lower'.")
    if sap not in {"none", "minimal", "conservative", "persuasive"}:
        raise ValueError(
            "Invalid sap_level. Expected one of: 'none', 'minimal', 'conservative', 'persuasive'."
        )

    base_ria_by_ir_cr: dict[tuple[str, str], float] = {
        ("significant", "higher"): 5.0,
        ("significant", "lower"): 10.0,
        ("higher", "higher"): 10.0,
        ("higher", "lower"): 15.0,
        ("lower", "higher"): 15.0,
        ("lower", "lower"): 25.0,
    }
    base = base_ria_by_ir_cr[(ir, cr)]

    sap_adj_pct: dict[str, float] = {
        "none": 0.0,
        "minimal": 5.0,
        "conservative": 10.0,
        "persuasive": 20.0,
    }
    return base + sap_adj_pct[sap]


def _mus_confidence_factor(ria_pct: float) -> float:
    """
    Confidence Factor lookup for *zero expected misstatements* (Poisson-based).

    Uses the AICPA/CaseWare-style lookup table. Values in between thresholds
    use the next-highest RIA bracket; values above 37% use the 37% factor.
    """
    ria = float(ria_pct)
    if not np.isfinite(ria) or ria <= 0:
        raise ValueError("RIA% must be a positive finite number.")

    # Thresholds are (max_RIA_pct, confidence_factor)
    table: list[tuple[float, float]] = [
        (5.0, 3.00),
        (10.0, 2.31),
        (15.0, 1.90),
        (20.0, 1.61),
        (25.0, 1.38),
        (30.0, 1.21),
        (37.0, 1.00),
    ]

    for max_ria, cf in table:
        if ria <= max_ria:
            return cf
    return 1.00


def mus_sample_size_parameters(
    *,
    population_value: float,
    performance_materiality: float,
    inherent_risk: RiskLevel,
    control_risk: ControlRiskLevel,
    sap_level: SAPLevel,
) -> dict[str, float | int]:
    """
    Industry-standard MUS sample size parameters (zero expected misstatements).

    Sample Size = CEILING((Population Value × Confidence Factor) / Tolerable Misstatement)
    Tolerable Misstatement = Performance Materiality (input)
    """
    pv = float(population_value)
    tm = float(performance_materiality)
    if not np.isfinite(tm) or tm <= 0:
        raise ValueError("performance_materiality must be a positive number.")
    if not np.isfinite(pv) or pv <= 0:
        return {"ria_pct": 0.0, "confidence_factor": 0.0, "sample_size": 0}

    ria_pct = _mus_ria_pct(inherent_risk, control_risk, sap_level)
    confidence_factor = _mus_confidence_factor(ria_pct)
    sample_size = int(math.ceil((pv * confidence_factor) / tm))
    sample_size = max(sample_size, 1)
    return {
        "ria_pct": float(ria_pct),
        "confidence_factor": float(confidence_factor),
        "sample_size": int(sample_size),
    }


def target_testing(
    gl_transactions: pd.DataFrame,
    performance_materiality: float,
    risk_level: RiskLevel,
) -> pd.DataFrame:
    """
    Return all GL transactions where `amount` exceeds a threshold.

    Threshold = Performance Materiality × Risk percentage
      - significant risk: 10%
      - higher risk: 25%
      - lower risk: 50%
    """
    risk_pct_by_level: dict[str, float] = {
        "significant": 0.10,
        "higher": 0.25,
        "lower": 0.50,
    }

    level = str(risk_level).strip().lower()
    if level not in risk_pct_by_level:
        raise ValueError(
            "Invalid risk_level. Expected one of: 'significant', 'higher', 'lower'."
        )
    if "amount" not in gl_transactions.columns:
        raise ValueError("gl_transactions must include an 'amount' column.")

    threshold = float(performance_materiality) * risk_pct_by_level[level]

    df = gl_transactions.copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df.loc[df["amount"] > threshold]


def mus_sampling(
    gl_transactions: pd.DataFrame,
    performance_materiality: float,
    inherent_risk: RiskLevel,
    control_risk: ControlRiskLevel,
    sap_level: SAPLevel,
    confidence_level: ConfidenceLevel,
    *,
    exclude_invoice_numbers: set[str] | None = None,
    invoice_col: str = "invoice_number",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Monetary Unit Sampling (MUS) selection using positive amounts only.

    Methodology note:
      When target testing is performed, MUS should typically be performed on the
      residual population (i.e., total population less target-tested items).
      You can enforce that by passing a residual `gl_transactions`, or by
      providing `exclude_invoice_numbers` to filter target-tested items out here.

    Sample size formula:
      CEILING((Population Value × Confidence Factor) / Tolerable Misstatement)

    Notes:
      - Tolerable Misstatement = Performance Materiality (input)
      - Confidence Factor is determined from RIA% using a Poisson-based lookup table
        assuming zero expected misstatements.
      - RIA% is determined from Inherent Risk + Control Risk, adjusted upward by SAP.

    Selection method:
      - interval = Population Value / Sample Size
      - pick a random start in [0, interval) using a fixed seed
      - select items where the cumulative amount crosses each interval point
    """
    if "amount" not in gl_transactions.columns:
        raise ValueError("gl_transactions must include an 'amount' column.")

    pm = float(performance_materiality)
    if not np.isfinite(pm) or pm <= 0:
        raise ValueError("performance_materiality must be a positive number.")

    df = gl_transactions.copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if exclude_invoice_numbers:
        if invoice_col not in df.columns:
            raise ValueError(
                f"invoice_col '{invoice_col}' not found; cannot exclude target-tested items."
            )
        exclude = {str(x) for x in exclude_invoice_numbers}
        df = df.loc[~df[invoice_col].astype(str).isin(exclude)].copy()

    population_df = df.loc[df["amount"] > 0].copy()
    population_value = float(population_df["amount"].sum(skipna=True))

    if not np.isfinite(population_value) or population_value <= 0:
        out = population_df.iloc[0:0].copy()
        out["run_id"] = str(uuid4())
        out["seed"] = int(seed)
        return out

    params = mus_sample_size_parameters(
        population_value=population_value,
        performance_materiality=pm,
        inherent_risk=inherent_risk,
        control_risk=control_risk,
        sap_level=sap_level,
    )
    sample_size = int(params["sample_size"])

    interval = population_value / sample_size
    rng = np.random.default_rng(int(seed))
    start = float(rng.uniform(0, interval))
    points = start + interval * np.arange(sample_size)

    cumulative = population_df["amount"].cumsum().to_numpy()
    selected_pos_idx = np.searchsorted(cumulative, points, side="left")
    selected_pos_idx = np.unique(np.clip(selected_pos_idx, 0, len(population_df) - 1))

    run_id = str(uuid4())
    selected = population_df.iloc[selected_pos_idx].copy()
    selected["run_id"] = run_id
    selected["seed"] = int(seed)
    return selected


def cutoff_testing(
    gl_transactions: pd.DataFrame,
    cutoff_date: dt.date,
    date_column: str,
) -> pd.DataFrame:
    """
    Select up to 5 transactions immediately before and after a cutoff date.
    """
    if date_column not in gl_transactions.columns:
        raise ValueError(f"gl_transactions must include '{date_column}' column.")

    df = gl_transactions.copy()
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    cutoff_ts = pd.Timestamp(cutoff_date)

    pre = (
        df.loc[df[date_column] < cutoff_ts]
        .sort_values(date_column, ascending=False)
        .head(5)
        .copy()
    )
    pre["cutoff_position"] = "pre"

    post = (
        df.loc[df[date_column] > cutoff_ts]
        .sort_values(date_column, ascending=True)
        .head(5)
        .copy()
    )
    post["cutoff_position"] = "post"

    return pd.concat([pre, post], ignore_index=True)


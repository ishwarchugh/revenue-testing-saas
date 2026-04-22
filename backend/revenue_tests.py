from __future__ import annotations

from typing import Literal

import pandas as pd


RiskLevel = Literal["significant", "higher", "lower"]


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


def mus_sampling() -> dict:
    """
    Placeholder for Monetary Unit Sampling (MUS) selection logic.
    """
    return {
        "name": "mus_sampling",
        "status": "not_implemented",
        "message": "Placeholder function. Implement MUS sampling logic later.",
    }


def cutoff_testing() -> dict:
    """
    Placeholder for cutoff testing logic (period-end revenue checks).
    """
    return {
        "name": "cutoff_testing",
        "status": "not_implemented",
        "message": "Placeholder function. Implement cutoff testing logic later.",
    }


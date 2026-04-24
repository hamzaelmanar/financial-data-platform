"""
LP Survival Analysis
─────────────────────
Reads from the fct_lp_positions mart (PostgreSQL) and runs:
  - Kaplan-Meier survival curves (all LPs + segmented by campaign entry)
  - Log-rank test for statistical significance of the campaign effect
  - Exit time distribution histogram

Replaces the notebook-style analysis. No raw data dependencies — reads
exclusively from the dbt mart output.

Usage:
    python analysis/lp_survival.py
    python analysis/lp_survival.py --pool 0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c
"""

import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test
from sqlalchemy import create_engine, text

load_dotenv()


def _get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(url, future=True)


def load_lp_positions(pool_address: str | None = None) -> pd.DataFrame:
    engine = _get_engine()
    where = "WHERE pool_address = :pool" if pool_address else ""
    query = text(f"SELECT * FROM marts.fct_lp_positions {where}")
    params = {"pool": pool_address.lower()} if pool_address else {}
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    df["duration_days"] = df["duration_seconds"].astype(float) / 86400
    return df


def km_all(lp: pd.DataFrame) -> KaplanMeierFitter:
    kmf = KaplanMeierFitter()
    kmf.fit(
        durations=lp["duration_days"],
        event_observed=lp["status"],
        label="All LPs",
    )

    plt.figure(figsize=(12, 7))
    kmf.plot_survival_function(at_risk_counts=True)
    plt.axvline(
        kmf.median_survival_time_,
        color="red", linestyle=":", alpha=0.5, label="Median survival"
    )
    plt.title("Kaplan-Meier Survival Curve — All LPs", fontsize=14)
    plt.xlabel("Duration (days)")
    plt.ylabel("Probability of still providing liquidity")
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()
    return kmf


def km_segmented(lp: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 7))

    cohort_labels = {
        "pre_campaign":    ("Pre-campaign",    "C0"),
        "during_campaign": ("During campaign", "C1"),
        "post_campaign":   ("Post-campaign",   "C2"),
    }

    results = {}
    for cohort, (label, color) in cohort_labels.items():
        mask = lp["lp_cohort"] == cohort
        if mask.sum() == 0:
            continue
        kmf = KaplanMeierFitter()
        kmf.fit(
            durations=lp.loc[mask, "duration_days"],
            event_observed=lp.loc[mask, "status"],
            label=f"{label} (n={mask.sum()})",
        )
        kmf.plot_survival_function(ci_show=True, color=color)
        results[cohort] = lp[mask]

    plt.title("Survival Curves by LP cohort: pre / during / post campaign", fontsize=14)
    plt.xlabel("Duration (days)")
    plt.ylabel("Survival Probability")
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Log-rank test: during vs. pre (direct campaign effect)
    during = results.get("during_campaign")
    pre    = results.get("pre_campaign")
    if during is not None and pre is not None and len(during) > 0 and len(pre) > 0:
        result = logrank_test(
            durations_A=during["duration_days"],
            durations_B=pre["duration_days"],
            event_observed_A=during["status"],
            event_observed_B=pre["status"],
        )
        print("\n── Log-rank: during_campaign vs. pre_campaign ─────────────────")
        print(f"  Test statistic : {result.test_statistic:.4f}")
        print(f"  p-value        : {result.p_value:.4f}")
        if result.p_value < 0.05:
            print("  → Significant (p < 0.05): campaign-entrant LPs behaved differently.")
        else:
            print("  → Not significant (p ≥ 0.05).")

    # Log-rank test: post vs. pre (indirect stickiness signal)
    post = results.get("post_campaign")
    if post is not None and pre is not None and len(post) > 0 and len(pre) > 0:
        result2 = logrank_test(
            durations_A=post["duration_days"],
            durations_B=pre["duration_days"],
            event_observed_A=post["status"],
            event_observed_B=pre["status"],
        )
        print("\n── Log-rank: post_campaign vs. pre_campaign ────────────────────")
        print(f"  Test statistic : {result2.test_statistic:.4f}")
        print(f"  p-value        : {result2.p_value:.4f}")
        if result2.p_value < 0.05:
            print("  → Significant: post-campaign pool attracted durably stickier LPs.")
        else:
            print("  → Not significant: no durable indirect stickiness effect detected.")


def exit_distribution(lp: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 5))
    exited = lp[lp["status"] == 1]["duration_days"]
    plt.hist(exited, bins=30, alpha=0.7, edgecolor="black")
    plt.xlabel("Days to exit")
    plt.ylabel("Number of LPs")
    plt.title("Distribution of Exit Times (exited LPs only)")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.show()


def main() -> None:
    parser = argparse.ArgumentParser(description="LP survival analysis")
    parser.add_argument("--pool", default=None, help="Filter to a specific pool address")
    args = parser.parse_args()

    lp = load_lp_positions(pool_address=args.pool)
    print(f"Loaded {len(lp)} LP records.")
    print(f"  Exited : {lp['status'].sum()}")
    print(f"  Active : {(lp['status'] == 0).sum()}")

    km_all(lp)
    km_segmented(lp)
    exit_distribution(lp)


if __name__ == "__main__":
    main()

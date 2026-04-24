"""
LP Retention Dashboard
───────────────────────
Dash app with three Plotly figures ported from analysis/lp_survival.py:

  Tab 1 — Kaplan-Meier curve, all LPs
  Tab 2 — KM curves segmented by campaign cohort + log-rank test results
  Tab 3 — Exit-time histogram (exited LPs only)

Run locally:
    python dashboard/app.py

In Docker (via docker-compose):
    docker compose up dashboard
"""

import os

import psycopg2
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dotenv import load_dotenv
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test

load_dotenv()

# ── palette ──────────────────────────────────────────────────────────────────
COHORTS = {
    "pre_campaign":    ("Pre-campaign (baseline)",           "#636EFA", "rgba(99,110,250,0.15)"),
    "during_campaign": ("During campaign",                   "#EF553B", "rgba(239,85,59,0.15)"),
    "post_campaign":   ("Post-campaign (stickiness signal)", "#00CC96", "rgba(0,204,150,0.15)"),
}

PLOT_LAYOUT = dict(
    paper_bgcolor="#0f1117",
    plot_bgcolor="#0f1117",
    font=dict(color="#e8e8e8", family="Inter, sans-serif"),
    xaxis=dict(gridcolor="#2a2a3a", zerolinecolor="#2a2a3a"),
    yaxis=dict(gridcolor="#2a2a3a", zerolinecolor="#2a2a3a", range=[0, 1.05]),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    hovermode="x unified",
    margin=dict(l=60, r=30, t=60, b=60),
)


# ── data ─────────────────────────────────────────────────────────────────────
def _connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def _load() -> pd.DataFrame:
    with _connect() as conn:
        df = pd.read_sql("SELECT * FROM marts.fct_lp_positions", conn)
    df["duration_days"] = df["duration_seconds"].astype(float) / 86400
    return df


# ── KM helpers ───────────────────────────────────────────────────────────────
def _km_traces(kmf: KaplanMeierFitter, name: str, color: str, fill_color: str):
    """Return Plotly traces for a KM step line + shaded CI band."""
    sf = kmf.survival_function_
    ci = kmf.confidence_interval_

    lower_col = next(c for c in ci.columns if "lower" in c)
    upper_col = next(c for c in ci.columns if "upper" in c)

    traces = [
        # CI upper boundary (invisible line, used as fill ceiling)
        go.Scatter(
            x=list(ci.index),
            y=list(ci[upper_col]),
            mode="lines",
            line=dict(width=0, shape="hv"),
            showlegend=False,
            hoverinfo="skip",
        ),
        # CI lower boundary fills up to the upper boundary
        go.Scatter(
            x=list(ci.index),
            y=list(ci[lower_col]),
            mode="lines",
            fill="tonexty",
            fillcolor=fill_color,
            line=dict(width=0, shape="hv"),
            showlegend=False,
            hoverinfo="skip",
        ),
        # Survival step line
        go.Scatter(
            x=list(sf.index),
            y=list(sf.iloc[:, 0]),
            mode="lines",
            name=name,
            line=dict(color=color, width=2, shape="hv"),
        ),
    ]
    return traces


# ── Figure 1: All LPs ────────────────────────────────────────────────────────
def _fig_km_all(lp: pd.DataFrame) -> go.Figure:
    kmf = KaplanMeierFitter()
    kmf.fit(
        durations=lp["duration_days"],
        event_observed=lp["status"],
        label="All LPs",
    )

    fig = go.Figure()
    for t in _km_traces(kmf, "All LPs", "#636EFA", "rgba(99,110,250,0.15)"):
        fig.add_trace(t)

    median = kmf.median_survival_time_
    if pd.notna(median):
        fig.add_vline(
            x=median,
            line_dash="dot",
            line_color="rgba(255,80,80,0.6)",
            annotation_text=f"Median: {median:.1f}d",
            annotation_position="top right",
            annotation_font_color="#ff8080",
        )

    fig.update_layout(
        title="Kaplan-Meier Survival Curve — All LPs",
        xaxis_title="Duration (days)",
        yaxis_title="P(still providing liquidity)",
        **PLOT_LAYOUT,
    )
    return fig


# ── Figure 2: Cohort comparison ───────────────────────────────────────────────
def _fig_km_cohorts(lp: pd.DataFrame) -> tuple[go.Figure, dict]:
    fig = go.Figure()
    fitted: dict[str, pd.DataFrame] = {}

    for cohort, (label, color, fill) in COHORTS.items():
        mask = lp["lp_cohort"] == cohort
        if mask.sum() == 0:
            continue
        subset = lp[mask]
        kmf = KaplanMeierFitter()
        kmf.fit(
            durations=subset["duration_days"],
            event_observed=subset["status"],
            label=f"{label} (n={mask.sum()})",
        )
        for t in _km_traces(kmf, f"{label} (n={mask.sum()})", color, fill):
            fig.add_trace(t)
        fitted[cohort] = subset

    fig.update_layout(
        title="Survival by LP Cohort: pre / during / post campaign",
        xaxis_title="Duration (days)",
        yaxis_title="Survival Probability",
        **PLOT_LAYOUT,
    )
    return fig, fitted


def _logrank_cards(fitted: dict[str, pd.DataFrame]) -> list:
    """Build HTML blocks for both log-rank tests."""
    cards = []
    pairs = [
        ("during_campaign", "pre_campaign",  "During vs. Pre  (direct campaign effect)"),
        ("post_campaign",   "pre_campaign",  "Post vs. Pre  (durable stickiness signal)"),
    ]
    for a_key, b_key, title in pairs:
        a = fitted.get(a_key)
        b = fitted.get(b_key)
        if a is None or b is None or len(a) == 0 or len(b) == 0:
            continue
        res = logrank_test(
            durations_A=a["duration_days"],
            durations_B=b["duration_days"],
            event_observed_A=a["status"],
            event_observed_B=b["status"],
        )
        significant = res.p_value < 0.05
        verdict_color = "#00CC96" if significant else "#aaaaaa"
        verdict = "Significant (p < 0.05)" if significant else "Not significant (p ≥ 0.05)"
        cards.append(
            html.Div(
                style={
                    "background": "#1a1a2e",
                    "border": f"1px solid {verdict_color}",
                    "borderRadius": "8px",
                    "padding": "16px 20px",
                    "marginTop": "12px",
                    "fontFamily": "Inter, monospace",
                },
                children=[
                    html.P(title, style={"margin": "0 0 8px 0", "color": "#aaaaaa", "fontSize": "13px"}),
                    html.P(
                        f"Test statistic: {res.test_statistic:.4f}   |   p-value: {res.p_value:.4f}",
                        style={"margin": "0 0 4px 0", "fontSize": "15px"},
                    ),
                    html.P(
                        f"→ {verdict}",
                        style={"margin": 0, "color": verdict_color, "fontWeight": "600"},
                    ),
                ],
            )
        )
    return cards


# ── Figure 3: Exit distribution ───────────────────────────────────────────────
def _fig_exit_dist(lp: pd.DataFrame) -> go.Figure:
    exited = lp[lp["status"] == 1]["duration_days"]
    fig = go.Figure(
        go.Histogram(
            x=exited,
            nbinsx=30,
            marker=dict(color="#636EFA", line=dict(color="#3a3a5a", width=1)),
            opacity=0.8,
            name="Exited LPs",
        )
    )
    fig.update_layout(
        title=f"Exit-Time Distribution (n={len(exited)} exited LPs)",
        xaxis_title="Days to exit",
        yaxis_title="Number of LPs",
        yaxis=dict(gridcolor="#2a2a3a", zerolinecolor="#2a2a3a"),
        showlegend=False,
        **{k: v for k, v in PLOT_LAYOUT.items() if k != "yaxis"},
    )
    return fig


# ── App ───────────────────────────────────────────────────────────────────────
lp = _load()
fig_all = _fig_km_all(lp)
fig_cohorts, fitted = _fig_km_cohorts(lp)
logrank_blocks = _logrank_cards(fitted)
fig_exit = _fig_exit_dist(lp)

n_total  = len(lp)
n_exited = int(lp["status"].sum())
n_active = n_total - n_exited

app = Dash(__name__)
app.title = "LP Retention Dashboard"

app.layout = html.Div(
    style={"background": "#0f1117", "minHeight": "100vh", "padding": "24px 32px", "color": "#e8e8e8"},
    children=[
        html.H2(
            "LP Retention Dashboard — Celo WETH/USDT",
            style={"marginBottom": "4px", "fontFamily": "Inter, sans-serif"},
        ),
        html.P(
            f"{n_total} LPs tracked  ·  {n_exited} exited  ·  {n_active} still active",
            style={"color": "#888", "marginTop": 0, "fontFamily": "Inter, sans-serif"},
        ),
        dcc.Tabs(
            style={"fontFamily": "Inter, sans-serif"},
            colors={"border": "#2a2a3a", "primary": "#636EFA", "background": "#0f1117"},
            children=[
                dcc.Tab(
                    label="Survival — All LPs",
                    style={"color": "#aaa", "background": "#0f1117"},
                    selected_style={"color": "#fff", "background": "#1a1a2e", "borderTop": "2px solid #636EFA"},
                    children=[dcc.Graph(figure=fig_all, style={"height": "520px"})],
                ),
                dcc.Tab(
                    label="Survival — Cohorts",
                    style={"color": "#aaa", "background": "#0f1117"},
                    selected_style={"color": "#fff", "background": "#1a1a2e", "borderTop": "2px solid #636EFA"},
                    children=[
                        dcc.Graph(figure=fig_cohorts, style={"height": "520px"}),
                        html.Div(logrank_blocks, style={"maxWidth": "680px", "paddingBottom": "32px"}),
                    ],
                ),
                dcc.Tab(
                    label="Exit Distribution",
                    style={"color": "#aaa", "background": "#0f1117"},
                    selected_style={"color": "#fff", "background": "#1a1a2e", "borderTop": "2px solid #636EFA"},
                    children=[dcc.Graph(figure=fig_exit, style={"height": "520px"})],
                ),
            ],
        ),
    ],
)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)

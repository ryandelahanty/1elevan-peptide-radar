"""
1ElevanBio Peptide Radar — v1.0.1
Databricks App (Streamlit) — NO personal API key, runs on DBU via Model Serving.
"""

import os
import json
from datetime import datetime
from typing import List, Dict

import pandas as pd
import streamlit as st

from databricks.sdk import WorkspaceClient

APP_VERSION    = "peptide-radar-v1.0.1"
WAREHOUSE_ID   = os.environ.get("WAREHOUSE_ID", "d6302cf341bcdde0")
MODEL_ENDPOINT = os.environ.get("MODEL_ENDPOINT_NAME", "databricks-claude-sonnet-4-5")

BRAND = {
    "primary_green": "#299143",
    "deep_green":    "#2E7D32",
    "charcoal":      "#2A2A2A",
    "mint_bg":       "#F4F8F2",
}

# ── Clients ───────────────────────────────────────────────────────────────────

@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()


def _run_sql(statement: str, timeout: str = "30s") -> pd.DataFrame:
    w = get_workspace_client()
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=statement,
            wait_timeout=timeout,
        ).result
        if not result or not result.schema:
            return pd.DataFrame()
        cols = [c.name for c in result.schema.columns]
        rows = [[getattr(r, c, None) for c in cols] for r in (result.data_array or [])]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.warning(f"SQL error: {e}")
        return pd.DataFrame()


# ── Data loaders ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_watchlist() -> pd.DataFrame:
    return _run_sql("""
        SELECT
            p.canonical_name,
            COALESCE(o.composite_score,     p.strategic_fit_score) AS composite_score,
            COALESCE(o.regulatory_score,    0.0) AS regulatory_score,
            COALESCE(o.evidence_score,      0.0) AS evidence_score,
            COALESCE(o.ip_whitespace_score, 0.0) AS ip_whitespace_score,
            COALESCE(o.supply_score,        0.0) AS supply_score,
            COALESCE(o.score_delta_7d,      0.0) AS score_delta_7d,
            COALESCE(o.convergence_count_30d, 0) AS convergence_count_30d,
            COALESCE(o.regulatory_change,  FALSE) AS regulatory_change,
            COALESCE(o.alert_threshold_hit,FALSE) AS alert_threshold_hit,
            p.indication_tags
        FROM peptide_radar.silver.peptides p
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY peptide_id ORDER BY score_date DESC) AS rn
            FROM peptide_radar.silver.opportunity_scores
        ) o ON p.peptide_id = o.peptide_id AND o.rn = 1
        WHERE p.watchlist_active = TRUE
        ORDER BY composite_score DESC
    """)


@st.cache_data(ttl=300)
def load_recent_signals(days: int = 14) -> pd.DataFrame:
    return _run_sql(f"""
        SELECT p.canonical_name, s.event_date, s.source_type,
               s.event_type, s.event_direction, s.severity, s.event_value
        FROM peptide_radar.silver.signals s
        JOIN peptide_radar.silver.peptides p ON s.peptide_id = p.peptide_id
        WHERE s.event_date >= date_sub(current_date(), {days})
        ORDER BY s.event_date DESC, s.severity
        LIMIT 200
    """)


@st.cache_data(ttl=300)
def load_weekly_digest() -> pd.DataFrame:
    return _run_sql("""
        SELECT canonical_name, digest_week, composite_score,
               score_delta_7d, regulatory_status, top_signal_summary
        FROM peptide_radar.gold.weekly_digest_items
        ORDER BY digest_week DESC, composite_score DESC
        LIMIT 50
    """)


@st.cache_data(ttl=600)
def load_costs() -> dict:
    df = _run_sql("""
        SELECT COALESCE(SUM(cost_usd),0.0)            AS total_usd,
               COALESCE(SUM(tokens_in+tokens_out),0)  AS total_tokens,
               COUNT(*)                               AS total_calls
        FROM peptide_radar.gold.llm_costs
        WHERE date_format(call_timestamp,'yyyy-MM') =
              date_format(current_timestamp(),'yyyy-MM')
    """, timeout="15s")
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "total_usd":    float(r.get("total_usd")    or 0),
        "total_tokens": int(r.get("total_tokens")   or 0),
        "total_calls":  int(r.get("total_calls")    or 0),
    }


# ── LLM chat ─────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are 1ElevanBio's Peptide Radar intelligence assistant.
You monitor 36 peptides across FDA 503A/503B lists, ClinicalTrials.gov,
PubMed/bioRxiv, and NIH RePORTER for compounding opportunity signals.

Scoring: Regulatory 30% | Evidence 25% | IP Whitespace 20% | Supply 15% | Fit 10%
Alert thresholds: composite >= 0.72, delta_7d >= 0.15, convergence >= 3 sources.

Answer questions about watchlist scores, signals, and opportunities.
Be specific. Cite scores and sources. Do not fabricate data.
"""


def run_chat(history: List[Dict], question: str, context: str = "") -> str:
    w = get_workspace_client()
    try:
        from databricks_openai import DatabricksOpenAI
        client = DatabricksOpenAI(client=w)
        msgs = (
            [{"role": "system", "content": SYSTEM_PROMPT}]
            + history
            + [{"role": "user", "content": question + context}]
        )
        resp = client.chat.completions.create(
            model=MODEL_ENDPOINT,
            messages=msgs,
            max_tokens=1500,
            temperature=0.1,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"


# ── Page setup ────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Peptide Radar", page_icon="📡", layout="wide")

st.markdown(f"""
<style>
.radar-header {{
    background: linear-gradient(90deg, {BRAND['deep_green']} 0%, {BRAND['primary_green']} 100%);
    padding: 1.2rem 1.5rem; border-radius: 8px; color: white; margin-bottom: 1rem;
}}
* {{ color: inherit; }}
</style>
<div class="radar-header">
  <h2 style="margin:0;color:white;">📡 Peptide Radar</h2>
  <span style="opacity:0.85;font-size:0.9em;color:white;">
    FDA · ClinicalTrials · PubMed · NIH RePORTER &nbsp;|&nbsp;
    36 peptides &nbsp;|&nbsp; {APP_VERSION}
  </span>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    view = st.radio("", ["Watchlist", "Signals", "Weekly Digest", "Ask Radar", "System"])
    st.divider()
    costs = load_costs()
    if costs:
        st.caption("💰 Month-to-date LLM (DBU)")
        st.metric("Spend",  f"${costs.get('total_usd',0):.4f}")
        st.metric("Tokens", f"{costs.get('total_tokens',0):,}")
        st.metric("Calls",  str(costs.get("total_calls", 0)))
    st.divider()
    st.caption("Mon FDA · Tue CT · Wed PubMed\nThu NIH · Fri Score+Digest\n(all 06:00 UTC)")
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()


# ── WATCHLIST ─────────────────────────────────────────────────────────────────

if view == "Watchlist":
    st.subheader("Ranked Watchlist")
    df = load_watchlist()

    if df.empty:
        st.info("No data yet. Run DEPLOY_NOTEBOOK to seed the watchlist and start jobs.")
    else:
        c1, c2 = st.columns(2)
        min_score   = c1.slider("Min composite score", 0.0, 1.0, 0.0, 0.05)
        alerts_only = c2.checkbox("Alerts only (≥ 0.72 or reg change)")

        disp = df[df["composite_score"].astype(float) >= min_score]
        if alerts_only:
            disp = disp[
                (disp["composite_score"].astype(float) >= 0.72) |
                (disp["regulatory_change"] == True)
            ]

        for _, row in disp.iterrows():
            comp  = float(row.get("composite_score") or 0)
            delta = float(row.get("score_delta_7d")  or 0)
            badges = ""
            if row.get("regulatory_change"):   badges += "🚨 REG CHANGE &nbsp;"
            if row.get("alert_threshold_hit"): badges += "⚡ ELEVATED &nbsp;"
            if delta >  0.15: badges += f"📈 +{delta:.2f} &nbsp;"
            elif delta < -0.10: badges += f"📉 {delta:.2f} &nbsp;"

            with st.expander(
                f"**{str(row['canonical_name']).title()}** &nbsp; {comp:.2f} &nbsp; {badges}",
                expanded=bool(row.get("alert_threshold_hit") or row.get("regulatory_change"))
            ):
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Regulatory", f"{float(row.get('regulatory_score')    or 0):.2f}")
                c2.metric("Evidence",   f"{float(row.get('evidence_score')      or 0):.2f}")
                c3.metric("IP Space",   f"{float(row.get('ip_whitespace_score') or 0):.2f}")
                c4.metric("Supply",     f"{float(row.get('supply_score')        or 0):.2f}")
                c5.metric("Conv. 30d",  str(row.get("convergence_count_30d")    or 0))
                tags = row.get("indication_tags")
                if tags:
                    st.caption(str(tags))

        st.download_button(
            "Download CSV", disp.to_csv(index=False).encode(),
            f"watchlist_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv",
        )


# ── SIGNALS ───────────────────────────────────────────────────────────────────

elif view == "Signals":
    st.subheader("Recent Signals")
    days = st.slider("Days back", 7, 90, 14)
    df   = load_recent_signals(days)

    if df.empty:
        st.info("No signals yet — jobs collect Mon–Fri 06:00 UTC.")
    else:
        sev_f = st.multiselect("Severity", ["critical","high","medium","low"], default=["critical","high"])
        src_f = st.multiselect("Source", df["source_type"].dropna().unique().tolist())
        disp = df.copy()
        if sev_f: disp = disp[disp["severity"].isin(sev_f)]
        if src_f: disp = disp[disp["source_type"].isin(src_f)]

        SEV_ICON = {"critical":"🔴","high":"🟠","medium":"🔵","low":"🟢"}
        for _, row in disp.iterrows():
            icon = SEV_ICON.get(str(row.get("severity","")), "⚪")
            st.markdown(
                f"{icon} **{str(row['canonical_name']).title()}** &nbsp;·&nbsp; "
                f"`{row['event_type']}` &nbsp;·&nbsp; {row['source_type']} &nbsp;·&nbsp; {row['event_date']}"
            )
            if row.get("event_value"):
                try:
                    st.json(json.loads(str(row["event_value"])), expanded=False)
                except Exception:
                    st.caption(str(row["event_value"]))
        st.dataframe(disp, use_container_width=True, hide_index=True)


# ── WEEKLY DIGEST ─────────────────────────────────────────────────────────────

elif view == "Weekly Digest":
    st.subheader("Weekly Digest (LLM-enriched, DBU)")
    df = load_weekly_digest()

    if df.empty:
        st.info("No digest yet. Job 5 runs Fridays 06:00 UTC.")
    else:
        weeks  = df["digest_week"].dropna().unique().tolist()
        sel    = st.selectbox("Week", weeks)
        wk_df  = df[df["digest_week"] == sel]
        for _, row in wk_df.iterrows():
            with st.expander(
                f"**{str(row['canonical_name']).title()}** — "
                f"{float(row.get('composite_score') or 0):.2f} "
                f"(Δ {float(row.get('score_delta_7d') or 0):+.2f})"
            ):
                if row.get("regulatory_status"):
                    st.markdown(f"**Regulatory:** {row['regulatory_status']}")
                if row.get("top_signal_summary"):
                    st.markdown(str(row["top_signal_summary"]))


# ── ASK RADAR ────────────────────────────────────────────────────────────────

elif view == "Ask Radar":
    st.subheader("Ask Radar")
    st.caption("Chat with your monitoring data. Powered by Databricks Model Serving (DBU).")

    if "radar_history" not in st.session_state:
        st.session_state.radar_history = []
        st.session_state.radar_display = []

    for msg in st.session_state.radar_display:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if q := st.chat_input("Ask about peptide scores, signals, or opportunities"):
        wl      = load_watchlist()
        context = ""
        if not wl.empty:
            top5    = wl.head(5)[["canonical_name","composite_score","score_delta_7d"]].to_string(index=False)
            context = f"\n\n[Current top 5]\n{top5}"

        st.session_state.radar_display.append({"role":"user","content":q})
        with st.chat_message("user"):
            st.markdown(q)
        with st.chat_message("assistant"):
            with st.spinner("Querying Radar data..."):
                answer = run_chat(st.session_state.radar_history, q, context)
                st.markdown(answer)
                st.session_state.radar_history += [
                    {"role":"user","content":q},
                    {"role":"assistant","content":answer},
                ]
                st.session_state.radar_display.append({"role":"assistant","content":answer})

    if st.button("Clear"):
        st.session_state.radar_history = []
        st.session_state.radar_display = []
        st.rerun()


# ── SYSTEM ────────────────────────────────────────────────────────────────────

elif view == "System":
    st.subheader("System Status")
    wl  = load_watchlist()
    sig = load_recent_signals(30)
    dig = load_weekly_digest()
    costs = load_costs()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Peptides tracked", len(wl)  if not wl.empty  else 0)
    c2.metric("Signals (30d)",    len(sig) if not sig.empty else 0)
    c3.metric("Digest items",     len(dig) if not dig.empty else 0)
    c4.metric("LLM calls/month",  costs.get("total_calls", 0))

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Job Schedule (UTC)**")
        for s, d in [
            ("Mon 06:00", "FDA 503A/503B differ — $0"),
            ("Tue 06:00", "ClinicalTrials poller — $0"),
            ("Wed 06:00", "PubMed + bioRxiv — $0"),
            ("Thu 06:00", "NIH RePORTER — $0"),
            ("Fri 06:00", "Scorer + weekly digest — ~$0.15 max"),
        ]:
            st.markdown(f"- **{s}**: {d}")
        st.markdown(f"\n**LLM endpoint:** `{MODEL_ENDPOINT}` (DBU, no API key)")
    with col2:
        st.markdown("**Scoring weights**")
        st.json({"regulatory":0.30,"evidence":0.25,"ip":0.20,"supply":0.15,"fit":0.10})
        st.markdown("**Alert thresholds**")
        st.json({"composite":">=0.72","delta_7d":">=0.15","convergence":">=3 sources"})

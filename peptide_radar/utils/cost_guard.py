"""
cost_guard.py — Central LLM gateway for Peptide Radar.

KEY ARCHITECTURE DECISION (2026-05-21):
  ALL LLM calls go through Databricks Model Serving endpoints using the
  Databricks OpenAI-compatible API. This burns DBU, not personal API credits.

  endpoint: /serving-endpoints/databricks-claude-sonnet-4-5/invocations
  (or haiku equivalent on the workspace)

  The workspace token is obtained via:
    spark.conf.get("spark.databricks.token")      -- inside a job
    OR dbutils.notebook.entry_point.getDbutils()  -- inside a notebook

  NO Anthropic API key is needed. If ANTHROPIC_API_KEY secret exists,
  it is ignored here. Do not add direct anthropic.Anthropic() calls.

RULES (override everything):
  1. governed_llm_call() is the ONLY entry point for LLM calls. No exceptions.
  2. Never call this in a for-loop over individual items. Batch everything.
  3. One call per job run maximum.

GUARDS (in order):
  1. circuit_breaker — halt if > $0.50 spent in last hour
  2. prompt_size     — reject if prompt > 6,000 chars (~1,500 tokens)
  3. monthly_ceiling — reject if monthly cumulative + estimate > 50,000 tokens
  4. daily_job_ceiling — reject if today's job total + estimate > 5,000 tokens

All calls logged to peptide_radar.gold.llm_costs regardless of outcome.
Returns None (never raises) if any guard fires — callers must handle None.
"""

import uuid
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
CIRCUIT_BREAKER_HOURLY_USD   = 0.50
MONTHLY_TOKEN_LIMIT          = 50_000
DAILY_TOKEN_LIMIT_PER_JOB    = 5_000
MAX_PROMPT_CHARS             = 6_000   # ~1,500 tokens
MAX_TOKENS_PER_LLM_CALL      = 1_500
LLM_TIMEOUT_SECONDS          = 30

# Databricks Model Serving endpoint names (workspace-hosted, burns DBU not API credits)
# Use the haiku-equivalent for weekly digest; sonnet for monthly brief only.
# Adjust these names to match what's deployed in your workspace:
#   Databricks UI → Serving → check active endpoints
HAIKU_ENDPOINT   = "databricks-meta-llama-3-1-70b-instruct"   # swap to claude-haiku if available
SONNET_ENDPOINT  = "databricks-claude-sonnet-4-5"              # monthly brief only

# Cost estimates per 1K tokens (DBU-based, approximate — actual billed as DBU)
COST_PER_1K_TOKENS_IN   = 0.000_25   # rough DBU equivalent for haiku-class
COST_PER_1K_TOKENS_OUT  = 0.001_25


# ── Spark / dbutils helpers ──────────────────────────────────────────────────

def _get_spark():
    """Get active SparkSession. Works inside Databricks jobs and notebooks."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _get_workspace_token() -> str:
    """
    Get Databricks workspace token for model serving calls.
    Inside a Databricks job, the cluster has an auto-provisioned token.
    """
    spark = _get_spark()
    try:
        return spark.conf.get("spark.databricks.token")
    except Exception:
        pass
    try:
        import subprocess, os
        token = subprocess.check_output(
            ["databricks", "auth", "token", "--host",
             spark.conf.get("spark.databricks.workspaceUrl", "")],
            text=True, timeout=10
        ).strip()
        if token:
            return token
    except Exception:
        pass
    raise RuntimeError(
        "Cannot obtain Databricks workspace token. "
        "Ensure job cluster has token-based auth enabled."
    )


def _get_workspace_host() -> str:
    spark = _get_spark()
    host = spark.conf.get(
        "spark.databricks.workspaceUrl",
        "adb-252904149011683.3.azuredatabricks.net"
    )
    if not host.startswith("https://"):
        host = "https://" + host
    return host.rstrip("/")


# ── LLM call via Databricks Model Serving ───────────────────────────────────

def _call_model_serving(prompt: str, endpoint_name: str) -> dict:
    """
    Call a Databricks Model Serving endpoint using the OpenAI-compatible API.
    Returns dict with keys: content, tokens_in, tokens_out
    Raises on HTTP error after one retry.
    """
    import urllib.request

    host  = _get_workspace_host()
    token = _get_workspace_token()
    url   = f"{host}/serving-endpoints/{endpoint_name}/invocations"

    payload = json.dumps({
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS_PER_LLM_CALL,
        "temperature": 0.1,
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )

    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(req, timeout=LLM_TIMEOUT_SECONDS) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                content    = body["choices"][0]["message"]["content"].strip()
                tokens_in  = body.get("usage", {}).get("prompt_tokens", 0)
                tokens_out = body.get("usage", {}).get("completion_tokens", 0)
                return {"content": content, "tokens_in": tokens_in, "tokens_out": tokens_out}
        except urllib.error.HTTPError as e:
            if attempt == 2 or e.code not in (429, 503):
                raise
            time.sleep(5)


# ── Cost tracking ────────────────────────────────────────────────────────────

def _log_llm_call(job_name: str, endpoint: str,
                  tokens_in: int, tokens_out: int,
                  cost_usd: float, triggered_by: str, call_id: str):
    spark = _get_spark()
    row = [{
        "call_id":        call_id,
        "job_name":       job_name,
        "model":          endpoint,
        "tokens_in":      tokens_in,
        "tokens_out":     tokens_out,
        "cost_usd":       round(cost_usd, 6),
        "triggered_by":   triggered_by,
        "call_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }]
    spark.createDataFrame(row).write.mode("append").saveAsTable(
        "peptide_radar.gold.llm_costs"
    )


def _hourly_spend_usd() -> float:
    spark = _get_spark()
    try:
        row = spark.sql("""
            SELECT COALESCE(SUM(cost_usd), 0.0) AS total
            FROM peptide_radar.gold.llm_costs
            WHERE call_timestamp >= date_sub(current_timestamp(), INTERVAL 1 HOUR)
        """).first()
        return float(row["total"])
    except Exception:
        return 0.0


def _monthly_token_total() -> int:
    spark = _get_spark()
    try:
        row = spark.sql("""
            SELECT COALESCE(SUM(tokens_in + tokens_out), 0) AS total
            FROM peptide_radar.gold.llm_costs
            WHERE date_format(call_timestamp, 'yyyy-MM') =
                  date_format(current_timestamp(), 'yyyy-MM')
        """).first()
        return int(row["total"])
    except Exception:
        return 0


def _daily_job_token_total(job_name: str) -> int:
    spark = _get_spark()
    try:
        row = spark.sql(f"""
            SELECT COALESCE(SUM(tokens_in + tokens_out), 0) AS total
            FROM peptide_radar.gold.llm_costs
            WHERE job_name = '{job_name}'
              AND date(call_timestamp) = current_date()
        """).first()
        return int(row["total"])
    except Exception:
        return 0


# ── Guards ───────────────────────────────────────────────────────────────────

def check_circuit_breaker():
    """Raises RuntimeError if hourly spend exceeds limit. Call at job start."""
    spend = _hourly_spend_usd()
    if spend >= CIRCUIT_BREAKER_HOURLY_USD:
        raise RuntimeError(
            f"Circuit breaker tripped: ${spend:.4f} spent in last hour "
            f"(limit ${CIRCUIT_BREAKER_HOURLY_USD}). All LLM calls halted."
        )


# ── Main entry point ─────────────────────────────────────────────────────────

def governed_llm_call(
    prompt:       str,
    job_name:     str,
    triggered_by: str  = "scheduled",
    use_sonnet:   bool = False,
) -> Optional[str]:
    """
    THE ONLY ENTRY POINT FOR LLM CALLS in Peptide Radar.

    Uses Databricks Model Serving — burns DBU, not personal API credits.

    Args:
        prompt:       Full prompt text. Must be pre-batched (no per-item loops).
        job_name:     Name of calling job (for cost attribution).
        triggered_by: 'scheduled', 'manual', or 'threshold'.
        use_sonnet:   True only for job_monthly_brief. Default False (haiku-class).

    Returns:
        LLM response string, or None if any guard fired.
    """
    call_id  = str(uuid.uuid4())
    endpoint = SONNET_ENDPOINT if use_sonnet else HAIKU_ENDPOINT

    # Guard 1 — circuit breaker
    hourly = _hourly_spend_usd()
    if hourly >= CIRCUIT_BREAKER_HOURLY_USD:
        logger.warning(
            f"[{job_name}] Circuit breaker fired: ${hourly:.4f}/hr. Call skipped."
        )
        _log_llm_call(job_name, endpoint, 0, 0, 0.0, "circuit_breaker_fired", call_id)
        return None

    # Guard 2 — prompt size
    if len(prompt) > MAX_PROMPT_CHARS:
        logger.warning(
            f"[{job_name}] Prompt too large ({len(prompt)} chars > {MAX_PROMPT_CHARS}). "
            "Truncate or reduce items. Call skipped."
        )
        _log_llm_call(job_name, endpoint, 0, 0, 0.0, "prompt_too_large", call_id)
        return None

    # Guard 3 — monthly ceiling
    monthly_used = _monthly_token_total()
    est_tokens   = len(prompt) // 4 + MAX_TOKENS_PER_LLM_CALL
    if monthly_used + est_tokens > MONTHLY_TOKEN_LIMIT:
        logger.warning(
            f"[{job_name}] Monthly token ceiling: {monthly_used} used, "
            f"{est_tokens} estimated, limit {MONTHLY_TOKEN_LIMIT}. Call skipped."
        )
        _log_llm_call(job_name, endpoint, 0, 0, 0.0, "monthly_ceiling_hit", call_id)
        return None

    # Guard 4 — daily job ceiling
    daily_used = _daily_job_token_total(job_name)
    if daily_used + est_tokens > DAILY_TOKEN_LIMIT_PER_JOB:
        logger.warning(
            f"[{job_name}] Daily ceiling for job: {daily_used} used, "
            f"{est_tokens} estimated, limit {DAILY_TOKEN_LIMIT_PER_JOB}. Call skipped."
        )
        _log_llm_call(job_name, endpoint, 0, 0, 0.0, "daily_ceiling_hit", call_id)
        return None

    # Execute
    try:
        result    = _call_model_serving(prompt, endpoint)
        cost_usd  = (
            result["tokens_in"]  / 1000 * COST_PER_1K_TOKENS_IN +
            result["tokens_out"] / 1000 * COST_PER_1K_TOKENS_OUT
        )
        _log_llm_call(
            job_name, endpoint,
            result["tokens_in"], result["tokens_out"],
            cost_usd, triggered_by, call_id
        )
        logger.info(
            f"[{job_name}] LLM call OK: {result['tokens_in']}in "
            f"{result['tokens_out']}out ${cost_usd:.5f}"
        )
        return result["content"]

    except Exception as e:
        logger.error(f"[{job_name}] LLM call failed: {e}")
        _log_llm_call(job_name, endpoint, 0, 0, 0.0, f"error:{e}", call_id)
        return None

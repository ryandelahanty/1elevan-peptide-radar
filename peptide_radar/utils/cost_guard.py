import os
import uuid
from datetime import datetime, timezone

import anthropic

MAX_TOKENS_PER_LLM_CALL = 8000
MONTHLY_TOKEN_LIMIT = 500000
DAILY_TOKEN_LIMIT_PER_JOB = 50000
MODEL = "claude-haiku-4-5-20251001"


def _get_api_key():
    try:
        return dbutils.secrets.get("peptide-radar", "ANTHROPIC_API_KEY")
    except NameError:
        return os.environ["ANTHROPIC_API_KEY"]


def check_circuit_breaker():
    try:
        row = spark.sql("""
            SELECT COALESCE(SUM(tokens_in + tokens_out), 0) AS total_tokens
            FROM peptide_radar.gold.llm_costs
            WHERE MONTH(call_timestamp) = MONTH(current_timestamp())
              AND YEAR(call_timestamp) = YEAR(current_timestamp())
        """).first()
        if row and row["total_tokens"] >= MONTHLY_TOKEN_LIMIT:
            raise RuntimeError(
                f"Circuit breaker: monthly token limit reached ({row['total_tokens']} >= {MONTHLY_TOKEN_LIMIT})"
            )
    except NameError:
        pass


def governed_llm_call(prompt, job_name, triggered_by="system"):
    try:
        check_circuit_breaker()
    except RuntimeError:
        return None

    estimated_tokens = int(len(prompt) * 0.25)
    if estimated_tokens > MAX_TOKENS_PER_LLM_CALL:
        return None

    try:
        row = spark.sql(f"""
            SELECT COALESCE(SUM(tokens_in + tokens_out), 0) AS daily_tokens
            FROM peptide_radar.gold.llm_costs
            WHERE job_name = '{job_name}'
              AND DATE(call_timestamp) = current_date()
        """).first()
        daily_total = row["daily_tokens"] if row else 0
        if daily_total + estimated_tokens > DAILY_TOKEN_LIMIT_PER_JOB:
            return None
    except NameError:
        pass

    try:
        api_key = _get_api_key()
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS_PER_LLM_CALL,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text
        tokens_in = response.usage.input_tokens
        tokens_out = response.usage.output_tokens
        cost_usd = tokens_in * 0.00000025 + tokens_out * 0.00000125

        call_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        try:
            spark.sql(f"""
                INSERT INTO peptide_radar.gold.llm_costs VALUES (
                    '{call_id}', '{job_name}', '{MODEL}',
                    {tokens_in}, {tokens_out}, {cost_usd},
                    '{triggered_by}', '{now}'
                )
            """)
        except NameError:
            pass

        return text
    except Exception:
        return None

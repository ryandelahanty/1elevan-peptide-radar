"""
teams_notifier.py — Microsoft Teams webhook notifications for Peptide Radar.

Webhook URL stored in Databricks Secrets:
  scope: peptide-radar
  key:   TEAMS_WEBHOOK_URL

To set the webhook:
  1. In Teams: channel → ... → Connectors → Incoming Webhook → Configure
  2. Copy the URL
  3. Run: databricks secrets put-secret peptide-radar TEAMS_WEBHOOK_URL
  4. Paste URL when prompted

Fail-silent: a Teams outage must NEVER crash a Databricks job.
If the webhook URL is not yet configured (stub value), logs a warning and returns.
"""

import json
import logging
import urllib.request
import urllib.error
from typing import Optional

logger = logging.getLogger(__name__)

SEVERITY_COLORS = {
    "critical": "FF0000",   # red
    "high":     "FF8C00",   # orange
    "medium":   "0078D4",   # Teams blue
    "low":      "107C10",   # green
}


def _get_webhook_url() -> Optional[str]:
    """Get Teams webhook URL from Databricks Secrets. Returns None if not set."""
    try:
        # Works inside Databricks jobs/notebooks
        import subprocess
        result = subprocess.run(
            ["databricks", "secrets", "get-secret",
             "--scope", "peptide-radar", "--key", "TEAMS_WEBHOOK_URL"],
            capture_output=True, text=True, timeout=10
        )
        url = result.stdout.strip()
        if url and url.startswith("https://") and "webhook" in url.lower():
            return url
        return None
    except Exception:
        pass

    # Fallback: try dbutils (notebook context)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        sc    = spark.sparkContext
        dbutils_module = sc._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils0()
        url = str(dbutils_module.secrets().get("peptide-radar", "TEAMS_WEBHOOK_URL"))
        if url and url.startswith("https://"):
            return url
    except Exception:
        pass

    return None


def _post(payload: dict) -> bool:
    """POST JSON payload to webhook. Returns True on success. Never raises."""
    url = _get_webhook_url()
    if not url:
        logger.warning(
            "Teams webhook not configured. "
            "Set Databricks secret: peptide-radar / TEAMS_WEBHOOK_URL. "
            "Notification skipped."
        )
        return False

    try:
        data = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        logger.error(f"Teams webhook HTTP error {e.code}: {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Teams webhook error: {e}")
        return False


def send_alert(title: str, message: str, severity: str = "medium") -> bool:
    """
    Send an immediate alert card to Teams.

    Args:
        title:    Short title, e.g. "FDA Category Change — Vasopressin"
        message:  1-2 sentence body
        severity: 'critical', 'high', 'medium', 'low'

    Returns True on success. Never raises.
    """
    color = SEVERITY_COLORS.get(severity.lower(), SEVERITY_COLORS["medium"])

    # Teams Adaptive Card (v1.2, supported by all Teams clients)
    payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type":    "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type":   "TextBlock",
                        "text":   f"🔬 Peptide Radar — {title}",
                        "weight": "Bolder",
                        "size":   "Medium",
                        "color":  "Accent",
                        "wrap":   True,
                    },
                    {
                        "type": "TextBlock",
                        "text": message,
                        "wrap": True,
                    },
                    {
                        "type":     "FactSet",
                        "facts": [
                            {"title": "Severity", "value": severity.upper()},
                            {"title": "Source",   "value": "Peptide Radar v1"},
                        ],
                    },
                ],
            },
        }],
    }

    success = _post(payload)
    if success:
        logger.info(f"Teams alert sent: [{severity.upper()}] {title}")
    return success


def send_digest(text: str) -> bool:
    """
    Post the weekly digest text block to Teams.

    Args:
        text: Pre-formatted weekly digest string from job_opportunity_scorer.

    Returns True on success. Never raises.
    """
    payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type":    "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type":   "TextBlock",
                        "text":   "📊 Peptide Radar — Weekly Watchlist",
                        "weight": "Bolder",
                        "size":   "Large",
                        "color":  "Accent",
                    },
                    {
                        "type": "TextBlock",
                        "text": text,
                        "wrap": True,
                    },
                ],
            },
        }],
    }

    success = _post(payload)
    if success:
        logger.info("Teams weekly digest sent.")
    return success

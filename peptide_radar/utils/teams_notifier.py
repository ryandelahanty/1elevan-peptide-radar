import os

import requests

SEVERITY_COLORS = {
    "critical": "FF0000",
    "high": "FF6600",
    "medium": "0076D7",
}


def _get_webhook_url():
    try:
        return dbutils.secrets.get("peptide-radar", "SLACK_WEBHOOK_URL")
    except NameError:
        return os.environ["SLACK_WEBHOOK_URL"]


def send_alert(title, message, severity):
    try:
        url = _get_webhook_url()
        color = SEVERITY_COLORS.get(severity, "0076D7")
        payload = {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "summary": title,
            "themeColor": color,
            "sections": [{"activityTitle": title, "activityText": message}],
        }
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Teams alert failed: {e}")


def send_digest(text):
    try:
        send_alert("Peptide Radar Weekly Digest", text, "medium")
    except Exception as e:
        print(f"Teams digest failed: {e}")

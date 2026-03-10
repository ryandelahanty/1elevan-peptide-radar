from peptide_radar.ingestors.fda_bulks import SOURCES, process_source
from peptide_radar.utils.teams_notifier import send_alert


def run():
    """Entry point for FDA Bulks Differ job. Zero LLM calls."""
    print("=== job_fda_bulks: starting ===")

    total = {
        "sources_checked": 0,
        "snapshots_changed": 0,
        "signals_written": 0,
        "unresolved": 0,
    }

    for source_name, config in SOURCES.items():
        try:
            result = process_source(source_name, config["url"], config["content_type"])
            total["sources_checked"] += 1
            if result["changed"]:
                total["snapshots_changed"] += 1
            total["signals_written"] += result["signals_written"]
            total["unresolved"] += result["unresolved_count"]

            for sig in result.get("alerts", []):
                if sig["event_type"] in ("fda_category_change", "fda_safety_risk_added"):
                    title = f"FDA Alert: {sig['event_type'].replace('_', ' ').title()}"
                    message = (
                        f"Peptide: {sig['raw_ref']}\n"
                        f"Event: {sig['event_type']}\n"
                        f"Severity: {sig['severity']}"
                    )
                    send_alert(title, message, sig["severity"])
        except Exception as e:
            print(f"  ERROR processing {source_name}: {e}")

    print("=== job_fda_bulks: complete ===")
    print(f"  Sources checked: {total['sources_checked']}")
    print(f"  Snapshots changed: {total['snapshots_changed']}")
    print(f"  Signals written: {total['signals_written']}")
    print(f"  Entities unresolved: {total['unresolved']}")


if __name__ == "__main__":
    run()

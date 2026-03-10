from peptide_radar.ingestors.nih_reporter import fetch_grants, process_grants


def run():
    """Entry point for NIH RePORTER Monitor job. Zero LLM calls. No alerts."""
    print("=== job_nih_reporter: starting ===")

    try:
        projects = fetch_grants()
        print(f"  Grants fetched: {len(projects)}")

        result = process_grants(projects)

        print("=== job_nih_reporter: complete ===")
        print(f"  Grants fetched: {result['grants_fetched']}")
        print(f"  New signals: {result['new_signals']}")
        print(f"  Entities unresolved: {result['unresolved_count']}")
    except Exception as e:
        print(f"  ERROR: {e}")
        print("=== job_nih_reporter: failed ===")


if __name__ == "__main__":
    run()

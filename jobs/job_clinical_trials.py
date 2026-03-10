from peptide_radar.ingestors.clinical_trials import fetch_studies, process_studies


def run():
    """Entry point for ClinicalTrials Poller job. Zero LLM calls. No alerts."""
    print("=== job_clinical_trials: starting ===")

    try:
        studies = fetch_studies()
        print(f"  Fetched {len(studies)} studies from ClinicalTrials.gov")

        result = process_studies(studies)

        print("=== job_clinical_trials: complete ===")
        print(f"  Trials fetched: {result['trials_fetched']}")
        print(f"  New signals: {result['new_signals']}")
        print(f"  Entities unresolved: {result['unresolved_count']}")
    except Exception as e:
        print(f"  ERROR: {e}")
        print("=== job_clinical_trials: failed ===")


if __name__ == "__main__":
    run()

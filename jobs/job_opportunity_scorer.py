from peptide_radar.scoring.opportunity_scorer import score_all_peptides, generate_digest


def run():
    """Entry point for Opportunity Scorer + Weekly Digest. One LLM call max per run."""
    print("=== job_opportunity_scorer: starting ===")

    try:
        score_rows, elevated = score_all_peptides()
        print(f"  Peptides scored: {len(score_rows)}")
        print(f"  Peptides escalated: {len(elevated)}")

        digest_items = generate_digest(elevated)
        print(f"  Digest items written: {len(digest_items)}")

        print("=== job_opportunity_scorer: complete ===")
    except Exception as e:
        print(f"  ERROR: {e}")
        print("=== job_opportunity_scorer: failed ===")


if __name__ == "__main__":
    run()

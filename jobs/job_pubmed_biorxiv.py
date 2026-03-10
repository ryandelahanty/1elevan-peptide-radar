from peptide_radar.ingestors.pubmed_biorxiv import (
    fetch_pubmed,
    fetch_biorxiv,
    process_articles,
    _load_watchlist_names,
)


def run():
    """Entry point for PubMed + bioRxiv Harvester job. Zero LLM calls. No alerts."""
    print("=== job_pubmed_biorxiv: starting ===")

    try:
        pubmed_articles = fetch_pubmed()
        print(f"  PubMed articles fetched: {len(pubmed_articles)}")

        watchlist_names = _load_watchlist_names()
        biorxiv_articles = fetch_biorxiv(watchlist_names)
        print(f"  bioRxiv preprints fetched: {len(biorxiv_articles)}")

        result = process_articles(pubmed_articles, biorxiv_articles)

        print("=== job_pubmed_biorxiv: complete ===")
        print(f"  PubMed fetched: {result['pubmed_fetched']}")
        print(f"  bioRxiv fetched: {result['biorxiv_fetched']}")
        print(f"  New signals: {result['new_signals']}")
        print(f"  Entities unresolved: {result['unresolved_count']}")
    except Exception as e:
        print(f"  ERROR: {e}")
        print("=== job_pubmed_biorxiv: failed ===")


if __name__ == "__main__":
    run()

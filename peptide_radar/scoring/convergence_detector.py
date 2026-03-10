def get_convergence_count(peptide_id, spark, days=30):
    """Count distinct source_types in signals for a peptide in last N days."""
    try:
        safe_id = peptide_id.replace("'", "''")
        row = spark.sql(f"""
            SELECT COUNT(DISTINCT source_type) AS cnt
            FROM peptide_radar.silver.signals
            WHERE peptide_id = '{safe_id}'
            AND event_date >= date_sub(current_date(), {days})
        """).first()
        return int(row["cnt"]) if row else 0
    except Exception:
        return 0

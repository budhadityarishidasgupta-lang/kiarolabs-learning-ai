from ai_manager.repo.synonym_repo import (
    get_synonym_word_aggregates,
    upsert_synonym_word_insights,
)

def main():
    # Fetch aggregated rows (read-only)
    rows = get_synonym_word_aggregates(limit=100)
    print(f"Fetched {len(rows)} rows (read-only preview)")
    # Upsert into AI insight table
    written = upsert_synonym_word_insights(rows, model_version="phase1-v1")
    print(f"Upserted {written} rows into synonym_ai_word_insights")

    # Print a few examples
    for r in rows[:10]:
        print(r)

if __name__ == "__main__":
    main()

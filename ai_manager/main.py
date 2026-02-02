# ai_manager/main.py
from repo.synonym_repo import get_synonym_word_aggregates

def main():
    rows = get_synonym_word_aggregates(limit=10)
    for r in rows:
        print(r)

if __name__ == "__main__":
    main()

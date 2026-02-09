from datetime import datetime, timezone

from ai_manager.logging.job_runs import finish_job, start_job
from ai_manager.repo.synonym_repo import (
    get_synonym_lesson_rollups,
    get_synonym_word_aggregates,
    upsert_synonym_lesson_insights,
    upsert_synonym_word_insights,
)
from ai_manager.state.checkpoints import get_checkpoint, update_checkpoint

JOB_NAME = "synonym_ai_phase1"


def main():
    job_id = start_job(JOB_NAME)

    try:
        since_ts = get_checkpoint(JOB_NAME)

        rows = get_synonym_word_aggregates(
            limit=500,
            since_ts=since_ts,
        )

        written_words = upsert_synonym_word_insights(
            rows,
            model_version="phase1-v1",
        )

        lesson_rows = get_synonym_lesson_rollups(limit=500)
        written_lessons = upsert_synonym_lesson_insights(
            lesson_rows,
            model_version="phase1-v1",
        )

        update_checkpoint(JOB_NAME, datetime.now(timezone.utc))
        finish_job(job_id, "SUCCESS")

        print(
            "Synonym AI job complete: "
            f"{written_words} word rows written, "
            f"{written_lessons} lesson rows written"
        )

    except Exception as e:
        finish_job(job_id, "FAILED", str(e))
        raise


if __name__ == "__main__":
    main()

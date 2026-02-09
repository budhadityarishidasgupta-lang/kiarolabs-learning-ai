from datetime import datetime, timezone

from ai_manager.logging.job_runs import finish_job, start_job
from ai_manager.repo.spelling_repo import (
    get_spelling_word_aggregates,
    upsert_spelling_word_insights,
)
from ai_manager.state.checkpoints import get_checkpoint, update_checkpoint

JOB_NAME = "spelling_ai_phase1"


def run_spelling_lane():
    job_id = start_job(JOB_NAME)

    try:
        since_ts = get_checkpoint(JOB_NAME)

        rows = get_spelling_word_aggregates(
            limit=500,
            since_ts=since_ts,
        )

        written_words = upsert_spelling_word_insights(
            rows,
            model_version="phase1-v1",
        )

        update_checkpoint(JOB_NAME, datetime.now(timezone.utc))
        finish_job(job_id, "SUCCESS")

        print(
            "Spelling AI job complete: "
            f"{written_words} word rows written"
        )

    except Exception as e:
        finish_job(job_id, "FAILED", str(e))
        raise


def main():
    run_spelling_lane()


if __name__ == "__main__":
    main()

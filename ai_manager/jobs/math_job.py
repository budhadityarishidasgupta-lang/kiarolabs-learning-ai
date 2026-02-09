from datetime import datetime, timezone

from ai_manager.logging.job_runs import finish_job, start_job
from ai_manager.repo.math_repo import (
    get_math_question_aggregates,
    upsert_math_question_insights,
)
from ai_manager.state.checkpoints import get_checkpoint, update_checkpoint

JOB_NAME = "math_ai_phase1"


def run_math_lane():
    job_id = start_job(JOB_NAME)

    try:
        since_ts = get_checkpoint(JOB_NAME)

        rows = get_math_question_aggregates(
            limit=500,
            since_ts=since_ts,
        )

        written_questions = upsert_math_question_insights(
            rows,
            model_version="phase1-v1",
        )

        update_checkpoint(JOB_NAME, datetime.now(timezone.utc))
        finish_job(job_id, "SUCCESS")

        print(
            "Math AI job complete: "
            f"{written_questions} question rows written"
        )

    except Exception as e:
        finish_job(job_id, "FAILED", str(e))
        raise


def main():
    run_math_lane()


if __name__ == "__main__":
    main()

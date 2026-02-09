import os
from datetime import datetime, timezone

from ai_manager.llm.client import generate_summary
from ai_manager.llm.prompts import lesson_summary_prompt
from ai_manager.logging.job_runs import finish_job, start_job
from ai_manager.repo.synonym_repo import (
    get_synonym_lesson_rollups,
    get_synonym_word_aggregates,
    update_synonym_lesson_summary,
    upsert_synonym_lesson_insights,
    upsert_synonym_word_insights,
)
from ai_manager.state.checkpoints import get_checkpoint, update_checkpoint

JOB_NAME = "synonym_ai_phase1"
ENABLE_LLM_SUMMARIES = os.getenv("ENABLE_LLM_SUMMARIES", "false").lower() == "true"


def run_synonym_summaries(lesson_rows):
    for row in lesson_rows:
        prompt = lesson_summary_prompt(row)
        summary = generate_summary(prompt)

        if summary:
            update_synonym_lesson_summary(
                user_id=row["user_id"],
                lesson_id=row["lesson_id"],
                summary_text=summary,
                model_version="phase1-v1+llm",
            )


def run_synonym_lane():
    job_id, job_run_id = start_job(JOB_NAME)

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

        if ENABLE_LLM_SUMMARIES:
            run_synonym_summaries(lesson_rows)

        update_checkpoint(JOB_NAME, datetime.now(timezone.utc))
        finish_job(
            job_id,
            status="SUCCESS",
            processed_attempts=len(rows),
            model_version="phase1-v1",
        )

        print(
            "Synonym AI job complete: "
            f"{written_words} word rows written, "
            f"{written_lessons} lesson rows written "
            f"(run_id={job_run_id})"
        )

    except Exception as e:
        finish_job(job_id, status="FAILED", error_message=str(e))
        raise


def main():
    run_synonym_lane()


if __name__ == "__main__":
    main()

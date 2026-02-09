import uuid

from ai_manager.db import get_connection


def start_job(lane: str, job_run_id: str | None = None) -> tuple[int, str]:
    """
    Start a job run for a given lane.
    job_run_id is a logical run identifier (UUID).
    """
    if job_run_id is None:
        job_run_id = str(uuid.uuid4())

    sql = """
        INSERT INTO public.platform_ai_job_runs (
            job_run_id,
            lane,
            status,
            started_at
        )
        VALUES (%s, %s, 'STARTED', NOW())
        RETURNING id
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_run_id, lane))
            job_id = cur.fetchone()[0]
        conn.commit()

    return job_id, job_run_id


def finish_job(
    job_id: int,
    status: str,
    error_message: str = None,
    processed_users: int = 0,
    processed_lessons: int = 0,
    processed_attempts: int = 0,
    model_version: str = None,
):
    sql = """
        UPDATE public.platform_ai_job_runs
        SET status = %s,
            finished_at = NOW(),
            error_message = %s,
            processed_users = %s,
            processed_lessons = %s,
            processed_attempts = %s,
            model_version = %s
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    status,
                    error_message,
                    processed_users,
                    processed_lessons,
                    processed_attempts,
                    model_version,
                    job_id,
                ),
            )
        conn.commit()

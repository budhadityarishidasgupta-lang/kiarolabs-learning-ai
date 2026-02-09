from ai_manager.db import get_connection


def start_job(lane: str) -> int:
    sql = """
        INSERT INTO public.platform_ai_job_runs (lane, status, started_at)
        VALUES (%s, 'STARTED', NOW())
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (lane,))
            job_id = cur.fetchone()[0]
        conn.commit()
    return job_id


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

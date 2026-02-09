from ai_manager.db import get_connection


def start_job(job_name: str) -> int:
    sql = """
        INSERT INTO public.platform_ai_job_runs (job_name, status)
        VALUES (%s, 'STARTED')
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_name,))
            job_id = cur.fetchone()[0]
        conn.commit()
    return job_id


def finish_job(job_id: int, status: str, error_message: str = None):
    sql = """
        UPDATE public.platform_ai_job_runs
        SET status = %s,
            finished_at = NOW(),
            error_message = %s
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status, error_message, job_id))
        conn.commit()

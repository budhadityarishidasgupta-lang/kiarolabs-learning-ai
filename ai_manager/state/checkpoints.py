from ai_manager.db import get_connection


def get_checkpoint(job_name: str):
    sql = """
        SELECT last_processed_at
        FROM public.platform_ai_job_checkpoints
        WHERE job_name = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_name,))
            row = cur.fetchone()
            return row[0] if row else None


def update_checkpoint(job_name: str, ts):
    sql = """
        INSERT INTO public.platform_ai_job_checkpoints (job_name, last_processed_at)
        VALUES (%s, %s)
        ON CONFLICT (job_name)
        DO UPDATE SET last_processed_at = EXCLUDED.last_processed_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_name, ts))
        conn.commit()

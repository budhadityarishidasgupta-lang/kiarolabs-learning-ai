from ai_manager.db import get_connection
import psycopg2

CHECKPOINT_TABLE_CANDIDATES = [
    "public.platform_ai_job_checkpoints",
    "public.platform_ai_job_checkpoint",
]


def _pick_table(conn) -> str:
    """
    Pick the first checkpoints table that exists, else default to the primary name.
    """
    with conn.cursor() as cur:
        for t in CHECKPOINT_TABLE_CANDIDATES:
            cur.execute("SELECT to_regclass(%s);", (t,))
            exists = cur.fetchone()[0]
            if exists:
                return t
    return CHECKPOINT_TABLE_CANDIDATES[0]


def _ensure_table_exists(conn, table_name: str):
    """
    Ensure the checkpoints table exists (job_name PK + last_processed_at).
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        job_name TEXT PRIMARY KEY,
        last_processed_at TIMESTAMPTZ
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def get_checkpoint(job_name: str):
    """
    Returns last_processed_at or None.
    If the table doesn't exist yet, return None (first run).
    """
    try:
        with get_connection() as conn:
            table_name = _pick_table(conn)
            sql = f"""
                SELECT last_processed_at
                FROM {table_name}
                WHERE job_name = %s
            """
            with conn.cursor() as cur:
                cur.execute(sql, (job_name,))
                row = cur.fetchone()
                return row[0] if row else None
    except psycopg2.errors.UndefinedTable:
        # Infra table missing -> treat as first run
        return None


def update_checkpoint(job_name: str, ts):
    """
    Upsert checkpoint. Creates table if missing.
    """
    with get_connection() as conn:
        table_name = _pick_table(conn)
        _ensure_table_exists(conn, table_name)

        sql = f"""
            INSERT INTO {table_name} (job_name, last_processed_at)
            VALUES (%s, %s)
            ON CONFLICT (job_name)
            DO UPDATE SET last_processed_at = EXCLUDED.last_processed_at
        """
        with conn.cursor() as cur:
            cur.execute(sql, (job_name, ts))
        conn.commit()

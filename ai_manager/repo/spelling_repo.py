from typing import Dict, List

from ai_manager.db import get_connection


def get_spelling_word_aggregates(limit: int = 500, since_ts=None) -> List[Dict]:
    """
    Aggregate spelling attempts at word level.
    Reads ONLY from spelling_attempts.
    """
    sql = """
        SELECT
            user_id,
            lesson_id,
            word AS headword,
            COUNT(*) AS attempts_total,
            COUNT(*) FILTER (WHERE is_correct = FALSE) AS attempts_incorrect,
            AVG(CASE WHEN is_correct THEN 1 ELSE 0 END) AS accuracy_rate,
            AVG(response_ms) AS avg_response_ms,
            MAX(ts) AS last_attempt_at,
            MAX(ts) FILTER (WHERE is_correct = FALSE) AS last_incorrect_at,
            (
                AVG(CASE WHEN is_correct THEN 1 ELSE 0 END) * 0.7
                + (1 - AVG(CASE WHEN is_correct THEN 1 ELSE 0 END)) * 0.3
            ) AS weakness_score
        FROM spelling_attempts
        WHERE (%(since_ts)s IS NULL OR ts > %(since_ts)s)
        GROUP BY user_id, lesson_id, word
        ORDER BY last_attempt_at DESC
        LIMIT %(limit)s
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"since_ts": since_ts, "limit": limit})
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    return [dict(zip(cols, r)) for r in rows]


def upsert_spelling_word_insights(rows: List[Dict], model_version: str = "phase1-v1") -> int:
    """
    Upsert into spelling_ai_word_insights.
    """
    sql = """
        INSERT INTO public.spelling_ai_word_insights (
            user_id,
            lesson_id,
            headword,
            attempts_total,
            attempts_incorrect,
            accuracy_rate,
            avg_response_ms,
            last_attempt_at,
            last_incorrect_at,
            weakness_score,
            evaluated_at,
            model_version
        )
        VALUES (
            %(user_id)s,
            %(lesson_id)s,
            %(headword)s,
            %(attempts_total)s,
            %(attempts_incorrect)s,
            %(accuracy_rate)s,
            %(avg_response_ms)s,
            %(last_attempt_at)s,
            %(last_incorrect_at)s,
            %(weakness_score)s,
            NOW(),
            %(model_version)s
        )
        ON CONFLICT (user_id, lesson_id, headword)
        DO UPDATE SET
            attempts_total     = EXCLUDED.attempts_total,
            attempts_incorrect = EXCLUDED.attempts_incorrect,
            accuracy_rate      = EXCLUDED.accuracy_rate,
            avg_response_ms    = EXCLUDED.avg_response_ms,
            last_attempt_at    = EXCLUDED.last_attempt_at,
            last_incorrect_at  = EXCLUDED.last_incorrect_at,
            weakness_score     = EXCLUDED.weakness_score,
            evaluated_at       = NOW(),
            model_version      = EXCLUDED.model_version;
    """

    payload = [{**r, "model_version": model_version} for r in rows]

    if not payload:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()

    return len(payload)

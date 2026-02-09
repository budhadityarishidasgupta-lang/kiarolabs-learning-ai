# ai_manager/repo/synonym_repo.py
from typing import List, Dict
from ai_manager.db import get_connection

SYNONYM_COURSE_IDS = (2, 3, 4, 5, 6, 7, 8, 9)

def get_synonym_word_aggregates(limit: int = 50) -> List[Dict]:
    """
    Read-only aggregation of Synonym attempts from legacy `attempts` table.
    Groups by user_id, lesson_id, headword.
    """
    sql = """
        SELECT
          a.user_id,
          a.lesson_id,
          a.headword,

          COUNT(*) AS attempts_total,

          SUM(
            CASE
              WHEN a.is_correct = false THEN 1
              ELSE 0
            END
          ) AS attempts_incorrect,

          1.0 - (
            SUM(
              CASE
                WHEN a.is_correct = false THEN 1
                ELSE 0
              END
            )::numeric
            / NULLIF(COUNT(*), 0)
          ) AS accuracy_rate,

          AVG(a.response_ms) AS avg_response_ms,

          MAX(a.ts) AS last_attempt_at,

          MAX(
            CASE
              WHEN a.is_correct = false THEN a.ts
              ELSE NULL
            END
          ) AS last_incorrect_at,

          (
            (1.0 - (
              SUM(
                CASE
                  WHEN a.is_correct = false THEN 1
                  ELSE 0
                END
              )::numeric
              / NULLIF(COUNT(*), 0)
            )) * 0.7
            +
            (
              SUM(
                CASE
                  WHEN a.is_correct = false THEN 1
                  ELSE 0
                END
              )::numeric
              / NULLIF(COUNT(*), 0)
            ) * 0.3
          ) AS weakness_score

        FROM attempts a
        WHERE a.course_id = ANY(%s)
          AND a.archived_at IS NULL
        GROUP BY a.user_id, a.lesson_id, a.headword
        ORDER BY weakness_score DESC
        LIMIT %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (list(SYNONYM_COURSE_IDS), limit))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

    return [dict(zip(cols, row)) for row in rows]

def upsert_synonym_word_insights(rows: list, model_version: str = "phase1-v1"):
    """
    Controlled upsert into synonym_ai_word_insights.
    Expects rows from get_synonym_word_aggregates().
    """
    sql = """
        INSERT INTO public.synonym_ai_word_insights (
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
            attempts_total      = EXCLUDED.attempts_total,
            attempts_incorrect  = EXCLUDED.attempts_incorrect,
            accuracy_rate       = EXCLUDED.accuracy_rate,
            avg_response_ms     = EXCLUDED.avg_response_ms,
            last_attempt_at     = EXCLUDED.last_attempt_at,
            last_incorrect_at   = EXCLUDED.last_incorrect_at,
            weakness_score      = EXCLUDED.weakness_score,
            evaluated_at        = NOW(),
            model_version       = EXCLUDED.model_version;
    """

    payload = []
    for r in rows:
        payload.append({
            "user_id": r["user_id"],
            "lesson_id": r["lesson_id"],
            "headword": r["headword"],
            "attempts_total": r["attempts_total"],
            "attempts_incorrect": r["attempts_incorrect"],
            "accuracy_rate": r["accuracy_rate"],
            "avg_response_ms": r["avg_response_ms"],
            "last_attempt_at": r["last_attempt_at"],
            "last_incorrect_at": r["last_incorrect_at"],
            "weakness_score": r["weakness_score"],
            "model_version": model_version,
        })

    if not payload:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()

    return len(payload)


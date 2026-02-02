# ai_manager/repo/synonym_repo.py
from typing import List, Dict
from db import get_connection

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


# ai_manager/repo/synonym_repo.py
from typing import List, Dict
from ai_manager.db import get_connection

SYNONYM_COURSE_IDS = (2, 3, 4, 5, 6, 7, 8, 9)


def get_synonym_word_aggregates(limit: int = 100, since_ts=None) -> List[Dict]:
    """
    Aggregate synonym attempts.
    If since_ts is provided, only process attempts after that timestamp.
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
        WHERE a.course_id = ANY(%(course_ids)s)
          AND a.archived_at IS NULL
          AND (%(since_ts)s IS NULL OR a.ts > %(since_ts)s)
        GROUP BY a.user_id, a.lesson_id, a.headword
        ORDER BY last_attempt_at DESC
        LIMIT %(limit)s;
    """

    params = {
        "course_ids": list(SYNONYM_COURSE_IDS),
        "since_ts": since_ts,
        "limit": limit,
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

    return [dict(zip(cols, row)) for row in rows]


def upsert_synonym_word_insights(rows: List[Dict], model_version: str = "phase1-v1") -> int:
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


def get_synonym_lesson_rollups(limit: int = 200) -> List[Dict]:
    """
    Build lesson-level rollups from synonym_ai_word_insights.
    One row per (user_id, lesson_id), with top weak headwords.
    """
    sql = """
        WITH word_ranked AS (
            SELECT
                user_id,
                lesson_id,
                headword,
                attempts_total,
                attempts_incorrect,
                accuracy_rate,
                avg_response_ms,
                last_attempt_at,
                weakness_score,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, lesson_id
                    ORDER BY weakness_score ASC, attempts_total DESC
                ) AS rn
            FROM public.synonym_ai_word_insights
        ),
        lesson_agg AS (
            SELECT
                user_id,
                lesson_id,
                SUM(attempts_total) AS attempts_total,
                AVG(accuracy_rate) AS accuracy_rate,
                AVG(avg_response_ms) AS avg_response_ms,
                MAX(last_attempt_at) AS last_attempt_at
            FROM public.synonym_ai_word_insights
            GROUP BY user_id, lesson_id
        ),
        top_words AS (
            SELECT
                user_id,
                lesson_id,
                jsonb_agg(headword ORDER BY rn) AS top_weak_headwords
            FROM word_ranked
            WHERE rn <= 5
            GROUP BY user_id, lesson_id
        )
        SELECT
            a.user_id,
            a.lesson_id,
            a.attempts_total,
            a.accuracy_rate,
            a.avg_response_ms,
            a.last_attempt_at,
            COALESCE(t.top_weak_headwords, '[]'::jsonb) AS top_weak_headwords
        FROM lesson_agg a
        LEFT JOIN top_words t
          ON t.user_id = a.user_id AND t.lesson_id = a.lesson_id
        ORDER BY a.last_attempt_at DESC
        LIMIT %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

    return [dict(zip(cols, row)) for row in rows]


def upsert_synonym_lesson_insights(rows: List[Dict], model_version: str = "phase1-v1") -> int:
    """
    Controlled upsert into synonym_ai_lesson_insights.
    Uses derived rollups from synonym_ai_word_insights.
    """
    sql = """
        INSERT INTO public.synonym_ai_lesson_insights (
            user_id,
            lesson_id,
            attempts_total,
            accuracy_rate,
            avg_response_ms,
            last_attempt_at,
            top_weak_word_ids,
            summary_text,
            evaluated_at,
            model_version
        )
        VALUES (
            %(user_id)s,
            %(lesson_id)s,
            %(attempts_total)s,
            %(accuracy_rate)s,
            %(avg_response_ms)s,
            %(last_attempt_at)s,
            %(top_weak_headwords)s,
            NULL,
            NOW(),
            %(model_version)s
        )
        ON CONFLICT (user_id, lesson_id)
        DO UPDATE SET
            attempts_total    = EXCLUDED.attempts_total,
            accuracy_rate     = EXCLUDED.accuracy_rate,
            avg_response_ms   = EXCLUDED.avg_response_ms,
            last_attempt_at   = EXCLUDED.last_attempt_at,
            top_weak_word_ids = EXCLUDED.top_weak_word_ids,
            evaluated_at      = NOW(),
            model_version     = EXCLUDED.model_version;
    """

    payload = []
    for r in rows:
        payload.append({
            "user_id": r["user_id"],
            "lesson_id": r["lesson_id"],
            "attempts_total": r["attempts_total"],
            "accuracy_rate": r["accuracy_rate"],
            "avg_response_ms": r["avg_response_ms"],
            "last_attempt_at": r["last_attempt_at"],
            "top_weak_headwords": r["top_weak_headwords"],
            "model_version": model_version,
        })

    if not payload:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()

    return len(payload)

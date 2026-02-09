# ai_manager/repo/synonym_repo.py
import json
from typing import List, Dict
from ai_manager.db import get_connection

SYNONYM_COURSE_IDS = (2, 3, 4, 5, 6, 7, 8, 9)


def get_synonym_word_aggregates(limit: int = 500, since_ts=None) -> List[Dict]:
    """
    Aggregate synonym attempts at word level.
    Source of truth: public.attempts
    Map attempts.headword -> canonical word_id (words.word_id).
    """
    sql = """
        SELECT
            a.user_id,
            a.course_id,
            a.lesson_id,
            w.word_id AS word_id,
            COUNT(*) AS attempts_total,
            COUNT(*) FILTER (WHERE a.is_correct = FALSE) AS attempts_incorrect,
            AVG(CASE WHEN a.is_correct THEN 1 ELSE 0 END) AS accuracy_rate,
            AVG(a.response_ms) AS avg_response_ms,
            MAX(a.ts) AS last_attempt_at,
            MAX(a.ts) FILTER (WHERE a.is_correct = FALSE) AS last_incorrect_at,
            (
                AVG(CASE WHEN a.is_correct THEN 1 ELSE 0 END) * 0.7
                + (1 - AVG(CASE WHEN a.is_correct THEN 1 ELSE 0 END)) * 0.3
            ) AS weakness_score
        FROM public.attempts a
        JOIN public.words w
          ON LOWER(w.headword) = LOWER(a.headword)
        WHERE a.course_id = ANY(%(course_ids)s)
          AND a.headword IS NOT NULL
          AND (%(since_ts)s IS NULL OR a.ts > %(since_ts)s)
        GROUP BY a.user_id, a.course_id, a.lesson_id, w.word_id
        ORDER BY last_attempt_at DESC
        LIMIT %(limit)s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"course_ids": list(SYNONYM_COURSE_IDS), "since_ts": since_ts, "limit": limit})
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    return [dict(zip(cols, r)) for r in rows]



def upsert_synonym_word_insights(rows: List[Dict], job_run_id: int = None, model_version: str = "phase1-v1") -> int:
    """
    Controlled upsert into synonym_ai_word_insights.
    Expects rows from get_synonym_word_aggregates().
    """
    sql = """
        INSERT INTO synonym_ai_word_insights (
            user_id,
            course_id,
            lesson_id,
            word_id,
            attempts_total,
            attempts_incorrect,
            accuracy_rate,
            avg_response_ms,
            last_attempt_at,
            last_incorrect_at,
            weakness_score,
            evaluated_at,
            model_version,
            job_run_id
        )
        VALUES (
            %(user_id)s,
            %(course_id)s,
            %(lesson_id)s,
            %(word_id)s,
            %(attempts_total)s,
            %(attempts_incorrect)s,
            %(accuracy_rate)s,
            %(avg_response_ms)s,
            %(last_attempt_at)s,
            %(last_incorrect_at)s,
            %(weakness_score)s,
            NOW(),
            %(model_version)s,
            %(job_run_id)s
        )
        ON CONFLICT (user_id, lesson_id, word_id)
        DO UPDATE SET
            attempts_total      = EXCLUDED.attempts_total,
            attempts_incorrect  = EXCLUDED.attempts_incorrect,
            accuracy_rate       = EXCLUDED.accuracy_rate,
            avg_response_ms     = EXCLUDED.avg_response_ms,
            last_attempt_at     = EXCLUDED.last_attempt_at,
            last_incorrect_at   = EXCLUDED.last_incorrect_at,
            weakness_score      = EXCLUDED.weakness_score,
            evaluated_at        = NOW(),
            model_version       = EXCLUDED.model_version,
            job_run_id          = EXCLUDED.job_run_id;
    """

    payload = []
    for row in rows:
        payload.append({
            "user_id": row["user_id"],
            "course_id": row["course_id"],
            "lesson_id": row["lesson_id"],
            "word_id": row["word_id"],
            "attempts_total": row["attempts_total"],
            "attempts_incorrect": row["attempts_incorrect"],
            "accuracy_rate": row["accuracy_rate"],
            "avg_response_ms": row["avg_response_ms"],
            "last_attempt_at": row["last_attempt_at"],
            "last_incorrect_at": row["last_incorrect_at"],
            "weakness_score": row["weakness_score"],
            "model_version": model_version,
            "job_run_id": job_run_id,
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
    One row per (user_id, lesson_id), with top weak word_ids.
    """
    sql = """
        WITH word_ranked AS (
            SELECT
                user_id,
                lesson_id,
                word_id,
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
                jsonb_agg(word_id ORDER BY rn) AS top_weak_word_ids
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
            COALESCE(t.top_weak_word_ids, '[]'::jsonb) AS top_weak_word_ids
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
            %(top_weak_word_ids)s::jsonb,
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
            "top_weak_word_ids": json.dumps(r["top_weak_word_ids"]),
            "model_version": model_version,
        })

    if not payload:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, payload)
        conn.commit()

    return len(payload)


def update_synonym_lesson_summary(
    user_id,
    lesson_id,
    summary_text,
    model_version,
):
    sql = """
        UPDATE public.synonym_ai_lesson_insights
        SET summary_text = %s,
            model_version = %s,
            evaluated_at = NOW()
        WHERE user_id = %s AND lesson_id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (summary_text, model_version, user_id, lesson_id))
        conn.commit()

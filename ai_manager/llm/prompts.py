def lesson_summary_prompt(lesson_row: dict) -> str:
    """
    Build a compact, App-Store-safe prompt.
    """
    focus_words = lesson_row.get("top_weak_word_ids") or lesson_row.get("top_weak_headwords") or []

    return f"""
Summarise this learner’s performance in neutral, parent-friendly language.

Lesson performance:
- Total attempts: {lesson_row['attempts_total']}
- Accuracy rate: {round(float(lesson_row['accuracy_rate']) * 100)}%
- Average response time: {int(lesson_row['avg_response_ms'])} ms
- Focus words: {", ".join(focus_words)}

Rules:
- Do NOT give advice
- Do NOT predict future performance
- Use calm, factual language
- 1–2 sentences only
""".strip()

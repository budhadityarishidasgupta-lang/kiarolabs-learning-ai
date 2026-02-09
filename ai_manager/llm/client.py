import os

from openai import OpenAI

OPENAI_MODEL = os.getenv("AI_SUMMARY_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

_client = None


def get_client():
    global _client

    if not OPENAI_API_KEY:
        return None

    if _client is None:
        _client = OpenAI(api_key=OPENAI_API_KEY)

    return _client


def generate_summary(prompt: str) -> str | None:
    client = get_client()
    if not client:
        return None

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You summarise learning performance clearly and neutrally.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=120,
        )
        content = resp.choices[0].message.content
        return content.strip() if content else None
    except Exception:
        return None

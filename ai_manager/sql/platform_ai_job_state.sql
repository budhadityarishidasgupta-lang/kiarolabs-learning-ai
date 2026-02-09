CREATE TABLE IF NOT EXISTS public.platform_ai_job_runs (
    id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS public.platform_ai_job_checkpoints (
    job_name TEXT PRIMARY KEY,
    last_processed_at TIMESTAMPTZ
);

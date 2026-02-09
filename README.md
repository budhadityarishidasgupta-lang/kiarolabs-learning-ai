# Kiarolabs Learning Intelligence Service

This repository contains the **offline AI / analytics layer** for WordSprint.

## Hard Rules
- No UI code
- No real-time execution
- No cross-app database access
- Reads only from app-local tables (spelling_*, math_*, synonym_*)
- Writes only to *_ai_* tables

If this service is OFF, all learning apps must continue to work normally.


## Cron-safe synonym AI job setup

Run this one-time SQL before running the job:

```sql
-- ai_manager/sql/platform_ai_job_state.sql
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
```

For existing environments with schema drift on `platform_ai_job_runs`, run:

```sql
-- ai_manager/sql/platform_ai_job_runs_schema_drift_fix.sql
```

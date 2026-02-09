-- Fix schema for platform_ai_job_runs to match AI manager expectations
-- SAFE: additive only (no data loss)

BEGIN;

-- 1) Ensure lane exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'platform_ai_job_runs'
          AND column_name = 'lane'
    ) THEN
        ALTER TABLE public.platform_ai_job_runs
        ADD COLUMN lane TEXT;
    END IF;
END$$;

-- 2) Ensure status exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'platform_ai_job_runs'
          AND column_name = 'status'
    ) THEN
        ALTER TABLE public.platform_ai_job_runs
        ADD COLUMN status TEXT;
    END IF;
END$$;

-- 3) Ensure started_at exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'platform_ai_job_runs'
          AND column_name = 'started_at'
    ) THEN
        ALTER TABLE public.platform_ai_job_runs
        ADD COLUMN started_at TIMESTAMPTZ DEFAULT NOW();
    END IF;
END$$;

-- 4) Ensure finished_at exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'platform_ai_job_runs'
          AND column_name = 'finished_at'
    ) THEN
        ALTER TABLE public.platform_ai_job_runs
        ADD COLUMN finished_at TIMESTAMPTZ;
    END IF;
END$$;

-- 5) Ensure error_message exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'platform_ai_job_runs'
          AND column_name = 'error_message'
    ) THEN
        ALTER TABLE public.platform_ai_job_runs
        ADD COLUMN error_message TEXT;
    END IF;
END$$;

COMMIT;

-- Add pgboss_job_id column to jobs table for cross-referencing with pg-boss internal job tracking
ALTER TABLE jobs ADD COLUMN pgboss_job_id UUID;

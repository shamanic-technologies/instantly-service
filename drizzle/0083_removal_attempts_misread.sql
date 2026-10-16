-- 2026-10-16: the first 11 automated SURBL removal posts were refused by SURBL
-- ("That form was submitted faster than a person can fill it in") and our
-- reader stored them as `not_listed` while DNS still listed every domain. They
-- are failures: relabelled so no surface reads them as delisted. Removal now
-- goes by email (lib/blocklist-removal.ts). Hand-written; idempotent.
UPDATE "domain_blocklist_removal_attempts"
SET "outcome" = 'failed',
    "error" = 'SURBL refused the automated form post (submitted faster than a person can fill it in); stored as not_listed by mistake'
WHERE "outcome" = 'not_listed'
  AND "attempted_at" < '2026-10-17T00:00:00Z';

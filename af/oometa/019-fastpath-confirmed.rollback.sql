-- Remove confirmed and anomaly columns to fastpath table
-- Formatted with pgformatter 3.3

BEGIN;
SELECT
    _v.unregister_patch ('019-fastpath-confirmed');
ALTER TABLE fastpath
    DROP COLUMN anomaly,
    DROP COLUMN confirmed;
COMMIT;


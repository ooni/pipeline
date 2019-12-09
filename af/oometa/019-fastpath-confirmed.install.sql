-- Add confirmed and anomaly columns to fastpath table
-- Formatted with pgformatter 3.3

BEGIN;
SELECT
    _v.register_patch ('019-fastpath-confirmed',
        ARRAY['018-fastpath'],
        NULL);
ALTER TABLE fastpath
    ADD COLUMN anomaly boolean,
    ADD COLUMN confirmed boolean;
COMMIT;


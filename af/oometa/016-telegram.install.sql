BEGIN;

select _v.register_patch( '016-telegram', ARRAY[ '015-fingerprint-fix' ], NULL );

-- Everything goes to `public` schema.

CREATE TABLE telegram (
    msm_no                int4 not null, -- references measurement,
    web_failure           text,
    web_blocking          bool,
    http_blocking         bool,
    tcp_blocking          bool,
    unreachable_endpoints int,
    accessible_endpoints  int
);

COMMENT ON TABLE telegram IS 'Features: data from `telegram` measurements';

COMMIT;

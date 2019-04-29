BEGIN;

select _v.register_patch( '016-telegram', ARRAY[ '015-fingerprint-fix' ], NULL );

-- Everything goes to `public` schema.

CREATE TABLE telegram (
    msm_no                  int4 not null, -- references measurement,
    telegram_web_failure    text,
    telegram_web_blocking   bool,
    telegram_http_blocking  bool,
    telegram_tcp_blocking   bool
);

COMMENT ON TABLE telegram IS 'Features: data from `telegram` measurements';

COMMIT;

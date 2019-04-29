BEGIN;

select _v.unregister_patch( '016-telegram');

DROP TABLE telegram;

COMMIT;

BEGIN;

select _v.unregister_patch( '006-test-lists' );

DROP SEQUENCE cat_no_seq;

DROP TABLE url_categories;

DROP SEQUENCE IF EXISTS url_no_seq;

DROP TABLE IF EXISTS urls;

DROP SEQUENCE IF EXISTS country_no_seq;

DROP TABLE IF NOT EXISTS countries;

COMMIT;

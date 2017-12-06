BEGIN;

select _v.register_patch( '006-test-lists', ARRAY[ '005-repeated-report' ], NULL );

CREATE SEQUENCE IF NOT EXISTS cat_no_seq;

CREATE TABLE IF NOT EXISTS url_categories
(
    cat_no INT NOT NULL default nextval('cat_no_seq') PRIMARY KEY,
    cat_code VARCHAR UNIQUE NOT NULL,
    cat_desc VARCHAR NOT NULL,
    cat_long_desc VARCHAR,
    cat_old_codes VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS url_no_seq;

CREATE TABLE IF NOT EXISTS url
(
    url_no INT NOT NULL default nextval('url_no_seq') PRIMARY KEY,
    url VARCHAR,
    cat_no INT,
    country_no INT,
    date_added TIMESTAMP WITH TIME ZONE,
    source VARCHAR,
    notes VARCHAR,
    active BOOLEAN,
    UNIQUE (url, country_no)
);

CREATE SEQUENCE IF NOT EXISTS country_no_seq;

CREATE TABLE IF NOT EXISTS country
(
    country_no INT NOT NULL default nextval('country_no_seq') PRIMARY KEY,
    full_name VARCHAR UNIQUE NOT NULL,
    name VARCHAR UNIQUE NOT NULL,
    alpha_2 VARCHAR(2) UNIQUE NOT NULL,
    alpha_3 VARCHAR(3) UNIQUE NOT NULL
);

COMMIT;

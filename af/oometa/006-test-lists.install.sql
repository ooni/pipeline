BEGIN;

select _v.register_patch( '006-test-lists', ARRAY[ '005-repeated-report' ], NULL );

CREATE SEQUENCE cat_no_seq;

CREATE TABLE url_categories
(
    cat_no INT NOT NULL default nextval('cat_no_seq') PRIMARY KEY,
    cat_code VARCHAR UNIQUE NOT NULL,
    cat_desc VARCHAR NOT NULL,
    cat_long_desc VARCHAR,
    cat_old_codes VARCHAR[]
);
comment on table url_categories is 'Contains the citizenlab URL category codes';

CREATE SEQUENCE url_no_seq;

CREATE TABLE urls
(
    url_no INT NOT NULL default nextval('url_no_seq') PRIMARY KEY,
    url VARCHAR NOT NULL,
    cat_no INT,
    country_no INT NOT NULL,
    date_added TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR,
    notes VARCHAR,
    active BOOLEAN NOT NULL,
    UNIQUE (url, country_no)
);

comment on table urls is 'Contains information on URLs included in the citizenlab URL list';

CREATE SEQUENCE country_no_seq;

CREATE TABLE countries
(
    country_no INT NOT NULL default nextval('country_no_seq') PRIMARY KEY,
    full_name VARCHAR UNIQUE NOT NULL,
    name VARCHAR UNIQUE NOT NULL,
    alpha_2 CHAR(2) UNIQUE NOT NULL,
    alpha_3 CHAR(3) UNIQUE NOT NULL
);

comment on table countries is 'Contains country names and ISO codes';

COMMIT;

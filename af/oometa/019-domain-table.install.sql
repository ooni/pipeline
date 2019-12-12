-- Create domain_input table
-- Formatted with pgformatter 3.3

BEGIN;

SELECT
    _v.register_patch ('019-domain-table',
        ARRAY['018-fastpath'],
        NULL);

CREATE TABLE domain_input (
    "domain" TEXT NOT NULL,
    "input" TEXT NOT NULL,
    "input_no" integer NOT NULL,
    "category_code" TEXT
);

CREATE INDEX domain_input_domain_idx ON domain_input (domain);

CREATE INDEX domain_input_category_code_idx ON domain_input (category_code);

COMMENT ON COLUMN domain_input.domain IS 'FQDN or ipaddr without http and port number';

COMMENT ON COLUMN domain_input.category_code IS 'Category from Citizen Lab and test lists or NULL';

-- Populate table with initial contents albeit not well sanitized

INSERT INTO domain_input (domain, input, input_no, category_code)
SELECT
    split_part(replace(replace(input, 'https://', ''), 'http://', ''), '/', 1),
    input,
    input_no,
    NULL
FROM
    input;

COMMIT;


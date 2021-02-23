CREATE TABLE datagen
(
    b       BOOLEAN,--          BOOLEAN	random
    c       CHAR, --         CHAR	random / sequence
    v       VARCHAR, --         VARCHAR	random / sequence
    s       STRING,--         STRING	random / sequence
    d       DECIMAL,--         DECIMAL	random / sequence
    t       TINYINT,--         TINYINT	random / sequence
    small   SMALLINT, --         SMALLINT	random / sequence
    i       INT, --         INT	random / sequence
    big     BIGINT, --         BIGINT	random / sequence
    f       FLOAT,--         FLOAT	random / sequence
    doubble DOUBLE, --         DOUBLE	random / sequence
    today   DATE,--         DATE	random	Always resolves to the current date of the local machine.
    now TIME, --         TIME	random	Always resolves to the current time of the local machine.
    ts      TIMESTAMP, --         TIMESTAMP	random	Always resolves to the current timestamp of the local machine.
    tslz    TIMESTAMP WITH LOCAL TIME ZONE --         TIMESTAMP WITH LOCAL TIMEZONE	random	Always resolves to the current timestamp of the local machine.
--     mon INTERVAL YEAR , --         INTERVAL YEAR TO MONTH	random
--     dtm INTERVAL MONTH --         INTERVAL DAY TO MONTH	random
-- ROW	random	Generates a row with random subfields.
-- ARRAY	random	Generates an array with random entries.
-- MAP	random	Generates a map with random entries.
-- MULTISET	random	Generates a multiset with random entries.
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10',
      'number-of-rows' = '10000',
      'fields.b.kind' = 'random',
      'fields.i.kind' = 'random',
      'fields.i.min' = '10000',
      'fields.i.max' = '90000',
      'fields.c.length' = '6',
      'fields.v.length' = '6',
      'fields.s.length' = '6',
      'fields.big.kind' = 'sequence',
      'fields.big.start' = '10000',
      'fields.big.end' = '20000'
      );

CREATE TABLE print WITH('connector'='print') LIKE datagen(EXCLUDING ALL);

INSERT INTO print
SELECT *
FROM datagen;
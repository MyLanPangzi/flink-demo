CREATE TABLE f1
(
    `user_id` BIGINT PRIMARY KEY NOT ENFORCED,
    `f1`      BIGINT
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'f1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'key.format' = 'json'
);
CREATE TABLE f2
(
    `user_id` BIGINT PRIMARY KEY NOT ENFORCED,
    `f2`      BIGINT
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'f2',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'key.format' = 'json'
);

CREATE TABLE dg
(
    f BIGINT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '10000'
);

BEGIN STATEMENT SET ;

INSERT INTO f1
SELECT 1,COUNT(*) FROM dg
GROUP BY 1;

INSERT INTO f2
SELECT 1,COUNT(*) FROM dg
GROUP BY 1;

END;
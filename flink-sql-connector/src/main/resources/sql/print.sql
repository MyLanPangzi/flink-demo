-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/print.html
CREATE TABLE print
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING
) WITH (
    'connector' = 'print',
    'standard-error' = 'false',
--     'sink.parallelism' = '1', -- not support 1.12.1
    'print-identifier' = 'test'
);

INSERT INTO print
SELECT 1, 'hello';

-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/elasticsearch.html
-- CREATE TABLE datagen WITH(
-- 'connector' = 'datagen',
-- 'rows-per-second' = '1',
-- 'fields.id.min' = '1',
-- 'fields.id.max' = '10'
-- ) LIKE test(EXCLUDING ALL);

CREATE TABLE test (
                      id INT,
                      cf ROW(
                          name STRING,
                          ts BIGINT
                          ),
                      PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'hbase-2.2',
      'table-name' = 'default:test',
      'zookeeper.quorum' = 'yh001:2181'
      );

CREATE TABLE datagen(
                        id INT,
                        proctime AS PROCTIME()
) WITH(
      'connector' = 'datagen',
      'rows-per-second' = '1',
      'fields.id.min' = '1',
      'fields.id.max' = '10'
      );

CREATE TABLE print(
                      id INT,
                      name STRING,
                      ts BIGINT
) WITH(
      'connector' = 'print'
      );

INSERT INTO print
SELECT d.id, t.cf.name, t.cf.ts
FROM datagen d
         LEFT JOIN test  FOR SYSTEM_TIME AS OF d.proctime AS t ON t.id = d.id;

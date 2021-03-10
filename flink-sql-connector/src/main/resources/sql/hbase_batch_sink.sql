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

INSERT INTO test
SELECT 1, ('hello', UNIX_TIMESTAMP());

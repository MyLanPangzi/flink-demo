-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/jdbc.html
CREATE TABLE users
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name VARCHAR(30)
)WITH (
     'connector' = 'jdbc',
     'username' = 'root',
     'password' = 'Yh002Yh002@',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'url' = 'jdbc:mysql://yh002:3306/flink',
     'sink.buffer-flush.max-rows' = '1',
     'sink.buffer-flush.interval' = '1s',
     'sink.max-retries' = '3',
--      'sink.parallelism' = '1', -- not support 1.12.1
     'table-name' = 'users'
     );

INSERT INTO users
SELECT 3,'flink';

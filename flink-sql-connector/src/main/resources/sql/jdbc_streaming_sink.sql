-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/jdbc.html
CREATE TABLE users
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name VARCHAR(30)
)WITH (
     'connector' = 'jdbc',
     'username' = 'root',
     'password' = 'Hdp10Hdp10@',
     'url' = 'jdbc:mysql://hdp10:3306/flink',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'table-name' = 'users'
     );

CREATE TABLE datagen WITH(
                         'connector' = 'datagen',
                         'rows-per-second' = '10',
                         'fields.id.kind' = 'random',
                         'fields.id.min' = '1',
                         'fields.id.max' = '10',
                         'fields.name.length' = '4'
                         ) LIKE users (EXCLUDING ALL);

INSERT INTO users SELECT * FROM datagen;
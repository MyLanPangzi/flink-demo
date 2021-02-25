-- insert into users (id, name) values (1,'hello'),(2,'world');
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
     'scan.partition.column' = 'id',
     'scan.partition.num' = '1',
     'scan.partition.lower-bound' = '0',
     'scan.partition.upper-bound' = '10000',
     'scan.fetch-size' = '1',
     'scan.auto-commit' = 'true',
     'table-name' = 'users'
     );
CREATE TABLE print WITH('connector'='print') LIKE users(EXCLUDING ALL);

INSERT INTO print SELECT * FROM users;

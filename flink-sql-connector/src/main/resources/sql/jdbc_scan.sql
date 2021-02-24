-- insert into users (id, name) values (1,'hello');
CREATE TABLE users
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name VARCHAR(30)
)WITH (
    'connector' = 'jdbc',
    'username' = 'root',
    'password' = 'Hdp10Hdp10@',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://hdp10:3306/flink',
    'table-name' = 'users'
);

CREATE TABLE print WITH('connector'='print') LIKE users(EXCLUDING ALL);

INSERT INTO print SELECT * FROM users;
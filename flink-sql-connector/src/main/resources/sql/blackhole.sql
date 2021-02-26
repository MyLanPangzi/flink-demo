CREATE TABLE datagen
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING
)WITH(
     'connector' = 'datagen'
     );

CREATE TABLE blackhole WITH(
                           'connector'='blackhole'
                           ) LIKE datagen (EXCLUDING ALL);

INSERT INTO blackhole
SELECT *
FROM datagen;

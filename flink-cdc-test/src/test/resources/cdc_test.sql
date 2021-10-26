SET sql-client.execution.result-mode=TABLEAU;
SET 'execution.checkpointing.interval' = '3s';

CREATE TABLE orders
(
    order_id      INT,
    order_date    TIMESTAMP(0),
    customer_name STRING,
    price         DECIMAL(10, 5),
    product_id    INT,
    order_status  BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'dev',
     'port' = '3306',
     'username' = 'hiscat',
     'password' = '!QAZ2wsx',
     'database-name' = 'flink',
     'table-name' = 'orders');

SELECT * FROM orders;
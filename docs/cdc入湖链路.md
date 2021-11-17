# CDC入湖链路

## canal -> kafka -> flink -> hudi -> hive/presto/trino/flink/spark

```mysql
CREATE USER canal IDENTIFIED BY '!QAZ2wsx';  
-- GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;

```

## maxwell -> kafka -> flink -> hudi -> hive/presto/trino/flink/spark

## debezium -> kafka -> flink -> hudi -> hive/presto/trino/flink/spark

## flink cdc -> kafka -> flink -> hudi -> hive/presto/trino/flink/spark

## flink cdc -> hudi -> hive/presto/trino/flink/spark

## canal -> kafka -> flink -> iceberg -> hive/presto/trino/flink/spark

## maxwell -> kafka -> flink -> iceberg -> hive/presto/trino/flink/spark

## debezium -> kafka -> flink -> iceberg -> hive/presto/trino/flink/spark

## flink cdc-> kafka -> flink -> iceberg -> hive/presto/trino/flink/spark

## flink cdc -> iceberg -> hive/presto/trino/flink/spark


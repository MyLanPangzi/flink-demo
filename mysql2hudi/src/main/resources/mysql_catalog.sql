set table.exec.resource.default-parallelism=1;

set execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION;
set execution.checkpointing.interval=10s;
set execution.checkpointing.mode=AT_LEAST_ONCE;
-- set execution.checkpointing.max-concurrent-checkpoints=1;
set execution.checkpointing.min-pause=10;
set execution.checkpointing.timeout=600s;
-- set execution.checkpointing.tolerable-failed-checkpoints=50;
set state.backend=hashmap;
-- set state.backend=rocksdb;
-- set state.checkpoint-storage=filesystem;
-- set state.checkpoints.dir='';
-- set state.backend.incremental=true;
-- SET 'execution.savepoint.path' = '/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab';

CREATE CATALOG mysql WITH(
    'type' = 'mysql',
    'default-database' = 'test',
    'username' = 'root',
    'password' = '!QAZ2wsx',
    'base-url' = 'jdbc:mysql://localhost:3306/'
);

CREATE CATALOG hudi WITH(
    'type' = 'hudi',
    'default-database' = 'test',
    'catalog.path' = 'file:///Users/xiebo/IdeaProjects/winter/hudi'
);

CREATE TABLE print WITH('connector'='print') LIKE mysql.test.test(EXCLUDING ALL);

-- INSERT INTO print
-- SELECT * FROM mysql.test.test;
set custom.sync-db.source.db=mysql.test;
set custom.sync-db.dest.db=hudi.test;
call com.hiscat.fink.catalog.mysql.SyncDbFunction;
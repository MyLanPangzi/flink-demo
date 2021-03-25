--sql.path file:///E:/github/flink-demo/flink-hudi-test/src/main/resources/test.sql
SET table.exec.mini-batch.enabled=true;
SET table.exec.mini-batch.allow-latency=5 s;
SET table.exec.mini-batch.size=5000;
SET table.exec.resource.default-parallelism=1;
SET table.local-time-zone=Asia/Shanghai;
-- SET table.exec.source.idle-timeout=60000 ms;
-- SET table.exec.state.ttl=600000 ms;
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- SET table.optimizer.distinct-agg.split.enabled=true;
-- SET table.generated-code.max-length=64000;
-- SET table.dynamic-table-options.enabled=true;
-- SET table.sql-dialect=hive;
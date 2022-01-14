package com.hiscat.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlSourceTest {
    public static void main(String[] args) throws Exception {
        final MySqlSource<String> src = MySqlSource.<String>builder()
            .hostname("localhost")
            .port(3306)
            .databaseList("test")
            .tableList("test\\.test")
            .username("root")
            .password("!QAZ2wsx")
            .serverId("5400")
            .includeSchemaChanges(true)
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(10_000L);

        env.fromSource(src, WatermarkStrategy.noWatermarks(), "test")
            .print();

        env.execute("cdc test");
    }
}

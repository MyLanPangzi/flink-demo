package com.hiscat.flink.ds.connector.jdbc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(Tuple2.of(2, "world"))
                .addSink(
                        JdbcSink.sink(
                                "insert into test (id, name) values (?,?)",
                                (JdbcStatementBuilder<Tuple2<Integer, String>>) (ps, t) -> {
                                    for (int i = 0; i < t.getArity(); i++) {
                                        ps.setObject(i + 1, t.getField(i));
                                    }
                                },
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername("root")
                                        .withPassword("Yh002Yh002@")
                                        .withUrl("jdbc:mysql://yh002:3306/flink")
                                        .build()
                        )
                );

        env.execute("jdbc sink");
    }
}

package com.hiscat.flink.ds.connector.jdbc

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.sql.PreparedStatement
import java.util.Properties

//https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/jdbc.html
object KafkaSrcJdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "yh001:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test1")
    env.addSource(
      new FlinkKafkaConsumer[String](
        "test", new SimpleStringSchema(), props
      ).setStartFromEarliest()
    )
      .map(e => {
        val strings = e.split(",")
        (strings(0).toInt, strings(1))
      })
      .addSink(
        JdbcSink.sink[(Int, String)](
          "insert into test (id, name)values (?,?)",
          new JdbcStatementBuilder[(Int, String)] {
            override def accept(t: PreparedStatement, u: (Int, String)): Unit = {
              for (elem <- 1 to u.productArity) {
                t.setObject(elem, u.productElement(elem - 1))
              }
            }
          },
          new JdbcExecutionOptions.Builder()
            .withBatchIntervalMs(0)
            .withBatchSize(1)
            .withMaxRetries(3)
            .build(),
          new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withDriverName("com.mysql.cj.jdbc.Driver")
            .withUrl("jdbc:mysql://yh002:3306/flink")
            .withUsername("root")
            .withPassword("Yh002Yh002@")
            .build()
        )
      )

    env.execute("kafka src jdbc sink")
  }
}

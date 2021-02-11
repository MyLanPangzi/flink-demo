package com.hiscat.flink.ds.connector.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object FlinkKafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "yh001:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test")
    props.setProperty("flink.partition-discovery.interval-millis", "10000")
    val src = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), props)
//    src.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
    env.addSource(
      src
        //        .setStartFromGroupOffsets()
        .setStartFromEarliest()
      //        .setStartFromLatest()
      //        .setStartFromTimestamp(100000)
      //        .setStartFromSpecificOffsets(map)
    )
      .print()

    env.execute("flink kafka consumer")
  }
}

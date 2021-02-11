package com.hiscat.flink.ds.connector.kafka

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromSource(
      KafkaSource.builder[String]()
        .setBootstrapServers("yh001:9092")
        .setGroupId("test")
        .setTopics("test")
        .setDeserializer(
          new KafkaRecordDeserializer[String] {
            override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], collector: Collector[String]): Unit = {
              collector.collect(new String(record.value()))
            }

            override def getProducedType: TypeInformation[String] = TypeInformation.of(classOf[String])
          }
        )
        .build(),
      WatermarkStrategy.noWatermarks(),
      "kafka"
    )
      .print()

    env.execute("kafka read")
  }
}

package com.hiscat.flink.ds.connector.file

import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, PartFileInfo}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{CheckpointRollingPolicy, DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.scala._

object BulkEncodeFormatTest {

  case class Data(x: Long, y: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(10 * 1000)

    env.fromSequence(Long.MinValue, Long.MaxValue)
      .map(e => Data(e, -e))
      .sinkTo(
        FileSink.forBulkFormat(
          new Path("D:\\github\\flink-demo\\out"),
          ParquetAvroWriters.forReflectRecord(classOf[Data])
        )
          .withBucketCheckInterval(10 * 1000)
          .withRollingPolicy(
            new OnCheckpointRollingPolicy[Data, String]()

          )
          .withOutputFileConfig(
            OutputFileConfig.builder()
              .withPartPrefix("data")
              .withPartSuffix("parquet")
              .build()
          )
          .build()
      )

    env.execute("bulk encode format")
  }
}

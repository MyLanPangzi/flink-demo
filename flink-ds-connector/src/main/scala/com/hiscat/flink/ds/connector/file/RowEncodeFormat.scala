package com.hiscat.flink.ds.connector.file

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

//https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/file_sink.html
object RowEncodeFormat {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10 * 1000)

    env.setParallelism(1)
    val path = new Path("E:\\github\\flink-demo\\out")
    env.fromSequence(Long.MinValue, Long.MaxValue)
      .sinkTo(
        FileSink.forRowFormat(path, new SimpleStringEncoder[Long]())
          .withBucketAssigner(new DateTimeBucketAssigner[Long]("yyyy-MM-dd--HH-mm"))
          .withOutputFileConfig(
            OutputFileConfig.builder()
              .withPartPrefix("long")
              .withPartSuffix(".txt")
              .build()
          )
          // this config control how long to check the withRolloverInterval
          .withBucketCheckInterval(10 * 1000)
          .withRollingPolicy(
            DefaultRollingPolicy.builder()
              .withInactivityInterval(1000)
              .withMaxPartSize(1024 * 1024 * 128)
              .withRolloverInterval(30 * 1000)
              .build()
          )
          .build()
      )

    env.execute("row encode format")
  }
}

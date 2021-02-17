package com.hiscat.flink.ds.connector.file

import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, PartFileInfo}
import org.apache.flink.streaming.api.scala._

object AvroWriterTest {

  case class Data(x: Long, y: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(100 * 1000)

    env.fromSequence(Long.MinValue, Long.MaxValue)
      .map(e => Data(e, e))
      .sinkTo(
        FileSink.forBulkFormat(
          new Path("E:\\github\\flink-demo\\out"),
          AvroWriters.forReflectRecord(classOf[Data])
        )
          .withBucketAssigner(new DateTimeBucketAssigner[Data]("yyyy-MM-dd--HH-mm"))
          .withBucketCheckInterval(1 * 1000)
          .withRollingPolicy(new CustomRollingPolicy())
          .withOutputFileConfig(
            OutputFileConfig.builder()
              .withPartPrefix("data")
              .withPartSuffix(".avro")
              .build()
          )
          .build()
      )

    env.execute("bulk encode format")
  }

  class CustomRollingPolicy extends CheckpointRollingPolicy[Data, String] {
    override def shouldRollOnEvent(partFileState: PartFileInfo[String], element: Data): Boolean = {
      // it is ok
      val r = partFileState.getSize > 100 * 1024 * 1024
      if (r) {
        println(r)
      }
      r
    }

    override def shouldRollOnProcessingTime(partFileState: PartFileInfo[String], currentTime: Long): Boolean = {
      currentTime - partFileState.getCreationTime >= 10 * 1000
    }
  }

}



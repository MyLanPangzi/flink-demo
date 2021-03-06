package com.hiscat.flink.ds.connector.file

import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.{AvroBuilder, AvroWriterFactory}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, PartFileInfo}
import org.apache.flink.streaming.api.scala._

import java.io.OutputStream

object CustomAvroWriterTest {

  case class Data(x: Long, y: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(20 * 1000)

    val factory = new AvroWriterFactory[Data](new AvroBuilder[Data]() {
      override def createWriter(out: OutputStream): DataFileWriter[Data] = {
        val schema = ReflectData.get.getSchema(classOf[Data])
        val datumWriter = new ReflectDatumWriter[Data](schema)

        val dataFileWriter = new DataFileWriter[Data](datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec)
        dataFileWriter.create(schema, out)
        dataFileWriter
      }
    })
    env.fromSequence(Long.MinValue, Long.MaxValue)
      .map(e => Data(e, e + 1))
      .sinkTo(
        FileSink.forBulkFormat(
          new Path("E:\\github\\flink-demo\\out"),
          factory
        )
          .withBucketAssigner(new DateTimeBucketAssigner[Data]("yyyy-MM-dd--HH-mm"))
          .withBucketCheckInterval(1 * 1000)
          .withRollingPolicy(new CustomRollingPolicy())
          .withOutputFileConfig(
            OutputFileConfig.builder()
              .withPartPrefix("custom")
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
      val r = partFileState.getSize > 1 * 1024 * 1024
      if (r) {
        println(r)
      }
      r
    }

    override def shouldRollOnProcessingTime(partFileState: PartFileInfo[String], currentTime: Long): Boolean = {
      currentTime - partFileState.getCreationTime >= 1 * 1000
    }
  }

}



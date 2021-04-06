package com.hiscat.flink.sql.connector

import com.hiscat.flink.sql.parser.{DmlCommand, SqlCommandParser}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.flink.CatalogLoader.HadoopCatalogLoader
import org.apache.iceberg.flink.{CatalogLoader, FlinkCatalog}

import java.util
import scala.collection.mutable

object SqlJobSubmitter {
  val SQL_PATH = "sql.path"
  val CHK_INTERVAL = "chk.interval"
  val ENABLE_ICEBERG = "enable.iceberg"

  def main(args: Array[String]): Unit = {
    val tool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    if (tool.has(CHK_INTERVAL)) {
      env.enableCheckpointing(tool.getLong(CHK_INTERVAL, 10 * 1000))
    }
    val tEnv = StreamTableEnvironment.create(env)
    if (!tool.has(SQL_PATH)) {
      return
    }
    if (tool.has(ENABLE_ICEBERG)) {
      val map = new util.HashMap[String, String]()
      map.put("warehouse", "hdfs://yh001:9820/iceberg")
      val catalog = new FlinkCatalog(
        "iceberg",
        "default",
        Namespace.empty(),
        CatalogLoader.hadoop("iceberg", new Configuration(), map),
        true
      )
      tEnv.registerCatalog("iceberg", catalog);
    }

    val path = tool.get(SQL_PATH)
    val commands = SqlCommandParser.getCommands(path)
    commands.filterNot(_.isInstanceOf[DmlCommand])
      .foreach(_.call(tEnv))

    val upsert = commands.filter(_.isInstanceOf[DmlCommand]).map(_.asInstanceOf[DmlCommand])
    if (upsert.nonEmpty) {
      val set = tEnv.createStatementSet()
      upsert.foreach(e => set.addInsertSql(e.sql))
      set.execute()
    }
  }


}

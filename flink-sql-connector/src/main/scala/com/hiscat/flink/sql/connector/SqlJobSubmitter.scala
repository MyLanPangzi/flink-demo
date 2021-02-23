package com.hiscat.flink.sql.connector

import com.hiscat.flink.sql.parser.{DmlCommand, SqlCommandParser}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object SqlJobSubmitter {
    val SQL_PATH = "sql.path"

  def main(args: Array[String]): Unit = {
    val tool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    if (!tool.has(SQL_PATH)) {
      return
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

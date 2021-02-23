package com.hiscat.flink.sql.parser

import org.apache.flink.table.api.{Table, TableEnvironment}

case class SetCommand(sql: String) extends SqlCommand(sql) {
  override def call(env: TableEnvironment): Option[Table] = {
    val split = sql.trim.split("=")
    env.getConfig.getConfiguration.setString(split(0), split(1))
    None
  }
}

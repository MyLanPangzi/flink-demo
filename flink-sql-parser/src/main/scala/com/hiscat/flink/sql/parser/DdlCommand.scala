package com.hiscat.flink.sql.parser

import org.apache.flink.table.api.{Table, TableEnvironment}

case class DdlCommand(sql: String) extends SqlCommand(sql) {
  override def call(env: TableEnvironment): Option[Table] = {
    env.executeSql(sql)
    None
  }
}

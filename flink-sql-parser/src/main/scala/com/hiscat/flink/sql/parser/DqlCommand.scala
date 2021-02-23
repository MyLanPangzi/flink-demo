package com.hiscat.flink.sql.parser

import org.apache.flink.table.api.{Table, TableEnvironment}

case class DqlCommand(sql: String) extends SqlCommand(sql) {
  override def call(env: TableEnvironment): Option[Table] = {
    Option(env.sqlQuery(sql))
  }
}

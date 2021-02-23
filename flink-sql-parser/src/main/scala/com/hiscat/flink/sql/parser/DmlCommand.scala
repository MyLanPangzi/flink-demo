package com.hiscat.flink.sql.parser

import org.apache.flink.table.api.{Table, TableEnvironment}

case class DmlCommand(sql: String) extends SqlCommand(sql) {
  override def call(env: TableEnvironment): Option[Table] = {
    None
  }
}

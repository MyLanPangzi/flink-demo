package com.hiscat.flink.sql.parser

import org.apache.flink.table.api.{Table, TableEnvironment}

abstract class SqlCommand(sql: String) {
  def call(env: TableEnvironment): Option[Table]
}

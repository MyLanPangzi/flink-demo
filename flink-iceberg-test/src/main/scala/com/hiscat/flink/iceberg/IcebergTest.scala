package com.hiscat.flink.iceberg

import com.hiscat.flink.sql.connector.SqlJobSubmitter

object IcebergTest {
  def main(args: Array[String]): Unit = {
    SqlJobSubmitter.main(args)
  }
}

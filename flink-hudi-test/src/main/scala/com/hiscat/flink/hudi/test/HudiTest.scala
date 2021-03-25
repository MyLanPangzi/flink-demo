package com.hiscat.flink.hudi.test

import com.hiscat.flink.sql.connector.SqlJobSubmitter

object HudiTest {
  def main(args: Array[String]): Unit = {
    SqlJobSubmitter.main(args)
  }
}

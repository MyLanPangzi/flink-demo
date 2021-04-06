package com.hiscat.flink.sql.parser

import java.net.URI

import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.util.IOUtils

object SqlCommandParser {
  def main(args: Array[String]): Unit = {
    getCommands("E:/github/flink-demo/flink-hudi-test/src/main/resources/test.sql ")
  }

  def getCommands(path: String): Seq[SqlCommand] = {
    val uri = URI.create(path)
    val fs = FileSystem.get(uri)
    val stream = fs.open(new Path(uri))
    val bytes = new Array[Byte](stream.available())
    IOUtils.readFully(stream, bytes, 0, bytes.length)
    val text = new String(bytes)
    text
      .split("\n")
      .filterNot(_.startsWith("--"))
      .mkString("\n")
      .split(";")
      .map(_.trim.replace("；", ";"))
      .filter(_.nonEmpty)
      .map {
        case e if e.startsWith("INSERT") => DmlCommand(e)
        case e if e.startsWith("CREATE") || e.startsWith("USE") || e.startsWith("DROP") => DdlCommand(e)
        case e if e.startsWith("SELECT") => DqlCommand(e)
        case e if e.startsWith("SET") => SetCommand(e)
        case _ => null
      }
      .filter(_ != null)
  }
}

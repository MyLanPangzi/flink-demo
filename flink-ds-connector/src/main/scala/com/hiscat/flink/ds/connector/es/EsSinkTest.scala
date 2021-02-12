package com.hiscat.flink.ds.connector.es

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest

import java.util

//https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/elasticsearch.html
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val builder = new ElasticsearchSink.Builder[(Int, String)](
      util.Arrays.asList(
        new HttpHost("yh001", 9200)
      ),
      (t: (Int, String), _: RuntimeContext, requestIndexer: RequestIndexer) => {

        val map = new util.HashMap[String, String]()
        map.put("id", t._1.toString)
        map.put("name", t._2)
        val request = new IndexRequest("test", "test", t._1.toString).source(map)
        requestIndexer.add(request)
      }
    )
//    builder.setBulkFlushBackoff(true)
//    builder.setBulkFlushBackoffDelay(2 * 1000)
//    builder.setBulkFlushBackoffRetries(10)
//    builder.setBulkFlushBackoffType(FlushBackoffType.EXPONENTIAL)
//    builder.setBulkFlushInterval(2 * 1000)
//    builder.setBulkFlushMaxActions(100)
//    builder.setBulkFlushMaxSizeMb(10)
//    builder.setFailureHandler(new RetryRejectedExecutionFailureHandler)
//    builder.setRestClientFactory(
//      (restClientBuilder: RestClientBuilder) => {
//        restClientBuilder.setDefaultHeaders(Array(new BasicHeader("n","v")))
//        restClientBuilder.setPathPrefix("/")
//      }
//    )


    env.fromElements((1, "hello"))
      .addSink(builder.build())

    env.execute("es sink test")
  }
}

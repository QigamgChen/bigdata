package flink

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import java.util

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

case class DataBean(id:String,data:String)

object Sink2ES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env.readTextFile("/root/data.txt")
    var dataStream: DataStream[DataBean] = ds.map(data => {
      var strings: Array[String] = data.split("\t")
      DataBean(strings(0), strings(1))
    })

    val esFunc = new ElasticsearchSinkFunction[DataBean] {
      override def process(element: DataBean, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

        val map = new util.HashMap[String,String]()
        map.put("id",element.id)
        map.put("data",element.data)
        val indexRequest: IndexRequest = Requests.indexRequest().index("indexTest").`type`("_doc").source(map)
        indexer.add(indexRequest)
        println("保存1条")
      }
    }
    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("128.96.104.79",9200,"http"))

    val sinkBuilder = new ElasticsearchSink.Builder[DataBean](httpHosts, esFunc)

    //刷新前缓冲的最大动作量
    sinkBuilder.setBulkFlushMaxActions(10)
    var esBulid: ElasticsearchSink[DataBean] = sinkBuilder.build()

    dataStream.addSink(esBulid)

    env.execute("sink es")
  }
}

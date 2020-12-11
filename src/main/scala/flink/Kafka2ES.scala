package flink

import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

case class DataBean(id:String,data :String)

object Kafka2ES {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "128.96.104.29:9092")
    props.setProperty("group.id", "flink-group")
    val consumer = new FlinkKafkaConsumer010[String]("topictest", new SimpleStringSchema(), props)
    var ds: DataStream[String] = env.addSource(consumer)
    var dataStream: DataStream[DataBean] = ds.map(data => {
      var strings: Array[String] = data.split("\t")
      DataBean(strings(0), strings(1))
    })
    val esFunc = new ElasticsearchSinkFunction[DataBean] {
      override def process(element: DataBean, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

        val map = new util.HashMap[String,String]()
        map.put("id",element.id)
        map.put("data",element.data)
        val indexRequest: IndexRequest = Requests.indexRequest().index("indextest").`type`("_doc").source(map)
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

    env.execute("Kafka to ES")
  }

}

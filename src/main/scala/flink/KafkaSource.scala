package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import java.util.Properties

object KafkaSource {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "128.96.104.29:9092")
    props.setProperty("group.id", "flink-group")


    val consumer = new FlinkKafkaConsumer010[String]("topictest", new SimpleStringSchema(), props)
    var stream: DataStream[String] = env.addSource(consumer)
    var result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    result.print()
    env.execute("Kafka-Flink Test")
  }

}

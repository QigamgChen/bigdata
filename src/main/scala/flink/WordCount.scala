package flink

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    var env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    var ds: DataSet[String] = env.readTextFile("")
    var words: DataSet[String] = ds.flatMap(_.split("\t"))
    var word: DataSet[(String, Int)] = words.map((_, 1))
    var result: AggregateDataSet[(String, Int)] = word.groupBy(0).sum(1)



  }

}

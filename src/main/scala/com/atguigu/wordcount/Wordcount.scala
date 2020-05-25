package com.atguigu.wordcount

import org.apache.flink.api.scala._

/**
 * 批处理
 */
object Wordcount {
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "D:\\MyWork\\WorkSpaceIDEA\\flink\\src\\main\\resources\\data.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    // 分词之后，对单词进行groupby分组，然后用sum进行聚合
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDS.print()


  }
}

package com.atguigu.source

import org.apache.flink.streaming.api.scala._

/**
 * Source从文件中读取
 */
object SourceForFile {
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val fileDstream: DataStream[String] =
      env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    fileDstream.print("source for file")

    // 执行job
    env.execute("source test job")
  }
}

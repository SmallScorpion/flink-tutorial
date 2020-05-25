package com.atguigu.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 从外部命令中获取参数
    val params: ParameterTool =  ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 全局设置并行度
    // env.setParallelism(3)
    // 接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream(host, port)

    // flatMap和Map需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] = textDstream
      .flatMap(_.split(" "))// .setParallelism(2) // 单个设置并行度
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dataStream.print().setParallelism(1)

    // 启动executor，执行任务
    env.execute("Socket stream word count")
  }

}

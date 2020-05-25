package com.atguigu.source

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._


/**
 * 从集合中获取数据
 */
object SourceForCollection {
  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从集合中读取数据
    val listDstream : DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))

    listDstream.print("stream for list").setParallelism(1)

    // 执行job
    env.execute("source test job")
  }
}

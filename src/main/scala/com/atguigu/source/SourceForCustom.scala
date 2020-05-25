package com.atguigu.source

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

/**
 * 自定义一个Source
 */
object SourceForCustom {

  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val customDstream: DataStream[SensorReading] = env.addSource( MySensorSource())

    customDstream.print("source for custom").setParallelism(4)

    env.execute("source test job")
  }
}


// 自定义生成测试数据源的SourceFunction
case class MySensorSource() extends SourceFunction[SensorReading]{

  // 定义一个标识位，用来表示数据源是否正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  // 随机生成10个传感器的温度数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 初始化一个随机数生成器
    val random = new Random()

    // 初始化10个传感器的温度值，随机生成，包装成二元组（id, temperature）
    var createTemperature: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )

    // 无限循环生成数据，如果cancel的话就停止
    while (running) {
      // 更新当前温度值，再之前温度上增加微小扰动(上下浮动的数)
      createTemperature = createTemperature.map(
        data => (data._1, data._2 + random.nextGaussian())
      )

      // 获取当前时间戳，包装样例类
      val timestamp: Long = System.currentTimeMillis()
      createTemperature.foreach(
        data => sourceContext.collect( SensorReading(data._1, timestamp, data._2))
      )

      // 间隔200ms
      Thread.sleep(200)
    }
  }
}
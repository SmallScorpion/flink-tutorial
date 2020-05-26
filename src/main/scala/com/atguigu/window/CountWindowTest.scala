package com.atguigu.window

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._


object CountWindowTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      }
    )

    val windowDStream: DataStream[SensorReading] = dataDstream
      .keyBy(0)
      .countWindow(2) // 滚动窗口
      // 滑动窗口：sliding_size设置为了2，也就是说，每收到两个相同key的数据就计算一次，每一次计算的window范围是10个元素。
      // .countWindow(10,2)
      //.reduce( MyReduceFunc() )
      .reduce((r1,r2) => SensorReading(r1.id, r1.timestamp, r1.temperature.min(r2.temperature)))


    dataDstream.print("data")
    windowDStream.print("window")

    env.execute("window test job")
  }
}

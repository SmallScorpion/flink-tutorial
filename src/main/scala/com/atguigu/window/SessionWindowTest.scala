package com.atguigu.window

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


object SessionWindowTest {
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
      // 会话窗口，超时时间间隔，超过1分钟就是下一个窗口开启
      .window( EventTimeSessionWindows.withGap(Time.seconds(1)) )
      .reduce((r1,r2) => SensorReading(r1.id, r1.timestamp, r1.temperature.min(r2.temperature)))
      

    dataDstream.print("data")
    windowDStream.print("window")

    env.execute("window test job")
  }
}

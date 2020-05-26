package com.atguigu.event

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 滚动窗口 + EventTime
 */
object TumblingEventTimeWindowsTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置watermark的默认生成周期 -> 100毫秒生成一个WaterMark
    env.getConfig.setAutoWatermarkInterval(100L)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream
      .map( data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      // 自定义一个周期性
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]
      ( Time.seconds(1000)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    val eventTimeDStream: DataStream[SensorReading] = dataDstream
      .keyBy(0)
      .timeWindow( Time.seconds(15))
      .reduce( (r1, r2) =>
        SensorReading(r1.id, (r2.temperature-r1.temperature).toLong, r1.timestamp.min(r2.timestamp).toDouble) )

    dataDstream.print("WaterMark")

    eventTimeDStream.print("EventTime")

    env.execute("eventTime test job")
  }
}

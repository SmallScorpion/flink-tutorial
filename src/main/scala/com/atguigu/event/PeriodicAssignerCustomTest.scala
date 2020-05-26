package com.atguigu.event

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

object PeriodicAssignerCustomTest {
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
      .assignTimestampsAndWatermarks( MyPeriodicAssigner() )

    dataDstream.print("WaterMark")

    env.execute("eventTime test job")

  }
}

/**
 * 自定义一个周期性的WaterMark
 */
case class MyPeriodicAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{

  // 延时为1分钟
  val bound: Long = 60 * 1000

  // 观察到的最大时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}
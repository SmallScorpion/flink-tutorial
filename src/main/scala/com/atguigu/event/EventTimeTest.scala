package com.atguigu.event

import com.atguigu.bean.SensorReading
import com.atguigu.window.MyReduceFunc
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object EventTimeTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置watermark的默认生成周期 -> 100毫秒生成一个WaterMark
    env.getConfig.setAutoWatermarkInterval(100L)

    val inputDStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val dataDstream: DataStream[SensorReading] = inputDStream
      .map( data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      // .assignAscendingTimestamps( _.timestamp * 1000L ) // 理想状态下直接指定时间戳字段就可以了
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]
        // 给WaterMark的一个初始值延时时间
        (Time.milliseconds(1000)) {
        // 指定时间戳字段以秒为单位 * 1000
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 三重保证 watermark(水位线) | allowedLateness(最大迟到数据)  | sideOutputLateData(侧输出流)
    val resultDStream: DataStream[SensorReading] = dataDstream
        .keyBy("id")
        .timeWindow( Time.seconds(5) )
        .allowedLateness( Time.minutes(1) )
        .sideOutputLateData( new OutputTag[SensorReading]("late") )
        .reduce( MyReduceFunc() )

    dataDstream.print("data")
    resultDStream.print("result")
    // 获取测输出流的late并打印
    resultDStream.getSideOutput( new OutputTag[SensorReading]("late") ).print("late")

    env.execute("eventTime test job")

  }
}

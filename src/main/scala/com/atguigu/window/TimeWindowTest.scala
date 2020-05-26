package com.atguigu.window

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindowTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      }
    )

    val windowDStream: DataStream[SensorReading] = dataDstream
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      // .timeWindow(Time.seconds(15),Time.seconds(5)) 滑动窗口，第二个参数为滑动步长
      .reduce( MyReduceFunc() )
      //  .reduce((r1, r2) => (r1._1, r1._2.min(r2._2))) // 也可以直接写，这里是自定义

    dataDstream.print("data")
    windowDStream.print("window")

    env.execute("window test job")
  }
}

case class MyReduceFunc() extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
  }
}

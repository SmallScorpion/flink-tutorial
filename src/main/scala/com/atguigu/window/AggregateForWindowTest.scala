package com.atguigu.window

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateForWindowTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      }
    )

    val windowDStream: DataStream[Double] = dataDstream
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      .aggregate( MyAggregateFunc() )

    dataDstream.print("data")
    windowDStream.print("window")

    env.execute("window test job")
  }
}

/**
 * 自定义一个求温度平均值的函数
 */
case class MyAggregateFunc() extends AggregateFunction[SensorReading, (Double, Int), Double]{

  // 状态的初始值，温度和计数都为0
  override def createAccumulator(): (Double, Int) = (0.0, 0)

  // 函数所作操作 用之前的数据加上此时的数据，计数+1
  override def add(in: SensorReading, acc: (Double, Int)): (Double, Int) =
    (in.temperature + acc._1, acc._2 + 1)

  // 获取数据的最终值
  override def getResult(acc: (Double, Int)): Double = acc._1 / acc._2

  // 可以直接写null 这里不会出现合并的现象，防止的话可以填一个聚合操作
  override def merge(acc: (Double, Int), acc1: (Double, Int)): (Double, Int) =
    (acc._1 + acc1._1, acc._2 + acc1._2)
}
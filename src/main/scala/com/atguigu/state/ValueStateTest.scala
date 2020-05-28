package com.atguigu.state

import java.lang

import com.atguigu.bean.SensorReading
import com.atguigu.window.MyReduceFunc
import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object ValueStateTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultDStrem: DataStream[SensorReading] = dataDstream
      .keyBy("id")
        .reduce( MyStateTestFunc() )

    dataDstream.print("data")

    env.execute("state test job")

  }
}


case class MyStateTestFunc() extends RichReduceFunction[SensorReading]{

  // state 定义
  lazy val myValueState: ValueState[Double] = getRuntimeContext
    .getState( new ValueStateDescriptor[Double]("myValue", classOf[Double]))

  lazy val myListState: ListState[String] = getRuntimeContext
    .getListState( new ListStateDescriptor[String]("myList", classOf[String]) )

  lazy val myMapState: MapState[String, Double] = getRuntimeContext
    .getMapState( new MapStateDescriptor[String, Double]("myMap", classOf[String], classOf[Double]))

  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext
    .getReducingState( new ReducingStateDescriptor[SensorReading]("myReduce", MyReduceFunc(), classOf[SensorReading]) )


  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {

    // 获取状态
    val myValue: Double = myValueState.value()
    val myList: lang.Iterable[String] = myListState.get()
    val myMap: Double = myMapState.get("sensor_1")
    val myReducing: SensorReading = myReducingState.get()


    // 状态写入
    myValueState.update( 0.0 )
    myMapState.put( "sensor_1", 1.0)
    myReducingState.add( t1 )

    t1
  }
}
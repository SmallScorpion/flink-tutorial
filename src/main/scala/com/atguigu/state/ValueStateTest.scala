package com.atguigu.state

import java.lang
import java.util.concurrent.TimeUnit

import com.atguigu.bean.SensorReading
import com.atguigu.window.MyReduceFunc
import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object ValueStateTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(1000L) // 开启 触发时间间隔为1000毫秒
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE) // 语义 默认EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointTimeout(60000L) // 超时时间
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 最大允许同时出现几个CheckPoint
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) // 最小得间隔时间
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true) // 是否倾向于用CheckPoint做故障恢复
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // 容忍多少次CheckPoint失败

    // 重启策略
    // 固定时间重启
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    // 失败率重启
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))


    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultDStrem: DataStream[SensorReading] = dataDstream
      .keyBy("id")
      .reduce(MyStateTestFunc())

    dataDstream.print("data")

    env.execute("state test job")

  }
}


case class MyStateTestFunc() extends RichReduceFunction[SensorReading] {

  // state 定义
  lazy val myValueState: ValueState[Double] = getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("myValue", classOf[Double]))

  lazy val myListState: ListState[String] = getRuntimeContext
    .getListState(new ListStateDescriptor[String]("myList", classOf[String]))

  lazy val myMapState: MapState[String, Double] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Double]("myMap", classOf[String], classOf[Double]))

  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext
    .getReducingState(new ReducingStateDescriptor[SensorReading]("myReduce", MyReduceFunc(), classOf[SensorReading]))


  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {

    // 获取状态
    val myValue: Double = myValueState.value()
    val myList: lang.Iterable[String] = myListState.get()
    val myMap: Double = myMapState.get("sensor_1")
    val myReducing: SensorReading = myReducingState.get()


    // 状态写入
    myValueState.update(0.0)
    myMapState.put("sensor_1", 1.0)
    myReducingState.add(t1)

    t1
  }
}
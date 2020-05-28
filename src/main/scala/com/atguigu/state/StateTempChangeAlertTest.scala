package com.atguigu.state

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTempChangeAlertTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultDStrem: DataStream[(String, Double, Double)] = dataDstream
      .keyBy("id")
      .flatMap( TempChangeAlert(10.0) )

    dataDstream.print("data")
    resultDStrem.print("result")

    env.execute("stateBackendsApp test job")
  }
}

/**
 * 获取上一次的温度进行 对比，若 两个值得温度相差10度则进行报警输出
 * @param tpr
 */
case class TempChangeAlert(tpr: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  var lastTempState: ValueState[Double] = _
  var firstId: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext
      .getState( new ValueStateDescriptor[Double]( "last_time", classOf[Double]) )

    firstId = getRuntimeContext
      .getState( new ValueStateDescriptor[Boolean]( "first_id", classOf[Boolean]) )
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

    // 获取上一次得值
    val lastTemp: Double = lastTempState.value()
    val bool: Boolean = firstId.value()
    if(bool == false){
      firstId.update(true)
    }

    // 更新状态
    lastTempState.update(value.temperature)

    // 两次得值相减得绝对值，大于传入得警告温度，则发生报警
    val diff: Double = (value.temperature - lastTemp).abs
    // 不是第一个数据，则上一次取出得数据永远是0.0,永远会输出
    if( diff >= tpr && bool == true){
      out.collect( (value.id, lastTemp, value.temperature) )
    }

  }
}
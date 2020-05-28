package com.atguigu.state

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

object FlatMapWithStateTest {
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
      //.flatMap( TempChangeAlert(10.0) )
      .flatMapWithState[(String, Double, Double), Double]({

        case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
        case (inputData: SensorReading, lastTemp: Some[Double]) => {
          val diff = (inputData.temperature - lastTemp.get).abs
          if( diff >= 10.0 ){
            ( List( (inputData.id, lastTemp.get, inputData.temperature) ), Some(inputData.temperature) )
          } else {
            ( List.empty, Some(inputData.temperature) )
          }
        }

      })

    dataDstream.print("data")
    resultDStrem.print("result")

    env.execute("stateBackendsApp test job")
  }
}

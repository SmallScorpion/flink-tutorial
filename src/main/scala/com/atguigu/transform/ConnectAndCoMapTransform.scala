package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * 合流操作，connect/comap
 */
object ConnectAndCoMapTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 打上标记
    val splitStream: SplitStream[SensorReading] = dataDstream.split(
      data => {
        if (data.temperature >= 30)
          Seq("high")
        else
          Seq("low")
      }
    )

    // 根据标记将SplitStream又转换成DataStream
    val highSensorDStream: DataStream[SensorReading] = splitStream.select("high")
    val lowSensorDStream: DataStream[SensorReading] = splitStream.select("low")

    // 为了验证connect可将两个不相同参数的流进行合并，将其中一条流进行格式转换成二元组形式
    val highWarningDStream: DataStream[(String, Double)] = highSensorDStream.map(
      data => (data.id, data.temperature)
    )

    // 将两条流进行连接
    val connectedStreams: ConnectedStreams[(String,Double),SensorReading] = highWarningDStream
      .connect(lowSensorDStream)

    // 将两条流的数据分别处理合为一条流
    val coMapDStream: DataStream[(String, Double, String)] = connectedStreams.map(
      // highWarningData是一个元组类型(String,Double)
      highWarningData => (highWarningData._1, highWarningData._2, "Wraning"),
      // 本身是一个SensorReading
      lowTempData => (lowTempData.id, lowTempData.temperature, "normal")
    )

    coMapDStream.print("coMap")

    env.execute("transform test job")
  }
}

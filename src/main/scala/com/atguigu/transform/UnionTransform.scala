package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * Union:对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream。
 */
object UnionTransform {
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
    val allSensorDStream: DataStream[SensorReading] = splitStream.select("high", "low")

    // 必须为同类型流
    val unionDStream: DataStream[SensorReading] = highSensorDStream.union(lowSensorDStream,allSensorDStream)

    unionDStream.print(" union ")

    env.execute("transform test job")


  }
}

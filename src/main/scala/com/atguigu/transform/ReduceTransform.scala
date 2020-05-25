package com.atguigu.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * Reduce:KeyedStream → DataStream：一个分组数据流的聚合操作，
 * 合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
 */
object ReduceTransform {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 3. 复杂聚合操作，reduce，得到当前id最小的温度值，以及最新的时间戳+1
    val reduceStream: DataStream[SensorReading] = dataDstream
      .keyBy("id")
      .reduce( (curState, newData) =>
        // curState是之前数据 newData是现在数据
        SensorReading( curState.id, newData.timestamp + 1, curState.temperature.min(newData.temperature)) )

    reduceStream.print("reduce")

    env.execute("reduce test job")
  }
}

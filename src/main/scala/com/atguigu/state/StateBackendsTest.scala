package com.atguigu.state

import com.atguigu.bean.SensorReading
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._

/**
 * 状态后端StateBackends
 */
object StateBackendsTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置状态后端
    env.setStateBackend( new MemoryStateBackend() ) // 只适合做测试开发
    env.setStateBackend( new FsStateBackend("") ) // 若状态过多容易OOM
    env.setStateBackend( new FsStateBackend("") ) // 需要引入包flink-statebackend-rocksdb_2.11


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

    env.execute("stateBackends test job")

  }
}

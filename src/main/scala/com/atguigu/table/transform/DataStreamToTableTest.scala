package com.atguigu.table.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object DataStreamToTableTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })



    // 常规创建
    val dataTable: Table = tableEnv.fromDataStream(dataDstream)
    // Schema基于名称
    val sensorTable = tableEnv.fromDataStream(dataDstream,
      'timestamp as 'ts, 'id as 'myId, 'temperature)
    // Schema基于位置 一一对应
    val sensorTable_lo = tableEnv.fromDataStream(dataDstream, 'myId, 'ts)


    // ------------------------  创建视图 -----------------
    tableEnv.createTemporaryView("sensorView", dataDstream)
    tableEnv.createTemporaryView("sensorView", dataDstream, 'id, 'temperature, 'timestamp as 'ts)
    tableEnv.createTemporaryView("sensorView", sensorTable)

    // 测试输出
    val resultTable: Table = dataTable
      .select('id, 'temperature) // 查询id和temperature字段
      .filter('id === "sensor_1") // 输出sensor_1得数据

    resultTable.toAppendStream[ (String, Double) ].print( "data" )


    env.execute(" table DS test job")
  }
}

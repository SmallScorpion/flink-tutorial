package com.atguigu.table

import com.atguigu.bean.SensorReading
// 隐式转换
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
// 隐式转换
import org.apache.flink.table.api.scala._

object Example {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 1. 基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2. 基于tableEnv 将流转换成表
    val dataTable: Table = tableEnv.fromDataStream(dataDstream)

    // 3. 只输出id为sensor_1的id和温度值
    // 3.1 调用table api，做转换操作
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 3.2 直接调用SQL - 写sql实现转换
    tableEnv.registerTable("dataTable", dataTable) // 注册表
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        |   id, temperature
        |from
        |   dataTable
        |where
        |   id = 'sensor_1'
        |""".stripMargin
    )

    // 4. 将表转换成流操作
    resultTable.toAppendStream[ (String, Double) ].print("table")
    resultSqlTable.toAppendStream[ (String, Double) ].print( " sql " )

    env.execute("table test job")

  }
}

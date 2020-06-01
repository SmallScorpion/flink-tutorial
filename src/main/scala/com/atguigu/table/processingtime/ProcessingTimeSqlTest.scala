package com.atguigu.table.processingtime

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object ProcessingTimeSqlTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val sinkDDL: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  pt AS PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'file:D:\MyWork\WorkSpaceIDEA\flink-tutorial\src\main\resources\SensorReading.txt',
        |  'format.type' = 'csv'
        |)
  """.stripMargin

    tableEnv.sqlUpdate(sinkDDL)

    val dataTable: Table = tableEnv.from("dataTable")
    // 查询
    val resultTable: Table = dataTable
      .select('id, 'temperature, 'pt) // 查询id和temperature字段
      .filter('id === "sensor_1") // 输出sensor_1得数据

    // 测试输出
    resultTable.toAppendStream[ (String, Double, Timestamp) ].print( "process" )

    // 查看表结构
    dataTable.printSchema()
    tableEnv.sqlUpdate(sinkDDL) // 执行 DDL

    env.execute(" table ProcessingTimeSqlTest test job")
  }
}

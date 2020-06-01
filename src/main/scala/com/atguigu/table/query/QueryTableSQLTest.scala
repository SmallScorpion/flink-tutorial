package com.atguigu.table.query

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object QueryTableSQLTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // --------------------- 读取文件数据 ---------------------------
    val filePath = "D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt"

    tableEnv.connect( new FileSystem().path(filePath) ) // 定义表的数据来源，和外部系统建立连接
      .withFormat( new Csv() ) // 定义从外部文件读取数据之后的格式化方法
      .withSchema( new Schema() // 定义表结构
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable( "inputTable" ) // 在表环境中注册一张表(创建)


    // ----------------------- 表得查询 ---------------------

    val sensorTable: Table = tableEnv.from( "inputTable" )

    tableEnv.registerTable("sensorTable", sensorTable) // 注册表
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        |   id, temperature
        |from
        |   sensorTable
        |where
        |   id = 'sensor_1'
        |""".stripMargin
    )

    val aggResultSqlTable: Table = tableEnv
      .sqlQuery("select id, count(id) as cnt from sensorTable group by id")


    // 测试输出
    resultSqlTable.toAppendStream[ (String, Double) ].print( "easy " )
    aggResultSqlTable.toRetractStream[ (String, Long) ].print( "agg" )

    env.execute(" tableSQL query test job")
  }
}

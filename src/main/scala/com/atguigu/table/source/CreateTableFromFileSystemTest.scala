package com.atguigu.table.source

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object CreateTableFromFileSystemTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // --------------------- 读取文件数据 ---------------------------
    val fileInputPath = "D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt"

    tableEnv.connect( new FileSystem().path(fileInputPath) ) // 定义表的数据来源，和外部系统建立连接
      .withFormat( new OldCsv() ) // 定义从外部文件读取数据之后的格式化方法
      .withSchema( new Schema() // 定义表结构
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable( "fileInputTable" ) // 在表环境中注册一张表(创建)


    // 查询
    val sensorTable: Table = tableEnv.from( "fileInputTable" )
    val resultTable: Table = sensorTable
        .select('id, 'temperature) // 查询id和temperature字段
        .filter('id === "sensor_1") // 输出sensor_1得数据


    // 输出到文件的表结构定义
    val fileOutPath = "D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReadingOutput.txt"

    tableEnv.connect( new FileSystem().path(fileOutPath) )
      .withFormat( new OldCsv() )
      .withSchema( new Schema()
        // 字段要与查询中select()中的字段一致
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable( "fileOutputTable" )


    // 将查询结果输出到目标表
    resultTable.insertInto("fileOutputTable")



    env.execute(" table connect fileSystem test job")
  }
}

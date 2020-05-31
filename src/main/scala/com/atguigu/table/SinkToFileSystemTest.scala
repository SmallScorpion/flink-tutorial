package com.atguigu.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object SinkToFileSystemTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // --------------------- 读取文件数据 ---------------------------
    val filePath = "D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt"

    tableEnv.connect( new FileSystem().path(filePath) ) // 定义表的数据来源，和外部系统建立连接
      .withFormat( new OldCsv() ) // 定义从外部文件读取数据之后的格式化方法
      .withSchema( new Schema() // 定义表结构
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable( "fileInputTable" ) // 在表环境中注册一张表(创建)


    val sensorTable: Table = tableEnv.from( "fileInputTable" )
    val resultTable: Table = sensorTable
      .select('id, 'temperature) // 查询id和temperature字段
      .filter('id === "sensor_1") // 输出sensor_1得数据

    resultTable.toAppendStream[ (String, Double) ].print( "FileSystem" )


    env.execute(" table connect fileSystem test job")
  }
}

package com.atguigu.table

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

object ProcessingTimeSchemaTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    tableEnv.connect( new Kafka()
      .version( "0.11" ) // 版本
      .topic( "sensor" ) // 主题
      .property("zookeeper.connect", "hadoop102:2181")
      .property("bootstrap.servers", "hadoop102:9092")
    )
      .withFormat( new Csv() ) // 新版本得Csv
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
          .field("pt", DataTypes.TIMESTAMP(3))
          .proctime()
      )
      .createTemporaryTable( "proctimeInputTable" )

    val dataTable: Table = tableEnv.from("proctimeInputTable")

    // 查询
    val resultTable: Table = dataTable
      .select('id, 'temperature, 'pt) // 查询id和temperature字段
      .filter('id === "sensor_1") // 输出sensor_1得数据

    // 测试输出
    resultTable.toAppendStream[ (String, Double, Timestamp) ].print( "process" )
    // 查看表结构
    dataTable.printSchema()

    env.execute(" table ProcessingTime test job")
  }
}

package com.atguigu.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object SinkToKafkaTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // --------------------- 消费Kafka数据 ---------------------------

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
      )
      .createTemporaryTable( "kafkaInputTable" )

    // 查询
    val sensorTable: Table = tableEnv.from( "kafkaInputTable" )
    val resultTable: Table = sensorTable
      .select('id, 'temperature) // 查询id和temperature字段
      .filter('id === "sensor_1") // 输出sensor_1得数据

    // 定义输出的Kafka结构
    tableEnv.connect( new Kafka()
      .version( "0.11" ) // 版本
      .topic( "sensor_out" ) // 主题
      .property("zookeeper.connect", "hadoop102:2181")
      .property("bootstrap.servers", "hadoop102:9092")
    )
      .withFormat( new Csv() ) // 新版本得Csv
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable( "kafkaOutputTable" )

    // 查询结果输出到Kafka中
    resultTable.insertInto("kafkaOutputTable")

    env.execute(" kafka pipeline test job")
  }
}

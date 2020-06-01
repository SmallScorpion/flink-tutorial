package com.atguigu.table.udf

import java.sql.Timestamp

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 开启事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDStream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]
      ( Time.seconds(1) ) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 用proctime定义处理时间
    val dataTable: Table = tableEnv
      .fromDataStream(dataDStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 使用自定义的hash函数，求id的哈希值
    val myHashCode = MyHashCode(1.23)

    // 查询 Table API 方式
    val resultTable: Table = dataTable
      .select('id, 'ts, myHashCode('id)) // 查询id和temperature字段


    // SQL调用方式，首先要注册表
    tableEnv.createTemporaryView("dataTable", dataTable)
    // 注册函数
    tableEnv.registerFunction("myHashCode", myHashCode)

    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, ts, myHashCode(id)
        |from dataTable
        |""".stripMargin)


    // 测试输出
    resultTable.toAppendStream[ Row ].print( "scalar" )
    resultSqlTable.toAppendStream[ Row ].print( "scalar_sql" )
    // 查看表结构
    dataTable.printSchema()

    env.execute(" table ProcessingTime test job")
  }
}

// 自定义一个求hash code的标量函数
case class MyHashCode(factor: Double) extends ScalarFunction{
  def eval( value: String ): Int ={

    (value.hashCode * factor).toInt

  }
}
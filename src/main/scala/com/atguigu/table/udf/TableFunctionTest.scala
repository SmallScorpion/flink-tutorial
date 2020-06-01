package com.atguigu.table.udf

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {
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

    // 先创建一个UDF对象
    val mySplit = MySplit("_")

    // 查询 Table API 方式
    val resultTable: Table = dataTable
      // 分割成字段
      .joinLateral( mySplit('id) as ('word, 'length) )
      .select('id, 'ts, 'word, 'length)


    // SQL调用方式，首先要注册表
    tableEnv.createTemporaryView("dataTable", dataTable)
    // 注册函数
    tableEnv.registerFunction("mySplit", mySplit)

    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, ts, word, length
        |from dataTable, lateral table(mySplit(id)) as splitid(word, length)
        |""".stripMargin)


    // 测试输出
    resultTable.toAppendStream[ Row ].print( "scalar" )
    resultSqlTable.toAppendStream[ Row ].print( "scalar_sql" )
    // 查看表结构
    dataTable.printSchema()

    env.execute(" table function test job")
  }

}

// 自定义TableFunction，实现分割字符串并统计长度(word, length)
case class MySplit( separator: String ) extends TableFunction[ (String, Int) ] {
  def eval( str: String ): Unit ={

    str.split( separator ).foreach(
        word => collector.collect( (word, word.length) )
    )

  }
}
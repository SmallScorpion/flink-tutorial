package com.atguigu.table

import java.sql.Timestamp

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object EventTimeTableTest {
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
      .fromDataStream(dataDStream, 'id, 'temperature, 'timestamp, 'ts.rowtime)

    // 查询
    val resultTable: Table = dataTable
      .select('id, 'temperature,'ts) // 查询id和temperature字段
      .filter('id === "sensor_1") // 输出sensor_1得数据

    // 测试输出
    resultTable.toAppendStream[ (String, Double, Timestamp) ].print( "event" )
    // 查看表结构
    dataTable.printSchema()

    env.execute(" table EventTimeTableTest test job")
  }
}

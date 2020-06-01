package com.atguigu.table.udf

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionTest {
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
    val avgTemp = AvgTempFunc()

    // 查询 Table API 方式
    val resultTable: Table = dataTable
      .groupBy('id)
      .aggregate( avgTemp('temperature) as 'avgTemp )
      .select('id, 'avgTemp)


    // SQL调用方式，首先要注册表
    tableEnv.createTemporaryView("dataTable", dataTable)
    // 注册函数
    tableEnv.registerFunction("avgTemp", avgTemp)

    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, avgTemp(temperature)
        |from dataTable
        |group by id
        |""".stripMargin)


    // 测试输出
    resultTable.toRetractStream[ Row ].print( "scalar" )
    resultSqlTable.toRetractStream[ Row ].print( "scalar_sql" )
    // 查看表结构
    dataTable.printSchema()

    env.execute(" table function test job")
  }


}

// 专门定义一个聚合函数的状态类，用于保存聚合状态（sum，count）
case class AvgTempAcc() {
  var sum: Double = 0.0
  var count: Int = 0
}

// 自定义一个聚合函数 求相同id的温度平均值
case class AvgTempFunc() extends AggregateFunction[Double, AvgTempAcc]{

  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum/accumulator.count

  override def createAccumulator(): AvgTempAcc =  AvgTempAcc()

  def accumulate(acc: AvgTempAcc, temp: Double): Unit ={
    acc.sum += temp
    acc.count += 1
  }

}



package com.atguigu.table.udf

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionTest {
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
    val myAggTabTemp = MyAggTabTemp()

    // 查询 Table API 方式
    val resultTable: Table = dataTable
      .groupBy('id)
      .flatAggregate( myAggTabTemp('temperature) as ('temp, 'rank) )
      .select('id, 'temp, 'rank)


    // SQL调用方式，首先要注册表
    tableEnv.createTemporaryView("dataTable", dataTable)
    // 注册函数
    tableEnv.registerFunction("myAggTabTemp", myAggTabTemp)

/*
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, temp, `rank`
        |from dataTable, lateral table(myAggTabTemp(temperature)) as aggtab(temp, `rank`)
        |group by id
        |""".stripMargin)
*/


    // 测试输出
    resultTable.toRetractStream[ Row ].print( "scalar" )
    //resultSqlTable.toAppendStream[ Row ].print( "scalar_sql" )
    // 查看表结构
    dataTable.printSchema()

    env.execute(" table ProcessingTime test job")
  }
}

// 自定义状态类
case class AggTabTempAcc() {
  var highestTemp: Double = Double.MinValue
  var secondHighestTemp: Double = Double.MinValue
}

case class MyAggTabTemp() extends TableAggregateFunction[(Double, Int), AggTabTempAcc]{
  // 初始化状态
  override def createAccumulator(): AggTabTempAcc = new AggTabTempAcc()

  // 每来一个数据后，聚合计算的操作
  def accumulate( acc: AggTabTempAcc, temp: Double ): Unit ={
    // 将当前温度值，跟状态中的最高温和第二高温比较，如果大的话就替换
    if( temp > acc.highestTemp ){
      // 如果比最高温还高，就排第一，其它温度依次后移
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if( temp > acc.secondHighestTemp ){
      acc.secondHighestTemp = temp
    }
  }

  // 实现一个输出数据的方法，写入结果表中
  def emitValue( acc: AggTabTempAcc, out: Collector[(Double, Int)] ): Unit ={
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }
}
package com.atguigu.table

import com.atguigu.bean.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._

object OldAndNewInStreamComparisonBatchTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表执行环境
    // val TableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // --------------    Old -> 老版本批流处理得方式    ---------------


    // 老版本planner的流式查询
    val oldStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance() // 创建实例  -> return new Builder()
      .useOldPlanner() // 用老版本得Planner
      .inStreamingMode() //流处理模式
      .build()
    // 创建老版本planner的表执行环境
    val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, oldStreamSettings)

    // 老版本得批处理查询
    val oldBatchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(oldBatchEnv)



    // -------------------------------    new -> 新版本批流处理得方式    -------------------------------



    // 新版本planner的流式查询
    val newStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // 用新版本得Blink得Planner(添加pom依赖)
      .inStreamingMode() // 流处理
      .build()

    val newStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, newStreamSettings)


    // 新版本得批处理查询
    val newBatchSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inBatchMode() // 批处理
      .build()

    val newBatchTableEnv: TableEnvironment = TableEnvironment.create( newBatchSettings )


    // 数据读入并转换
    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")
    val dataDStream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      }
    )

    env.execute(" table test jobs " )
  }

}

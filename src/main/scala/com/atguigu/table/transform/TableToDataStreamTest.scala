package com.atguigu.table.transform

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object TableToDataStreamTest {
  def main(args: Array[String]): Unit = {

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      // 创建表环境
      val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

      val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

      val dataDStream: DataStream[SensorReading] = inputDStream.map(
        data => {
          val dataArray: Array[String] = data.split(",")
          SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
        })


      val dataTable: Table = tableEnv.fromDataStream(dataDStream)

      // 查询
      val resultTable: Table = dataTable
        .select('id, 'temperature) // 查询id和temperature字段
        .filter('id === "sensor_1") // 输出sensor_1得数据


      // 缀加模式
      val resultAppDStream: DataStream[ (String, Double) ] = tableEnv
        .toAppendStream[ (String, Double) ](resultTable)
      // 撤回模式
      val resultRetDStream: DataStream[(Boolean, (String, Double))] = tableEnv
        .toRetractStream[ (String, Double) ](resultTable)


      // 测试输出
      resultAppDStream.print( "app" )
      resultRetDStream.print( "ret" )

      env.execute(" table DS test job")
    }
}

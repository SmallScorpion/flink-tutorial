package com.atguigu.process

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      val inputDStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

      val dataDstream: DataStream[SensorReading] = inputDStream.map(
        data => {
          val dataArray: Array[String] = data.split(",")
          SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
        })

      val resultDStrem: DataStream[String] = dataDstream
        .keyBy( _.id )
          .process( MyKeyedProcessFunction() )

      dataDstream.print("data")
      resultDStrem.print("result")

      env.execute("stateBackendsApp test job")
    }
  }
}

case class MyKeyedProcessFunction() extends KeyedProcessFunction[String, SensorReading, String]{

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {


    ctx.output( new OutputTag[String]("side"), value.id )// 输出侧输出流得方法
    ctx.getCurrentKey() // 获取Key
    ctx.timestamp() // 获取时间戳
    ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 1000) // 注册定时器

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}











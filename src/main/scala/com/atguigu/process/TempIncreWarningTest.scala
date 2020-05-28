package com.atguigu.process

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreWarningTest {
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
        .process( TempIncreWarning(10000L) )

      dataDstream.print("data")
      resultDStrem.print("result")

      env.execute("stateBackendsApp test job")
    }
  }
}

/**
 * 自定义Process Function，检测10秒之内温度连续上升
 */
case class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个ValueState，用来保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  // 定义一个状态，用来保存设置的定时器时间戳
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("cur-timer", classOf[Long]) )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上一次温度值
    val lastTemp: Double = lastTempState.value()
    val curTimer: Long = curTimerState.value()

    // 更新温度值状态
    lastTempState.update(value.temperature)

    // 将当前的温度值，跟上次的比较
    if( value.temperature > lastTemp && curTimer == 0){
      // 如果温度上升，且没有注册过定时器，那么按当前时间加10s注册定时器
      val ts = ctx.timerService().currentProcessingTime() + interval
      // 注册一个当前key 且指定当前时间就触发得定时器
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerState.update(ts)

    } else if( value.temperature < lastTemp ){
      // 如果温度下降，那么直接删除定时器，重新开始
      ctx.timerService().deleteProcessingTimeTimer(curTimer)
      curTimerState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect( "传感器 " + ctx.getCurrentKey + " 的温度值连续" + interval / 1000 + "秒上升" )
    // 清空timer状态
    curTimerState.clear()
  }
}

package com.atguigu.sink

import com.atguigu.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkToRedis {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 连接池配置对象
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()    // 创建对象

    dataDstream.addSink( new RedisSink[SensorReading](config, MyRedisMapper()))

    env.execute("sink test job")
  }
}

// 自定义一个RedisMapper
case class MyRedisMapper() extends RedisMapper[SensorReading]{

  // 定义写入redis的命令，HSET sensor_temp key value
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

  override def getKeyFromData(data: SensorReading): String = data.id

  override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
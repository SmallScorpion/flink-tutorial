package com.atguigu.source

import java.util.Properties


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
 * 以kafka消息队列的数据作为来源
 */
object SourceForKafka {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 先创建kafka的相关配置
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaDstream:DataStream[String] = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    kafkaDstream.print("source for kafka")

    env.execute("source test job")
  }
}

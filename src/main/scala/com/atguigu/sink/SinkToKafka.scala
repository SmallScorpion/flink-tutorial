package com.atguigu.sink

import java.util.Properties

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * 完整的kafka source生成数据 经过flink的transform转换结构， 最后还是输出到kafka sink
 */
object SinkToKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从kafka读取数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 在kafka的sensor的topic发送数据 获取到flink中
    val inputDStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties)
    )
    // 转换操作
    val dataDstream: DataStream[String] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
      })

    // 将数据传输到kafka的sink_test中
    dataDstream.addSink(
      new FlinkKafkaProducer011[String]("hadoop102:9092","sink_test", new SimpleStringSchema())
    )

    env.execute("sink test job")
  }
}

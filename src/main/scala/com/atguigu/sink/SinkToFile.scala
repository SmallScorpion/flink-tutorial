package com.atguigu.sink


import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object SinkToFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[String] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString()
      })

     // 直接写入文件
    //dataDstream.writeAsText("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\out")
    // 上面方法要被丢弃，新方法用addSink，输出的文件带时间
    dataDstream.addSink( StreamingFileSink.forRowFormat[String](
          new Path("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\out"),
          new SimpleStringEncoder[String]("UTF-8")
        ).build() )

    env.execute("sink test job")
  }
}

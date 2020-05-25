package com.atguigu.sink

import java.util

import com.atguigu.bean.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * Elasticsearch
 */
object SinkToES {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 定义httphosts
    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    // 添加host和port
    httpHosts.add( new HttpHost("hadoop102", 9200) )

    // 定义ElasticsearchSinkFunction
    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading]() {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        // 首先定义写入es的source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", element.id)
        dataSource.put("temp", element.temperature.toString)
        dataSource.put("ts", element.timestamp.toString)

        // 创建index(表)
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("data")
          .source(dataSource)

        // 使用RequestIndexer发送http请求
        indexer.add(indexRequest)

        println("data " + element + " saved successfully")
      }
    }

    dataDstream.addSink( new ElasticsearchSink.Builder[SensorReading](httpHosts, esSinkFunc).build())


    env.execute("sink test job")
  }
}

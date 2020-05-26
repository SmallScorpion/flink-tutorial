package com.atguigu.sink



import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bean.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object SinkToJDBC {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.readTextFile("D:\\MyWork\\WorkSpaceIDEA\\flink-tutorial\\src\\main\\resources\\SensorReading.txt")

    val dataDstream: DataStream[SensorReading] = inputDStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    dataDstream.addSink( MyJdbcSink() )


    dataDstream.print("mysql")

    env.execute("sink test job")
  }
}

case class MyJdbcSink() extends RichSinkFunction[SensorReading]{

  // 声明连接变量
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 创建连接和预编译语句
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink","root","000000")
    insertStmt = conn.prepareStatement("insert into sensor_temp(id,temperature) values(?,?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
  }

  // 每来一条数据，就调用连接，执行一次sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 直接执行udate语句，如果没有更新数据，那么执行insert
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }


}

package org.bigdate.etl.common.scala.test.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.bigdata.etl.common.context.ETLContext
import org.junit.Test

/**
 * Author: GL
 * Date: 2022-04-28
 */
class FlinkETLTest {

  private val flink: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  private val jsonStr = "{\"source\":{\"processType\":\"sensor\"},\"middle\":[{\"processType\":\"window\"}],\"sink\":[{\"processType\":\"print\"}]}"

  @Test
  @throws[Exception]
  def start(): Unit = {
    new ETLContext[StreamExecutionEnvironment](classOf[FlinkETLTest], flink, jsonStr)
    flink.execute("Compute average sensor temperature")
  }

}

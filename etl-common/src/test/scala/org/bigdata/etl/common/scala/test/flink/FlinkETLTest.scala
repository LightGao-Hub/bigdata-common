package org.bigdata.etl.common.scala.test.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.bigdata.etl.common.context.ETLContext
import org.junit.Test

/**
 * Author: GL
 * Date: 2022-04-28
 */
class FlinkETLTest {

  private val flink: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  private val etlStr = """{
                         |	"source": {
                         |		"processType": "sensor"
                         |	},
                         |	"transform": [{
                         |		"processType": "window"
                         |	}],
                         |	"sink": [{
                         |		"processType": "print"
                         |	}]
                         |}""".stripMargin
  private var etl: ETLContext[StreamExecutionEnvironment] = _

  @Test
  @throws[Exception]
  def start(): Unit = {
    try{
      etl = new ETLContext[StreamExecutionEnvironment](classOf[FlinkETLTest], flink, etlStr)
      etl.start()
      flink.execute("Compute average sensor temperature")
    } catch {
      case ex: Throwable =>
        throw ex
    } finally {
      if (etl != null) {
        etl.close()
      }
    }
  }

}

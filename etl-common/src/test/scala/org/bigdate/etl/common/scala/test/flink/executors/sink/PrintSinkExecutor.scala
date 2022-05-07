package org.bigdate.etl.common.scala.test.flink.executors.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.configs.{ExecutorConfig, NilExecutorConfig}
import org.bigdata.etl.common.executors.SinkExecutor
import org.bigdate.etl.common.scala.test.flink.model.SensorReading

/**
 *  打印流数据
 *
 * Author: GL
 * Date: 2022-05-07
 */
@ETLExecutor("print")
class PrintSinkExecutor extends SinkExecutor[StreamExecutionEnvironment, DataStream[SensorReading], NilExecutorConfig] {

  override def init(engine: StreamExecutionEnvironment, config: NilExecutorConfig): Unit = {}

  override def process(engine: StreamExecutionEnvironment, value: DataStream[SensorReading], config: NilExecutorConfig): Unit = {
    value.print()
  }

  override def close(engine: StreamExecutionEnvironment, config: NilExecutorConfig): Unit = {}

  override def check(config: NilExecutorConfig): Boolean = {
    true
  }

}

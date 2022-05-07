package org.bigdate.etl.common.scala.test.flink.executors.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.configs.NilExecutorConfig
import org.bigdata.etl.common.executors.SourceExecutor
import org.bigdate.etl.common.scala.test.flink.model.SensorReading
import org.bigdate.etl.common.scala.test.flink.operator.{SensorGenerator, SensorTimeAssigner}

/**
 *  此source不需要配置
 *
 * Author: GL
 * Date: 2022-05-07
 */
@ETLExecutor("sensor")
class SensorSourceExecutor extends SourceExecutor[StreamExecutionEnvironment, DataStream[SensorReading], NilExecutorConfig] {

  override def init(engine: StreamExecutionEnvironment, config: NilExecutorConfig): Unit = {
    engine.getConfig.setAutoWatermarkInterval(1000L)
  }

  override def process(engine: StreamExecutionEnvironment, config: NilExecutorConfig): DataStream[SensorReading] = {
    val sensorData: DataStream[SensorReading] = engine
      .addSource(new SensorGenerator)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    sensorData
  }

  override def close(engine: StreamExecutionEnvironment, config: NilExecutorConfig): Unit = {
  }

  override def check(config: NilExecutorConfig): Boolean = {
    true
  }
}

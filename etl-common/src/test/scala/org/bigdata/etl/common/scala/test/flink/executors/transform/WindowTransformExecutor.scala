package org.bigdata.etl.common.scala.test.flink.executors.transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.configs.NilExecutorConfig
import org.bigdata.etl.common.executors.TransformExecutor
import org.bigdata.etl.common.scala.test.flink.model.SensorReading
import org.bigdata.etl.common.scala.test.flink.operator.TemperatureAverager

/**
 * Author: GL
 * Date: 2022-05-07
 */
@ETLExecutor("window")
class WindowTransformExecutor extends TransformExecutor[StreamExecutionEnvironment, DataStream[SensorReading], DataStream[SensorReading], NilExecutorConfig] {

  override def init(engine: StreamExecutionEnvironment, config: NilExecutorConfig): Unit = {
  }

  override def process(engine: StreamExecutionEnvironment, value: DataStream[SensorReading], config: NilExecutorConfig): DataStream[SensorReading] = {
    val avgTemp: DataStream[SensorReading] = value
      .map(r => SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new TemperatureAverager)
    avgTemp
  }

  override def close(engine: StreamExecutionEnvironment, config: NilExecutorConfig): Unit = {
  }

  override def check(config: NilExecutorConfig): Boolean = {
    true
  }
}

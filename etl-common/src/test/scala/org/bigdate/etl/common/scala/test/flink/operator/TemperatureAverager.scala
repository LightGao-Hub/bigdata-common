package org.bigdate.etl.common.scala.test.flink.operator

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.bigdate.etl.common.scala.test.flink.model.SensorReading

/**
 * Author: GL
 * Date: 2022-05-07
 */
class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  override def apply( sensorId: String,
                      window: TimeWindow,
                      vals: Iterable[SensorReading],
                      out: Collector[SensorReading]): Unit = {

    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}

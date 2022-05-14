package org.bigdata.etl.common.scala.test.flink.operator

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.bigdata.etl.common.scala.test.flink.model.SensorReading

class SensorTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  override def extractTimestamp(r: SensorReading): Long = r.timestamp

}

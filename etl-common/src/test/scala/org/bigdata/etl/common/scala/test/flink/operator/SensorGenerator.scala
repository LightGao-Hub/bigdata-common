package org.bigdata.etl.common.scala.test.flink.operator

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.bigdata.etl.common.scala.test.flink.model.SensorReading

import scala.collection.immutable
import scala.util.Random


/**
  * 使用Flink SourceFunction生成具有随机温度值的传感器读数。 注意，此类继承于RichParallelSourceFunction  这是一个可以并发执行的数据源
  * 源的每个并行实例模拟10个传感器，每个传感器发出一个传感器每100毫秒读一次。
  * 注意:这是一个简单的数据生成源函数，不检查其状态。
  * 如果发生故障，源不会重播任何数据。
  */
class SensorGenerator extends RichParallelSourceFunction[SensorReading] {

  var running: Boolean = true

  override def run(srcCtx: SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    var curFTemp: immutable.IndexedSeq[(String, Double)] = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }
    while (running) {
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      val curTime = Calendar.getInstance.getTimeInMillis
      curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}

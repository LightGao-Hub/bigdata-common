package org.bigdata.listener.common.scala.test.bus

import org.bigdata.listener.common.bus.ListenerBus
import org.bigdata.listener.common.scala.test.events._
import org.bigdata.listener.common.scala.test.listeners.SparkListenerInterface
import org.slf4j.{Logger, LoggerFactory}

/**
 * 强烈建议事件总线子类是单例模式
 *
 * Author: GL
 * Date: 2022-12-16
 */
object SparkListenerBus extends ListenerBus[SparkListenerInterface, SparkListenerEvent]("SparkListenerBus") {

  override protected def doPostEvent(listener: SparkListenerInterface, event: SparkListenerEvent): Unit = {
    event match {
      case jobStart: SparkListenerJobStart =>
        listener.onJobStart(jobStart)
      case jobEnd: SparkListenerJobEnd =>
        listener.onJobEnd(jobEnd)
      case taskStart: SparkListenerTaskStart =>
        listener.onTaskStart(taskStart)
      case taskEnd: SparkListenerTaskEnd =>
        listener.onTaskEnd(taskEnd)
      case _ => listener.onOtherEvent(event)
    }
  }

  override protected def onError(e: Throwable): Unit = {
    logger.error("doPostEvent error", e)
  }
}

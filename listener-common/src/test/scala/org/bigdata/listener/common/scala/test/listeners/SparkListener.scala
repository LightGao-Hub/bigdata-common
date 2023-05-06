package org.bigdata.listener.common.scala.test.listeners

import org.bigdata.listener.common.events.Event
import org.bigdata.listener.common.scala.test.events._

/**
 * 实现类方便子类重写
 *
 * Author: GL
 * Date: 2022-12-16
 */
class SparkListener extends SparkListenerInterface {

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  override def onOtherEvent(e: Event): Unit = {

  }
}

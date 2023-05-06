package org.bigdata.listener.common.scala.test.listeners

import org.bigdata.listener.common.listeners.ListenerInterface
import org.bigdata.listener.common.scala.test.events._

/**
 * Author: GL
 * Date: 2022-12-16
 */
trait SparkListenerInterface extends ListenerInterface {

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  def onJobStart(jobStart: SparkListenerJobStart): Unit

}

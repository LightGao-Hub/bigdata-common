package org.bigdata.listener.common.scala.test.events

import org.bigdata.listener.common.events.Event

/**
 * Author: GL
 * Date: 2022-12-16
 */
trait SparkListenerEvent extends Event {
}

case class SparkListenerJobEnd(jobId: Int,
                               time: Long = System.currentTimeMillis) extends SparkListenerEvent

case class SparkListenerJobStart(jobId: Int,
                                 time: Long = System.currentTimeMillis) extends SparkListenerEvent

case class SparkListenerTaskEnd(taskId: Int,
                                time: Long = System.currentTimeMillis) extends SparkListenerEvent

case class SparkListenerTaskStart(taskId: Int,
                                  time: Long = System.currentTimeMillis) extends SparkListenerEvent

case class SparkListenerStageEnd(stageId: Int,
                                  time: Long = System.currentTimeMillis) extends SparkListenerEvent

case class SparkListenerStageStart(stageId: Int,
                                   time: Long = System.currentTimeMillis) extends SparkListenerEvent

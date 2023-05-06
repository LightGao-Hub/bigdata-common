package org.bigdata.listener.common.scala.test.listeners

import org.slf4j.{Logger, LoggerFactory}
import org.bigdata.listener.common.scala.test.events._

/**
 *  sparkTask监听器
 *
 * Author: GL
 * Date: 2022-12-16
 */
class SparkTaskListener extends SparkListener {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    logger.info(s" SparkTaskListener-onTaskEnd-TaskEnd:$taskEnd ")
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    logger.info(s" SparkTaskListener-onTaskStart-TaskStart:$taskStart ")
  }
}

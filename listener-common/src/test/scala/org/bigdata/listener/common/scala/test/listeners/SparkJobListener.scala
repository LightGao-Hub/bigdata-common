package org.bigdata.listener.common.scala.test.listeners

import org.slf4j.{Logger, LoggerFactory}

import org.bigdata.listener.common.scala.test.events._

/**
 *  sparkjob监听器
 *
 * Author: GL
 * Date: 2022-12-16
 */
class SparkJobListener extends SparkListener {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info(s" SparkJobListener-onJobEnd-jobEnd:$jobEnd ")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.info(s" SparkJobListener-onJobStart-jobStart:$jobStart ")
  }
}

package org.bigdata.listener.common.scala.test


import org.bigdata.listener.common.scala.test.bus.SparkListenerBus
import org.bigdata.listener.common.scala.test.events._
import org.bigdata.listener.common.scala.test.listeners.{SparkJobListener, SparkTaskListener}
import org.junit.{After, Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random


/**
 * Author: GL
 * Date: 2022-12-16
 */
class SparkListenerTest {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val events = List(SparkListenerJobEnd(1), SparkListenerJobStart(1), SparkListenerTaskEnd(2), SparkListenerTaskStart(2))
  val rand = new Random

  @Before
  def init(): Unit = {
    SparkListenerBus.addListener(new SparkJobListener)
    SparkListenerBus.addListener(new SparkTaskListener)
    SparkListenerBus.start()
    logger.info("inited")
  }

  @Test
  def start(): Unit = {
    for (_ <- 1 to events.size * 2) {
      SparkListenerBus.post(getEvents)
      Thread.sleep(1000)
    }
  }

  private def getEvents : SparkListenerEvent =  {
    events(rand.nextInt(events.size))
  }

  @After
  def stop(): Unit = {
    SparkListenerBus.stop()
  }

}

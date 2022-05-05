package org.bigdate.etl.common.scala.test.spark.executors.middle

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.configs.DirtyConfig
import org.bigdata.etl.common.executors.MiddleExecutor
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-05-05
 */
@ETLExecutor("dirty")
class DirtyMiddleExecutor extends MiddleExecutor[SparkContext, RDD[Integer], DirtyConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[DirtyMiddleExecutor])

  override def init(engine: SparkContext, config: DirtyConfig): Unit = {
    log.info("DirtyMiddle init, config: {}", config)
  }

  override def process(dataCollection: util.Collection[RDD[Integer]], config: DirtyConfig): util.Collection[RDD[Integer]] = {
    log.info("DirtyMiddle process, config: {}", config)
    val first = dataCollection.stream.findFirst
    log.info("middle collect: {}", first.get().collect())
    dataCollection
  }

  override def close(engine: SparkContext, config: DirtyConfig): Unit = {
    log.info("DirtyMiddleExecutor close, config: {}", config)
  }

  override def check(config: DirtyConfig): Boolean = {
    log.info("DirtyMiddleExecutor check, config: {}", config)
    true
  }
}

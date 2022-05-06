package org.bigdate.etl.common.scala.test.spark.executors.middle

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.executors.MiddleExecutor
import org.bigdate.etl.common.scala.test.spark.configs.DirtyConfig
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-05-05
 */
@ETLExecutor("dirty")
class DirtyMiddleExecutor extends MiddleExecutor[SparkSession, RDD[String], DataFrame, DirtyConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[DirtyMiddleExecutor])

  override def init(engine: SparkSession, config: DirtyConfig): Unit = {
    log.info("DirtyMiddle init, config: {}", config)
  }

  override def process(engine: SparkSession, value: RDD[String], config: DirtyConfig): DataFrame = {
    import engine.implicits._
    value.toDF()
  }

  override def close(engine: SparkSession, config: DirtyConfig): Unit = {
    log.info("DirtyMiddleExecutor close, config: {}", config)
  }

  override def check(config: DirtyConfig): Boolean = {
    log.info("DirtyMiddleExecutor check, config: {}", config)
    true
  }
}

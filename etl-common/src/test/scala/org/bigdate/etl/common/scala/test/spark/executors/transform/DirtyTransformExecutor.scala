package org.bigdate.etl.common.scala.test.spark.executors.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.enums.CommonConstants
import org.bigdata.etl.common.executors.TransformExecutor
import org.bigdate.etl.common.scala.test.spark.configs.DirtyConfig
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-05-05
 */
@ETLExecutor("dirty")
class DirtyTransformExecutor extends TransformExecutor[SparkSession, RDD[String], DataFrame, DirtyConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[DirtyTransformExecutor])

  override def init(engine: SparkSession, config: DirtyConfig): Unit = {
    log.info("DirtyTransform init, config: {}", config)
  }

  override def process(engine: SparkSession, value: RDD[String], config: DirtyConfig): DataFrame = {
    import engine.implicits._
    val dirtyRdd = value.filter(v => v.split(CommonConstants.SPLIT_STRING).length == CommonConstants.FIRST)
    dirtyRdd.saveAsTextFile(config.dirtyPath) // 将脏数据存储至dirtyPath
    value.toDF()
  }

  override def close(engine: SparkSession, config: DirtyConfig): Unit = {
    log.info("DirtyTransformExecutor close, config: {}", config)
  }

  override def check(config: DirtyConfig): Boolean = {
    log.info("DirtyTransformExecutor check, config: {}", config)
    true
  }
}

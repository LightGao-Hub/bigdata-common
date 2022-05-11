package org.bigdate.etl.common.scala.test.spark.executors.source


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.enums.CommonConstants
import org.bigdata.etl.common.executors.SourceExecutor
import org.bigdate.etl.common.scala.test.spark.configs.FileConfig
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-04-28
 */
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[FileSourceExecutor])

  override def init(engine: SparkSession, config: FileConfig): Unit = {
    log.info("FileSourceExecutor init, config: {}", config)
  }

  override def process(engine: SparkSession, config: FileConfig): RDD[String] = {
    log.info("FileSourceExecutor process, config: {}", config)
    engine.sparkContext.textFile(config.path, CommonConstants.SECOND)
  }

  override def close(engine: SparkSession, config: FileConfig): Unit = {
    log.info("FileSourceExecutor close, config: {}", config)
  }

  override def check(config: FileConfig): Boolean = {
    log.info("FileSourceExecutor check, config: {}", config.path)
    true
  }

}

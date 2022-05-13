package org.bigdata.etl.common.scala.test.spark.executors.sink

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.executors.SinkExecutor
import org.bigdata.etl.common.scala.test.spark.configs.FileConfig
import org.bigdata.etl.common.scala.test.spark.executors.source.FileSourceExecutor
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-05-05
 */
@ETLExecutor("file")
class FileSinkExecutor extends SinkExecutor[SparkSession, DataFrame, FileConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[FileSourceExecutor])

  override def init(engine: SparkSession, config: FileConfig): Unit = {
    log.info("FileSinkExecutor init, config: {}", config)
  }

  override def process(engine: SparkSession, value: DataFrame, config: FileConfig): Unit = {
    log.info("FileSinkExecutor process, config: {}", config)
    value.rdd.saveAsTextFile(config.path)
  }

  override def close(engine: SparkSession, config: FileConfig): Unit = {
    log.info("FileSinkExecutorExecutor close, config: {}", config)
  }

  override def check(config: FileConfig): Boolean = {
    log.info("FileSinkExecutorExecutor check, config: {}", config)
    true
  }
}

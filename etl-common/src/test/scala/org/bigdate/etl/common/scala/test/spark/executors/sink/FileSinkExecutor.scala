package org.bigdate.etl.common.scala.test.spark.executors.sink

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.configs.FileConfig
import org.bigdata.etl.common.executors.SinkExecutor
import org.bigdate.etl.common.scala.test.spark.executors.source.FileSourceExecutor
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-05-05
 */
@ETLExecutor("file")
class FileSinkExecutor extends SinkExecutor[SparkContext, RDD[Integer], FileConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[FileSourceExecutor])

  override def init(engine: SparkContext, config: FileConfig): Unit = {
    log.info("FileSinkExecutor init, config: {}", config)
  }

  override def process(data: util.Collection[RDD[Integer]], config: FileConfig): Unit = {
    log.info("FileSinkExecutor process, config: {}", config)
    val rdd: RDD[Integer] = data.stream.findFirst.get()
    rdd.saveAsTextFile(config.getPath)
  }

  override def close(engine: SparkContext, config: FileConfig): Unit = {
    log.info("FileSinkExecutorExecutor close, config: {}", config)
  }

  override def check(config: FileConfig): Boolean = {
    log.info("FileSinkExecutorExecutor check, config: {}", config)
    true
  }
}

package org.bigdate.etl.common.scala.test.spark.executors.source

import java.util
import java.util.Collections

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bigdata.etl.common.annotations.ETLExecutor
import org.bigdata.etl.common.configs.FileConfig
import org.bigdata.etl.common.executors.SourceExecutor
import org.slf4j.{Logger, LoggerFactory}

/**
 * Author: GL
 * Date: 2022-04-28
 */
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkContext, RDD[String], FileConfig] {

  val log: Logger = LoggerFactory.getLogger(classOf[FileSourceExecutor])

  override def init(engine: SparkContext, config: FileConfig): Unit = {
    log.info("FileSourceExecutor init, config: {}", config)
  }

  override def process(engine: SparkContext, config: FileConfig): util.Collection[RDD[String]] = {
    log.info("FileSourceExecutor process, config: {}", config)
    val stringRDD: RDD[String] = engine.textFile(config.getPath, 2)
    val integerRdd = stringRDD.map(v => v.split(",")(0))
    Collections.singleton(integerRdd)
  }

  override def close(engine: SparkContext, config: FileConfig): Unit = {
    log.info("FileSourceExecutor close, config: {}", config)
  }

  override def check(config: FileConfig): Boolean = {
    log.info("FileSourceExecutor check, config: {}", config.getPath)
    true
  }

}

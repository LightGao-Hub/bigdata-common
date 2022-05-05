package org.bigdate.etl.common.scala.test.spark

import java.io.{File, IOException}
import java.util.Objects

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bigdata.etl.common.context.ETLContext
import org.junit.{Before, Test}


/**
 *  反射对于函数的参数类型泛型会自动强转
 *
 * Author: GL
 * Date: 2022-04-28
 */
class SparkETLTest {

  private var sc: SparkContext = _
  private val inputPath = Objects.requireNonNull(classOf[SparkETLTest].getClassLoader.getResource("input.txt")).getPath
  private val outPutPath = new File(inputPath).getParent.concat("/out")
  private val jsonStr = "{\"source\":{\"processType\":\"file\",\"path\":\""+inputPath+"\"},\"middle\":[{\"processType\":\"dirty\",\"dirtyPath\":\"/dirty\"}],\"sink\":[{\"processType\":\"file\",\"path\":\""+outPutPath+"\"}]}"

  @Before
  @throws[IOException]
  def init(): Unit = {
    val conf = new SparkConf().setAppName("test scala etl").setMaster("local[2]")
    sc = new SparkContext(conf)
    FileUtils.deleteDirectory(new File(outPutPath))
  }

  @Test
  @throws[Exception]
  def start(): Unit = {
    val etlContext = new ETLContext[SparkContext, RDD[String]](classOf[SparkETLTest], sc, jsonStr)
    etlContext.start()
  }



}

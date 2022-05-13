package org.bigdate.etl.common.scala.test.spark

import java.io.{File, IOException}
import java.util.Objects

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.bigdata.etl.common.context.ETLContext
import org.junit.{Before, Test}


/**
 *  反射对于函数的参数类型泛型会自动强转
 *
 * Author: GL
 * Date: 2022-04-28
 */
class SparkETLTest {
  private val inputPath = Objects.requireNonNull(classOf[SparkETLTest].getClassLoader.getResource("input.txt")).getPath
  private val dirtyPath = new File(inputPath).getParent.concat("/dirty")
  private val outPutPath = new File(inputPath).getParent.concat("/out")
  private val etlJson = s"""{
                           |	"source": {
                           |		"processType": "file",
                           |		"config": {
                           |			"path": "$inputPath"
                           |		}
                           |	},
                           |	"transform": [{
                           |		"processType": "dirty",
                           |		"config": {
                           |			"dirtyPath": "$dirtyPath"
                           |		}
                           |	}],
                           |	"sink": [{
                           |		"processType": "file",
                           |		"config": {
                           |			"path": "$outPutPath"
                           |		}
                           |	}]
                           |}""".stripMargin
  private var spark: SparkSession = _
  private var etl: ETLContext[SparkSession] = _


  @Before
  @throws[IOException]
  def init(): Unit = {
    val conf = new SparkConf().setAppName("test scala etl").setMaster("local[2]")
    spark = SparkSession.builder.config(conf).getOrCreate()
    FileUtils.deleteDirectory(new File(dirtyPath))
    FileUtils.deleteDirectory(new File(outPutPath))
  }

  @Test
  def start(): Unit = {
    try {
      etl = new ETLContext[SparkSession](classOf[SparkETLTest], spark, etlJson)
      etl.start()
    } catch {
      case ex: Throwable =>
        throw ex
    } finally {
      if (etl != null) {
        etl.close()
      }
    }
  }



}

package com.haizhi.bigdata.common.asyn.model.pojo

/**
 * Author: GL
 * Date: 2022-01-21
 */
case class PartitionInfo(
                          keyType: Option[Int],
                          unit: Option[String],
                          keyList: Option[Seq[String]],
                          dataProcessMode: Option[Int],
                          keyDefaultValue: Option[String]
                        )

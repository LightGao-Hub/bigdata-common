package com.haizhi.bigdata.common.asyn.model.pojo

/**
 * Author: GL
 * Date: 2022-01-21
 */
case class ExecStrategy(
                         timeout: Option[Int],
                         ignoreExpand: Option[Boolean],
                         nullValueSkewed: Option[Boolean] = None,
                         var fastView: Option[Boolean] = None,
                         var shufflePartitions: Option[Int]
                       )

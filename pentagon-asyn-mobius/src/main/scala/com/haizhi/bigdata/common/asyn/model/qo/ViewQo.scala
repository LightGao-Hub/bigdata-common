package com.haizhi.bigdata.common.asyn.model.qo

import com.haizhi.bigdata.common.asyn.model.pojo.{ExecStrategy, IncrTb, PartitionInfo, TmpTbSql}

/**
 *  合表异步请求类
 *
 * @param viewTypeCode 调用的接口，例如 0 = /view/create
 * @param weight 合表优先级权重, 默认1~3
 *
 * Author: GL
 * Date: 2022-01-20
 */
case class ViewQo(
                         viewTypeCode: Int = 0,
                         engine: String,
                         traceId: String,
                         sql: String,
                         tbName: String,
                         pool: String,
                         mapJoinEnabled: String,
                         partitions: Long,
                         iVersion: Int,
                         baseTempTable: Seq[TmpTbSql],
                         formula: String,
                         incrTbs: Option[Array[IncrTb]],
                         strategy: Option[ExecStrategy],
                         vars: Option[String],
                         keepOldData: Int,
                         lastTag: Option[String],
                         partitionInfo: Option[PartitionInfo],
                         redisKeyPrefix: String,
                         var weight: Int = 1
                       )

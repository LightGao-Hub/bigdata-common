package com.haizhi.bigdata.common.asyn.model.pojo

import org.json4s.JsonAST.JValue

/**
 * Author: GL
 * Date: 2022-01-21
 */
case class TmpTbSql(
                     tmpTb: String = "",
                     id: String = "",
                     sql: String,
                     refs: Option[List[String]] = None,
                     cache: Option[Boolean] = None,
                     `type`: Option[String] = None,
                     vars: Option[Map[String, JValue]] = None
                   )

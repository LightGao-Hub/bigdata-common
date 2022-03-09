package com.haizhi.bigdata.common.asyn.model.pojo

/**
 * Author: GL
 * Date: 2022-01-21
 */
case class IncrTb(
                   tb_id: String,
                   storage_id: String,
                   dep_version: Int,
                   var parts: String,
                   data_changed: Boolean,
                   user_id: String,
                   data_size: Long,
                   formula: String
                 )

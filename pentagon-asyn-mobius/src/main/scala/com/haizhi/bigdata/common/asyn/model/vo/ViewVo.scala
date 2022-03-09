package com.haizhi.bigdata.common.asyn.model.vo

/**
 * 异步返回计算结果
 *
 * @param status 0 成功，1 异常
 * @param viewTypeCode 调用的接口，比如 /view/create
 * @param result 结果 Json
 * @param taskId taskId
 * @param errMsg 异常信息
 *
 * Author: GL
 * Date: 2022-01-21
 */
case class ViewVo(
                   status: Int,
                   viewTypeCode: Int = 0,
                   result: Option[String],
                   taskId: String,
                   errMsg: Option[String]
                 )

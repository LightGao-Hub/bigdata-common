package com.haizhi.bigdata.common.asyn.config

import java.util.Objects

import com.haizhi.bigdata.common.asyn.enums.RedisConstant
import com.haizhi.bigdata.common.asyn.enums.RedisDefault

/**
 * Author: GL
 * Date: 2022-01-21
 */
object RedisCommonQueue {

  // pentagon合表预执行队列
  private val PRE_EXECUTE_QUEUE = "pre:execute:queue"
  // mobius待执行队列
  private val WAIT_EXECUTE_QUEUE = "wait:execute:queue"
  // mobius结果队列
  private val EXECUTED_RESULT_QUEUE = "executed:result:queue"

  def PRE_EXECUTE_QUEUE(prefix: String, entId: String): String = fullRedisQueue(prefix, entId, PRE_EXECUTE_QUEUE)

  def PRE_EXECUTE_QUEUE(prefix: String): String = wildcardRedisQueue(prefix, PRE_EXECUTE_QUEUE)

  def WAIT_EXECUTE_QUEUE(prefix: String, entId: String): String = fullRedisQueue(prefix, entId, WAIT_EXECUTE_QUEUE)

  def WAIT_EXECUTE_QUEUE(prefix: String): String = wildcardRedisQueue(prefix, WAIT_EXECUTE_QUEUE)

  def EXECUTED_RESULT_QUEUE(prefix: String): String =
    s"${checkPrefix(prefix)}${RedisConstant.REDIS_SEPARATOR}$EXECUTED_RESULT_QUEUE"

  private def fullRedisQueue(prefix: String, entId: String, queue: String): String = {
    s"${checkPrefix(prefix)}${RedisConstant.REDIS_SEPARATOR}$queue${RedisConstant.REDIS_SEPARATOR}${checkEntId(entId)}"
  }
  // 企业域通配符队列
  private def wildcardRedisQueue(prefix: String, queue: String): String = {
    s"${checkPrefix(prefix)}${RedisConstant.REDIS_SEPARATOR}$queue${RedisConstant.REDIS_SEPARATOR}${RedisConstant.
      REDIS_WILDCARD}"
  }

  private def checkPrefix(prefix: String) = if (Objects.nonNull(prefix)) prefix
  else RedisDefault.DEFAULT_PREFIX

  private def checkEntId(entId: String) = if (Objects.nonNull(entId)) entId
  else RedisDefault.DEFAULT_ENTID
}

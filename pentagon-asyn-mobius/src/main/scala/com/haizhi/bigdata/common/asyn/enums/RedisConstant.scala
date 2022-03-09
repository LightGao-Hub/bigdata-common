package com.haizhi.bigdata.common.asyn.enums

/**
 * Author: GL
 * Date: 2022-01-21
 */
object RedisConstant extends Enumeration {
  type RedisConstantConfig = Value
  val REDIS_SEPARATOR: RedisConstant.Value = Value(":")
  val REDIS_WILDCARD: RedisConstant.Value = Value("*")
}

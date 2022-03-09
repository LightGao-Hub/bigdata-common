package com.haizhi.bigdata.common.asyn.enums

/**
 * Author: GL
 * Date: 2022-01-21
 */
object RedisDefault extends Enumeration {
  type RedisDefaultConfig = Value
  val DEFAULT_ENTID: RedisDefault.Value = Value("haizhi")
  val DEFAULT_PREFIX: RedisDefault.Value = Value("DI:QUEUE:bdp_dionline")
}

package com.haizhi.bigdata.common.asyn.config

import org.junit.{Assert, Test}

/**
 * Author: GL
 * Date: 2022-01-21
 */
class testConfig {

  @Test
  def testQueue(): Unit = {
    println(RedisCommonQueue.EXECUTED_RESULT_QUEUE("com.haizhi", "shenzhen"))
    println(RedisCommonQueue.EXECUTED_RESULT_QUEUE("com.haizhi"))
  }

}

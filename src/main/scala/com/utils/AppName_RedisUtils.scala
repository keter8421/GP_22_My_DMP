package com.utils

import redis.clients.jedis.Jedis

object AppName_RedisUtils {
  def getAppName(appId:String) = {

    val jedis = new Jedis("yuke", 6379)

    val appName = jedis.hget("AppIdName", appId)

    jedis.close()

    appName
  }



}

package com.Tags

import com.utils.{AppName_RedisUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsApp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val appMap = args(1).asInstanceOf[Map[String, String]]

    val appId = row.getAs[String]("appid")
    val _appName = row.getAs[String]("appname")

    val appName = appMap.getOrElse("appid",_appName)

    list:+=("APP "+appName,1)

    list
  }

  def makeTags_Redis(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]

    val appId = row.getAs[String]("appid")
    val _appName = row.getAs[String]("appname")

    val appName = AppName_RedisUtils.getAppName(appId)

    if(appName!=null) list:+=("APP "+appName,1)
    else list:+=("APP "+appId,1)

    list
  }
}

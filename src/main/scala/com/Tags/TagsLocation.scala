package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsLocation extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    list:+=( "ZP"+provincename,1)
    list:+=( "ZC"+cityname,1)

    list
  }
}

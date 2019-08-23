package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAdplatformproviderid extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val adplatFormProviderid = row.getAs[Int]("adplatformproviderid")

    list:+=("CN "+adplatFormProviderid,1)

    list
  }
}

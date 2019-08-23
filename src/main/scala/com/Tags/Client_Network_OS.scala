package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object Client_Network_OS extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val client = row.getAs[Int]("client")

    val networkmannerid = row.getAs[Int]("networkmannerid")
    val networkmannername = row.getAs[String]("networkmannername")

    val ispid = row.getAs[Int]("ispid")
    val ispname = row.getAs[String]("ispname")

    if(client==1) list:+=( client+" Android D00010001",1)
    else if(client==2) list:+=( client+" IOS D00010002",1)
    else if(client==3) list:+=( client+" WinPhone D00010003",1)
    else list:+=("_ 其 他 D00010004",1)

    list:+=( networkmannername+" D0002000"+networkmannerid,1)

    list:+=( ispname+" D0003000"+ispid,1)

    list
  }
}

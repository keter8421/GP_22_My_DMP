package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * 关键字
  */
object TagKeyWord extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = new Jedis("yuke", 6379).sismember("keyword","word")
    // 获取关键字，打标签
    val kwds = row.getAs[String]("keywords").split("\\|")
    // 按照过滤条件进行过滤数据
    kwds.filter(word=>{
      word.length>=3 && word.length <=8 && !stopword
    })
      .foreach(word=>list:+=("K"+word,1))
    list
  }
}

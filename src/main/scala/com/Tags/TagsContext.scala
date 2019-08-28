package com.Tags

import com.utils.{AppNameUtils, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 上下文标签
  */
object  TagsContext {
  def main(args: Array[String]): Unit = {
//    if(args.length != 4){
//      println("目录不匹配，退出程序")
//      sys.exit()
//    }
    val Array(inputPath,outputPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)

    import sQLContext.implicits._

    val bApplist: Broadcast[Map[String, String]] = AppNameUtils.getAppName(sc)

    val jedis = new Jedis("yuke", 6379)

    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row=>{
      // 取出用户Id

//      val userId = TagUtils.getOneUserId(row)
      val userId = TagUtils.getAllUserId(row)
//      // 接下来通过row数据 打上 所有标签（按照需求）
//      val adList = TagsAd.makeTags(row)
////      val AppList = TagsApp.makeTags(row,bApplist.value)
//      val AppList = TagsApp.makeTags_Redis(row)
//      val adplatformprovideridList = TagsAdplatformproviderid.makeTags(row)
//      val client_network_os = Client_Network_OS.makeTags(row)
//      val KeyWord = TagKeyWord.makeTags(row)
//      val location = TagsLocation.makeTags(row)
      val business = BusinessTag.makeTags(row)


        (userId,business)
//      (userId,adList++AppList++adplatformprovideridList++KeyWord++client_network_os++location++business)
    }).reduceByKey((x,y)=>{
      x.++(y).groupBy(_._1).map(x=> (x._1,x._2.length)).toList
    }).take(10).foreach(println)

  }
}
package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object AppName_RedisAddDataUtils {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jedis = new Jedis("yuke", 6379)

    val lines: RDD[String] = sc.textFile("E:\\最终项目\\Spark用户画像分析\\app_dict.txt")
    lines.map(x => {
      val arr = x.split("\t", x.length)
      val id = if (x.length < 9) "" else arr(4)
      val name = if (x.length < 9) "" else arr(1)
      (id, name)
    }).collect.toMap.map(x => {
      jedis.hset("AppIdName", x._1, x._2)
    })

    jedis.close()

  }
}

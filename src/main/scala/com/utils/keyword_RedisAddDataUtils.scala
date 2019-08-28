package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

object keyword_RedisAddDataUtils {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jedis = new Jedis("yuke", 6379)


    val lines: RDD[String] = sc.textFile("E:\\最终项目\\Spark用户画像分析\\stopwords.txt")
    lines.foreach(x => {
      jedis.sadd("keyword", x)
    })

    jedis.close()

  }
}
//object A extends JedisPool("yuke", 6379){}
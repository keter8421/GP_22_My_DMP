package com.utils

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object AppNameUtils {
  def getAppName(sc:SparkContext):Broadcast[Map[String, String]] = {
    val lines: RDD[String] = sc.textFile("E:\\最终项目\\Spark用户画像分析\\app_dict.txt")
    val applist: Map[String, String] = lines.map(x => {
      val arr = x.split("\t", x.length)
      val id = if (x.length < 9) "" else arr(4)
      val name = if (x.length < 9) "" else arr(1)
      (id, name)
    }).collect.toMap
    sc.broadcast(applist)
  }
}

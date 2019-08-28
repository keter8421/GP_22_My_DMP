package Test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

   val json: RDD[String] = sc.textFile("C:\\Users\\keter\\Desktop\\json.txt")

    val data: RDD[ListBuffer[(String, String, String)]] = json.map(josnstr => {

      var list = ListBuffer[(String, String, String)]()

      val jsonParase: JSONObject = JSON.parseObject(josnstr)
      val status: Int = jsonParase.getIntValue("status")
      if (status == 0) ("", "")
      else {
        val regeocode: JSONObject = jsonParase.getJSONObject("regeocode")
        if (regeocode == null || regeocode.keySet().isEmpty) ("", "")
        else {
          val pois: JSONArray = regeocode.getJSONArray("pois")
          if (pois == null || pois.isEmpty) ("", "")
          else {
            var i = 0
            for (i <- 0 until pois.size()) {
              val poi: JSONObject = pois.getJSONObject(i)
              val id = poi.get("id").toString
              val businessarea = poi.get("businessarea").toString
              val types = poi.get("type").toString
              list.append((id, businessarea, types))

            }
          }
        }
      }
      list
    })

    // 1、按照pois，分类businessarea，并统计每个businessarea的总数。
    data.map(list =>

      list.map(x => x._2).groupBy(x => x).mapValues(x => x.size)

    ).foreach(println)



    println("---------------------------------------------------------------------------------------------------------")

    //2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
//    data.map(
//      _.map(
//        x => (
//          x._3.split(";").map((_,1))
//          )
//      ).flatten.groupBy(_._1).mapValues(_.size)
//    ).foreach(println)

  }
}

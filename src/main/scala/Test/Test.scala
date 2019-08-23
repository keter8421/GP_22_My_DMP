package Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    import sQLContext.implicits._

    val lines = sc.parallelize(List("1,,,,,,,,,10", "1,2,3,4,5,6,7,8,9,10"))

    lines.map(x => x.split(",").length).foreach(println)
    lines.map(x => x.split(",",-1).length).foreach(println)



  }
}

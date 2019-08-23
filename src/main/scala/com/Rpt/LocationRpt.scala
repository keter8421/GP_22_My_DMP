package com.Rpt

import java.util.Properties

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object LocationRpt {
  def main(args: Array[String]): Unit = {
    //判断路径
    if(args.length !=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val df = sQLContext.read.parquet(inputPath)
    val a09 = df.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标

      val lst1:List[Double] = RptUtils.request(requestmode, processnode)
      val lst2:List[Double] = RptUtils.click(requestmode, iseffective)
      val lst3:List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

      ((pro, city), List(lst1(0), lst1(1), lst1(2), lst3(0), lst3(1), lst2(0), lst2(1), lst3(2), lst3(3)))
    })
    val unit = a09.reduceByKey((x,y) => x.zip(y).map(x => x._1+x._2))
      .map(x => (
        x._1._1,x._1._2,
        x._2(0),x._2(1),x._2(2),
        x._2(3),x._2(4),if(x._2(3) == 0) 0 else x._2(4)/x._2(3),
        x._2(5),x._2(6),if(x._2(5) == 0) 0 else x._2(6)/x._2(5),
        x._2(7),x._2(8)))
    import sQLContext.implicits._
//    unit.sortBy(_._1).toDF().show()
    val df_mysql: DataFrame = unit.sortBy(_._1).toDF(
      "省","市",
      "原始请求","有效请求","广告请求",
      "参与竞价数", "竞价成功数","竞价成功率",
      "展示量","点击量","点击率",
      "广告成本","广告消费"
    )

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "042156")
    df_mysql.write.jdbc("jdbc:mysql://localhost:3306/test","LocationRpt",connectionProperties)



  }
}

package com.Rpt

import java.util.Properties

import com.utils.RptUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LocationRpt_SQL {
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
    df.registerTempTable("tmp_table")
//    val res = sQLContext.sql(
//      """
//        | select provincename ,cityname
//        | count(case when requestmode=1 and processnode>=1 then 1 else 0 end)
//        | from tmp_table group by provincename ,cityname
//        |  limit 3
//      """.stripMargin)
    val res: DataFrame = sQLContext.sql(
      """
 select provincename ,cityname ,
 count(case when requestmode=1 and processnode>=1 then 1 else 0 end) ,
 count(case when requestmode=1 and processnode>=2 then 1 else 0 end) ,
 count(case when requestmode=1 and processnode=3 then 1 else 0 end) ,
 count(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) ,
 count(case when iseffective=1 and isbilling=1 and iswin=1 and iswin!=0 then 1 else 0 end) ,
 count(case when requestmode=1 and iseffective=1 then 1 else 0 end) ,
 count(case when requestmode=1 and iseffective=1 then 1 else 0 end) ,
 count(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) ,
 count(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end)
 from tmp_table
 group by provincename,cityname
      """.stripMargin)
    res.show()








//      "省","市",
//      "原始请求","有效请求","广告请求",
//      "参与竞价数", "竞价成功数","竞价成功率",
//      "展示量","点击量","点击率",
//      "广告成本","广告消费"
//
//
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "042156")
//    df_mysql.write.jdbc("jdbc:mysql://localhost:3306/test","LocationRpt",connectionProperties)



  }
}

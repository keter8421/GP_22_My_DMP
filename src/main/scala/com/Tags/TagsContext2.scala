package com.Tags

import java.util.Date

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{AppNameUtils, TagUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 上下文标签
  */
object  TagsContext2 {
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

    val bApplist: Broadcast[Map[String, String]] = AppNameUtils.getAppName(sc)

    val jedis = new Jedis("yuke", 6379)

    // 过滤符合Id的数据
    val data = df.filter(TagUtils.OneUserId)

      .flatMap(row => {

      val userIdlist = TagUtils.getAllUserId(row)

      // 接下来通过row数据 打上 所有标签（按照需求）
      val adList = TagsAd.makeTags(row)
//      val AppList = TagsApp.makeTags(row,bApplist.value)

//      val AppList = TagsApp.makeTags_Redis(row)
//      val adplatformprovideridList = TagsAdplatformproviderid.makeTags(row)
//      val client_network_os = Client_Network_OS.makeTags(row)
//      val KeyWord = TagKeyWord.makeTags(row)
//      val location = TagsLocation.makeTags(row)
//      val business = BusinessTag.makeTags(row)

      userIdlist.map(userId => {
        if(userId==userIdlist.head)
//          (userId.hashCode.toLong,adList ++ AppList ++ adplatformprovideridList ++ KeyWord ++ client_network_os ++ location ++ business)
          (userId.hashCode.toLong,adList)
        else
          (userId.hashCode.toLong,List())
      })


      //        (userId,business)
//      (userId, adList ++ AppList ++ adplatformprovideridList ++ KeyWord ++ client_network_os ++ location ++ business)
    })



    val dataline = df.filter(TagUtils.OneUserId)

      .flatMap(row => {

      val userIdlist = TagUtils.getAllUserId(row)

      userIdlist.map(userId => {
        Edge(userIdlist.head.hashCode.toLong,userId.hashCode.toLong,0)
      })
    })

    val graph = Graph(data,dataline)

    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.join(data).map{
      case(userId, (maxId, info)) => {
        (maxId,info.groupBy(x => x._1).map(x => (x._1,x._2.size)).toList)
      }
    }.reduceByKey(_++_).foreach(println)


    // 80G->100G





//      .reduceByKey((x, y) => {
//      x.++(y).groupBy(_._1).map(x => (x._1, x._2.length)).toList
//    })
//
//
    val load: Config = ConfigFactory.load()//加载配置文件
    val hbaseTableName = load.getString("hbase.TableName")

    // 创建Hadoop任务
    //gp22
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    //yuke:2181
    configuration.set("hbase.zookeeper.quorum","yuke")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set("zookeeper.znode.parent","/")

    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin

    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    val datas: RDD[(ImmutableBytesWritable, Put)] = data.reduceByKey((list1, list2) =>
      // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
      (list1 ::: list2)
        // List(("APP爱奇艺",List()))
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
    ).map {
      case (userid, userTag) => {

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t => t._1 + "," + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(new Date().toString), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }
    datas
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)
  }
}
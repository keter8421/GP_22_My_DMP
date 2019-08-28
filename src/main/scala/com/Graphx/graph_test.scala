package com.Graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object graph_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("Z", 35)),
      (2L, ("H", 34)),
      (6L, ("D", 31)),
      (9L, ("K", 30)),
      (133L, ("H133", 30)),
      (138L, ("X138", 36)),
      (16L, ("F", 35)),
      (44L, ("N", 27)),
      (21L, ("J", 28)),
      (5L, ("G", 60)),
      (7L, ("O", 55)),
      (158L, ("M", 55))
    ))

    val egde: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    // 图计算准备
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,egde)

    // 所谓的求顶点，还不如说是分组，组名是每个组的hashcode最小的顶点名
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.foreach(println)

    // 这是为顶点数据补充上节点信息
    vertices.join(vertexRDD).foreach(println)

    // 清洗数据，提取组名，丢弃节点名，留下节点信息
    val node: RDD[(VertexId, List[Any])] = vertices.join(vertexRDD).map {
      case (user, (conId, (name, age))) => {
        (conId, List(name, age))
      }
    }
    node.foreach(println)

    // 根据组名，合并组信息
    node.reduceByKey(_++_).foreach(println)
  }
}

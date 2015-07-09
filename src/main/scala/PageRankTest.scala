package main.scala

/**
 * Created by cheryl on 2015/7/9.
 */
import org.apache.spark.graphx._
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
// A graph with edge attributes containing distances

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PageRankTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

  //dblp test
    val users = sc.textFile("data/authorId.txt").map { line =>
      val fields = line.split('\t')
      (fields(1).toLong, fields(0))
    }
    val edges = sc.textFile("data/coauthor.txt").map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }
    val graph = Graph(users, edges, "").cache()

    // Load the edges as a graph
//    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }
}

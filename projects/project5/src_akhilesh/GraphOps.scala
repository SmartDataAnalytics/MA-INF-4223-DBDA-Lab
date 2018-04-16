package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object GraphOps {
  def main(args: Array[String]) = {
    val input = "src/main/resources/rdf.nt"
    val spark = SparkSession.builder.master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("GraphX example").getOrCreate()
    val tripleRDD = spark.sparkContext.textFile(input).map(TripleUtils.parsTriples)
    val tutleSubjectObject = tripleRDD.map { x => (x.subject, x.`object`) }
    type VertexId = Long
    val indexVertexID = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
    val tuples = tripleRDD.keyBy(_.subject).join(indexVertexID).map(
      {
        case (k, (TripleUtils.Triples(s, p, o), si)) => (o, (si, p))
      })
    val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({ case (k, ((si, p), oi)) => Edge(si, oi, p) })
    val graph = Graph(vertices, edges)
    graph.vertices.collect().foreach(println(_))
    println("edges")
    graph.edges.collect().foreach(println(_))
    val subrealsourse = graph.subgraph(t => t.attr ==
      "http://commons.dbpedia.org/property/source")
    println("subrealsourse")
    subrealsourse.vertices.collect().foreach(println(_))
    val conncompo = subrealsourse.connectedComponents()
    val pageranl = graph.pageRank(0.0001)
    val printoutrankedtriples =
      pageranl.vertices.join(graph.vertices)
        .map({ case (k, (r, v)) => (k, r, v) })
        .sortBy(5 - _._2)
    println("printoutrankedtriples")
    printoutrankedtriples.take(5).foreach(println(_))

    spark.stop
   
  }
}
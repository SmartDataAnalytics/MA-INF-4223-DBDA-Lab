package net.sansa_stack.template.spark.graphOps

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.collection.mutable.ListBuffer

import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import net.sansa_stack.template.spark.graphOps.Allpaths.Path

object GraphOps {
  def main(args: Array[String]) = {

    val input = "src/main/resources/rdf.nt"
    val spark = SparkSession.builder.master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("GraphX example").getOrCreate()
    val tripleRDD = spark.sparkContext.textFile(input).filter(!_.startsWith("#")).map(TripleUtils.parsTriples)

    val tutleSubjectObject = tripleRDD.map { x => (x.subject, x.`object`) }
    type VertexId = Long
    val seq = new ListBuffer[VertexId]()
    val indexVertexID = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
    val tuples = tripleRDD.keyBy(_.subject).join(indexVertexID).map(
      {
        case (k, (TripleUtils.Triples(s, p, o), si)) => (o, (si, p))
      })
    val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({ case (k, ((si, p), oi)) => Edge(si, oi, p) })
    val graph = Graph(vertices, edges).cache()
    vertices.collect().foreach { case (a, b) => seq += a }

    // END of Graph Creation

    val tstart_walk = System.nanoTime()

    /*Code to run the improvised algorithm
    val path = AllpathsImpovised.runPregel(graph, EdgeDirection.Either)
    path.collect.foreach(println)
    
    * 
    * */

    /*Code to run the improvised algorithm*/
    val run = typeInfo.getTypeInfo(graph)
    val typRdf = run.map { x => (x._1, if (!x._2.isEmpty) List(x._2.groupBy(identity).mapValues(_.size).maxBy(_._2)._1)) }
    typRdf.collect.foreach(println)

    /*
     * Code to run the Shortest path algorithm
    
    		val gra = ShortestPaths.run(graph, seq.toList);

* 
 			* */

    /*
     * Code to run the tradiitonal FPC algorithm
    
    	
    for (a <- seq) {
      val allpath = Allpaths.runPregel(a, 654L, graph, EdgeDirection.Either)
      if (allpath.length != 0) {
        println("Number of paths lengths  654L " + a + " is", allpath.length)
        if (allpath.length > 2) {
          for (singlePath <- allpath) {

            for (edge <- singlePath) {
              println(edge.edgeToString())

            }
          }
        }
      }
    }
*/
    val tstop_walk = System.nanoTime()
    println("Graph Walk Time (ms)=", (tstop_walk - tstart_walk) / 1000000L)
  }

  // Function to check for cycles
  def checkCycle(singlepath: List[List[Path]]): Boolean = {
    var alreadyseen = List[VertexId]()
    for (a <- singlepath) {
      for (x <- a) {
        x.dstId :: alreadyseen
        x.srcId :: alreadyseen
      }
    }

    val s = alreadyseen.groupBy(identity).collect { case (x, List(_, _, _*)) => x }
    if (s.size != 0) {
      println(s)
      return true
    }
    return false
  }
}
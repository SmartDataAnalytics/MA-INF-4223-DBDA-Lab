package rdfKernels

import java.net.URI
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import net.sansa_stack.rdf.spark.graph.LoadGraph
import org.apache.spark.graphx.Graph

object TestSuite {

  /**
   * @param args
   */
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Test Suite Graph Kernels  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    
    val files = input.split(" ")
    println("======================================")
    println("|   Test suite for Graph Kernels     |")
    println("======================================")
    
   
    val triplesRDD = NTripleReader.load(spark, URI.create(files(0)))
    val triplesRDD2 = NTripleReader.load(spark, URI.create(files(1)))
    
    /*
    val intersectMatrix = KernelFunctions.intersectionAdjacencyMatrix(triplesRDD, triplesRDD2)
    val matrixPathKernel = KernelFunctions.matrixPathKernel(intersectMatrix, 4, 0.7)
    val matrixWalkKernel = KernelFunctions.matrixWalkKernel(intersectMatrix, 4, 0.7)
    val msgPathKernel = KernelFunctions.msgPathKernel(triplesRDD, triplesRDD2, 4, 1.5)
    val msgWalkKernel = KernelFunctions.msgWalkKernel(triplesRDD, triplesRDD2, 4, 1.5)
    
    val intersectRDD = triplesRDD.intersection(triplesRDD2)
    val graph = LoadGraph(intersectRDD)
    val intersectionTree = KernelFunctions.constructIntersectionTree(graph, 0, 10, 3, spark)
		*/
    spark.stop
    
  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Test Suite") {

    head("Test suite")

    opt[String]('i', "input").required().valueName("<paths>").
      action((x, c) => c.copy(in = x)).
      text("paths to two files (space-seperated) that contain the data (in N-Triples format)")
      
    help("help").text("prints this usage text")
  }
}
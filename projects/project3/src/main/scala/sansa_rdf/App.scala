package sansa_rdf

import org.apache.spark.sql.SparkSession
import sansa_rdf.io.LoadGraph
import sansa_rdf.io.NQuadReader.{load => nquadLoad, loadToDataFrame => loadToDataFrame_nq}
import org.apache.spark.SparkContext
import sansa_rdf.io.TurtleReader.{load => turtleLoad, loadToDataFrame => loadToDataFrame_ttl}
import sansa_rdf.io.XmlReader._
import java.net.URI
import org.apache.spark.storage.StorageLevel

object App {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder
      .master("local[*]")
        .appName("RDF SANSA READER")
      .getOrCreate()

    val inputFilePath = args(0)
    val extension = inputFilePath.substring(inputFilePath.lastIndexOf(".")+1)

    extension match {
      case "ttl" => TurtleExamples(session.sparkContext,inputFilePath)
      case "nq"  => NQuadExamples(session,inputFilePath)
      case "rdf" => RdfXmlExamples(session,inputFilePath)
      case _ => throw new IllegalArgumentException ("File extension is not valid")
    }

  }



  /*
  * Examples of using TurtleReader
  *
  * */
  def TurtleExamples(sc : SparkContext, path: String) = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val triples = turtleLoad(sc,path)
    triples.persist(StorageLevel.MEMORY_AND_DISK)

    println("Number of triples: " + triples.count())


    //----------.....Data Frame....-----------
    val TripleDF = loadToDataFrame_ttl(sqlContext,triples)

    //Store DataFrame Data into Table
    TripleDF.createOrReplaceTempView("Triple_T")
    //Select Query on DataFrame
    val dfr1 = sqlContext.sql("SELECT * FROM Triple_T")
    dfr1.select("Subject").show()
    dfr1.show()
    dfr1.select($"Subject", $"Predicate").show()
    val dfr2 = sqlContext.sql("SELECT * FROM Triple_T")
    dfr1.show()
    dfr1.groupBy("Subject").count().show()
    //----------......GraphX..... -----------


    val loadedGraph = LoadGraph.makeGraph(triples)
    val graph = loadedGraph.graph
    val vertices = graph.vertices
    val rank = graph.pageRank(0.00001).vertices

    //rank of each node computed by the page rank algorithm
    val rankByNode =  rank.join(vertices).map{
      case (id,(rank,node)) => (node,rank)
    }

    println(rankByNode.take(10).mkString("\n"))
  }



  /**
  * Examples of using NQuadReader
    *
  * */
  def NQuadExamples(sparkSession: SparkSession, inputFileName: String) = {

    // Example of reading a file of nquads into rdd
    val rdd  = nquadLoad(sparkSession, URI.create(inputFileName))

    println("RDD size \n"+rdd.count())


    //----------......GraphX..... -----------
    //An example of page rank algorithm with the distributed graph of GraphX

    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val loadedGraph = LoadGraph(rdd.map(quad => quad.asTriple()))
    val graph = loadedGraph.graph
    val vertices = graph.vertices
    val rank = graph.pageRank(0.00001).vertices

    val rankByNode = rank.join(vertices).map{
      case(id,(rank,node)) => (node,rank)
    }
    println(rankByNode.take(10).mkString("\n"))


    //----------.....Data Frame....-----------

    val df = loadToDataFrame_nq(sparkSession,rdd)
    df.createOrReplaceTempView("quadsView")

    // example of the query over MapType of DataFrame
    val res = sparkSession.sql("SELECT subject.uri from quadsView").where(df("subject.uri").notEqual("null"))
    res.show()

    //counts how many unique blankNodeId exists
    df.groupBy("object.dataType").count().show()

  }



  /**
    * Examples of using XmlReader
    *
    * */
  def RdfXmlExamples(sparkSession : SparkSession, inputFile : String )= {


    // Reading RDD using second method (our implementation)
    val rdd = loadRDDFromXML(sparkSession, inputFile, "rdf:Description", 4)
    println("count=", rdd.count())

    println("count=", rdd.count())



    //----------......GraphX..... -----------
    //An example of page rank algorithm with the distributed graph of GraphX

    val loadedGraph = LoadGraph(rdd)
    val graph = loadedGraph.graph
    val vertices = graph.vertices
    val rank = graph.pageRank(0.00001).vertices

    //rank of each node computed by the page rank algorithm
    val rankByNode =  rank.join(vertices).map{
      case (id,(rank,node)) => (node,rank)
    }

    println(rankByNode.take(10).mkString("\n"))


    //----------.....Data Frame....-----------

    val dataFrame = loadDataFrameFromXML(sparkSession,inputFile,"rdf:Description",4)
    dataFrame.createOrReplaceTempView("rdf_xml_table")

    val res = sparkSession.sql("SELECT * from rdf_xml_table")
    res.show()

  }


}

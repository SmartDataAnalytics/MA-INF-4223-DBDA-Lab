package sansa_rdf.io

/**
  * Created by emad on 10.07.17.
  */
import java.io.ByteArrayInputStream
import java.net.URI

import org.apache.jena.sparql.core.Quad
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.jena.graph.Node
import sansa_rdf.io.LoadGraph._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


/**
  * N-Quad reader
  * Reads N-Quad rdf data format into spark RDD and DataFrame
  *
  * @author Emad Bahrami Rad <emadbahramirad@gmail.com>
  *
  */
object NQuadReader {


  /**
    * Loads a N-Quads file into an RDD.
    *
    * @param session the Spark session
    * @param path    the path to the N-Quads file(s)
    * @return the RDD of quads
    */
  def load(session: SparkSession, path: URI): RDD[Quad] = {
    load(session, path.toString)
  }

  /**
    * Loads a N-Quads file into an RDD.
    *
    * @param session the Spark session
    * @param path    the path to the N-Quads file(s)
    * @return the RDD of quads
    */
  def load(session: SparkSession, path: String): RDD[Quad] = {
    session.sparkContext.textFile(path)
      .filter(line => !line.startsWith("#"))
       .filter(line => !line.isEmpty || line.length==0 )
      .map(line =>
        RDFDataMgr.createIteratorQuads(new ByteArrayInputStream(line.getBytes), Lang.NQUADS, null).next())
  }



  /**
    * Loads data from a N-Quad file to DataFrame
    *
    * @param sparkSession the spark session
    * @param path  the uri path to N-Quad file
    * @return DataFrame of quads
    *
    * */
  def loadToDataFrame(sparkSession: SparkSession, path: URI): DataFrame = {
    loadToDataFrame(sparkSession,load(sparkSession,path))
  }


  /**
    * Loads data from a N-Quad file to DataFrame
    *
    * @param sparkSession the spark session
    * @param path  the path to N-Qaud file
    * @return DataFrame of quads
    *
    * */
  def loadToDataFrame(sparkSession: SparkSession, path: String): DataFrame = {
    loadToDataFrame(sparkSession,load(sparkSession,path))
  }


  /**
    * Loads RDD of Quad into DataFrame
    *
    * The schema has four columns '''subject''', '''predicate''', '''object''', '''graph'''
    * in each of these columns a mapping of type '''MapType''' is stored.
    *  Each of the subject,predicate, object or graph can be
    *  - URI
    *  - Blank Node
    *  - Literal
    *
    *   According to above types there is a mapping with following keys:
    *    - '''uri'''
    *    - '''blankNodeId'''
    *    - '''literalValue'''
    *    - '''dataType'''
    *   Theses keys are used when querying the DataFrame.
    *
    * @param sparkSession the spark session
    * @param rdd  RDD of Jena Quad type
    * @return DataFrame of quads
    *
    * */
  def loadToDataFrame(sparkSession: SparkSession,rdd : RDD[Quad]): DataFrame = {
    val schemaStr = "subject predicate object graph"
    val fields = schemaStr.split(" ")
      .map(fieldName => StructField(fieldName, MapType(StringType,StringType), nullable = true))
    val schema = StructType(fields)

    /**
      *  Creates a map of data stored in a node(subject,predicate,object,graph)
      *  in order to store these data in the  DataFrame.
      *  Each of the subject,predicate, object or graph can be
      *  - URI
      *  - Blank Node
      *  - Literal
      *
      *   According to each above types there is a mapping with following keys:
      *    - uri
      *    - blankNodeId
      *    - literalValue
      *    - dataType
      *   Theses keys are useful when querying the DataFrame
      *
      * @param node a node of Jena Node class
      * @return a mapping from the keys of
      *          ''uri'',''blankNodeId'',''literalValue'' and ''dataType''
      *          to their corresponding value in the Jena node class
      *
      * */
    def nodeValueMap(node: Node): Map[String,String] = {
      if(node.isURI){
        Map("uri"-> node.getURI) withDefaultValue ""
      }
      else if(node.isBlank){
        Map("blankNodeId" -> node.getBlankNodeId.toString) withDefaultValue  ""
      }
      else if(node.isLiteral){
        if(node.getLiteral.isWellFormed)
          Map("literalValue"-> node.getLiteralValue.toString,"dataType" -> node.getLiteralDatatype.toString) withDefaultValue ""
        else
          Map("literalValue" -> node.getLiteral.getLexicalForm, "dataType" -> node.getLiteral.getDatatypeURI) withDefaultValue ""
      }
      else{
        Map() withDefaultValue ""
      }
    }

    val rowRDD = rdd.map(quad => Row(nodeValueMap(quad.getSubject),nodeValueMap(quad.getPredicate),nodeValueMap(quad.getObject),nodeValueMap(quad.getGraph)))

    sparkSession.createDataFrame(rowRDD,schema)
  }


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      //          .master("yarn")
      //          .master("spark://hadoopmaster:7077")
      .appName("N-Quad Reader")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    var inputFileName = args(0)

    //----------.....Reading ....-----------
    // Example of reading a file of nquads into rdd
    val rdd  = NQuadReader.load(sparkSession, URI.create(inputFileName))

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
    println(rankByNode.collect().mkString("\n"))


    //----------.....Data Frame....-----------

    val df = loadToDataFrame(sparkSession,rdd)
    df.createOrReplaceTempView("quadsView")

    // example of the query over MapType of DataFrame
    val res = sparkSession.sql("SELECT subject.uri from quadsView").where(df("subject.uri").notEqual("null"))
    res.show()

    //counts how many unique blankNodeId exists
    df.groupBy("object.dataType").count().show()

  }
}


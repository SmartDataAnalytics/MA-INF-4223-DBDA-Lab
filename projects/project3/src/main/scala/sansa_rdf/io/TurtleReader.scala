package sansa_rdf.io

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.ArrayList

import org.eclipse.rdf4j.rio.RDFHandlerException
import org.eclipse.rdf4j.rio.RDFParseException
import org.eclipse.rdf4j.rio.RDFParser
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.turtle.TurtleParser

import collection.mutable._
import org.apache.spark.SparkConf
import java.io.IOException
import java.io.InputStream
import java.io.{ByteArrayInputStream, File}
import java.io.PrintWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.types.StructType

import java.util.regex.{Matcher, Pattern}

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.io.Source
import java.io.File

import org.apache.spark.rdd.RDD
import sansa_rdf.io.LoadGraph

/**
  *  Reads and parses a Turtle file and load it into
  *  ''RDD'', ''DataFrame'' and ''Graph'' of ''GraphX''
  *
  *  @author Nayef Roqaya
  * */

object TurtleReader {

  /**
    * Load an Turtle file into RDD
    *
    * @param sc an instance of SparkContext
    * @param path the path to the Turtle file
    * @return RDD of triples
    *
    * */
  def load(sc: SparkContext, path: String) = {
    val prefixes = sc.textFile(path).flatMap(line => prefixParse(line))
    var pref = ""
    if(!prefixes.isEmpty()){
      pref = prefixes.reduce((a, b) => a + "\n" + b)
    }
    val b = readFile(sc,path)
    val blockRDD = sc.parallelize(b, 4)
    blockRDD.flatMap(block => blockParse(block, pref))
  }

  /**
    *  Load an Turtle file into DataFrame
    * @param sqlContext an instance of SQLContext
    * @param triples an RDD of String
    * @return DataFrame constructed from the given RDD
    * */
  def loadToDataFrame(sqlContext: SQLContext, triples : RDD[String]) = {
    //// Create an Encoded Schema in a String Format:
    val schemaString = "Subject Predicate Object"
    //Generate schema:
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    //Apply Transformation for Reading Data from Text File
    val rowRDD = triples.map(_.split(":")).map(e ⇒ Row(e(0), e(1), e(2)))
    //Apply RowRDD in Row Data based on Schema:
    sqlContext.createDataFrame(rowRDD, schema)
  }


  def readFile(sc : SparkContext, fileName: String): ListBuffer[String] = {
    var temp: String = ""
    var blocks = new ListBuffer[String]()
    var counter = 0
    val lines = sc.textFile(fileName).collect()
    for (line <- lines) {
      temp ++= line
      temp ++= "\n"
      if (line.trim().takeRight(1) == ".") {
        counter = counter + 1

        if (counter == 10) {
          blocks += temp
          temp = ""
          counter = 0
        }
      }
    }
    if (temp != "") {
      blocks += temp
    }
    blocks
  }

  def blockParse(block: String, prefixes: String): Array[String] = {
    var triples = ArrayBuffer.empty[String]
    var model = ModelFactory.createDefaultModel();
    val modelText = prefixes + "\n" + block
    model.read(new ByteArrayInputStream(modelText.getBytes()), null, "TTL");
    val iter = model.listStatements()
    while (iter.hasNext()) {
      var triple: String = iter.next().asTriple().toString()
      triples += triple
    }
    triples.toArray
  }

  def prefixParse(line: String): Array[String] = {
    var temp = line
    val prefixRegex = Pattern.compile("@prefix.*.")
    var matcher = prefixRegex.matcher(temp)
    var vars = ArrayBuffer.empty[String]
    while (matcher.find()) {
      vars += temp.substring(matcher.start(), matcher.end())
      temp = temp.substring(0, matcher.start()) + temp.substring(matcher.end(), temp.length())
      matcher = prefixRegex.matcher(temp)
    }
    val prefixes = vars.toArray
    prefixes
  }

  //////////////////////////RD4J Parser --- only to compare the results with parallel approach ////////////////
  def Parsing_Turtle_File(TTLPath: String): Unit = {

    val filename = TTLPath;
    val myList1: java.util.ArrayList[org.eclipse.rdf4j.model.Statement] = new ArrayList()
    val myList2: ArrayList[String] = new ArrayList()
    val Result: ArrayList[String] = new ArrayList()

    //read the TTI file for parsing :
    val documentUrl: java.net.URL = new File(filename).toURI().toURL()
    val inputStream: InputStream = documentUrl.openStream()
    val rdfParser: RDFParser = new TurtleParser()
    val collector: StatementCollector = new StatementCollector(myList1)

    //    sc.textFile(TTLPath, 4).map((Parsercollector(documentUrl,inputStream,rdfParser,collector,myList1,myList2))

    rdfParser.setRDFHandler(collector)
    try rdfParser.parse(inputStream, documentUrl.toString)
    catch {
      // handle a problem encountered by the RDFHandler
      case e @ (_: IOException | _: RDFParseException |
                _: RDFHandlerException) => {}
    }
    var counter = 0
    for (i <- 0 until myList1.size) {
      val element: String = myList1.get(i).toString();
      myList2.add(element.toString())
    } // end for
    val pw1 = new PrintWriter(new File("E:/URLs.txt"))
    for (i <- 0 until myList2.size()) {
      pw1.write(myList2.get(i) + "\n")
    }
    pw1.close()

  } // end Parser




  def main(args: Array[String]): Unit = {

    //----------.....READ from File System  and Hadoop....-----------

    //1. Set path TTL file from file system:
    val input_TTL: String = args(0)
    //2.Create spark configuration
    val conf = new SparkConf().setAppName("SANSA RDF READER Application").setMaster("local[*]")

    //3.Create spark context
    val sc = new SparkContext(conf)
    // Create SQLContext Object:
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val triples = load(sc,input_TTL)
        println("Number of triples: " + triples.count())


    //----------.....Data Frame....-----------
    val TripleDF = loadToDataFrame(sqlContext,triples)

    //Store DataFrame Data into Table
    TripleDF.registerTempTable("Triple_T")
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

    println(rankByNode.collect().mkString("\n"))


    sc.stop()

  } // end main function


}

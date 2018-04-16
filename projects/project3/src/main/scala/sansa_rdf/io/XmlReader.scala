package sansa_rdf.io

import java.io._
import java.util.regex.Pattern

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

/**
  *  Reads RDF/XML file into ''RDD'', ''DataFrame'', ''Graph'' of ''GraphX''
  *
  *  @author Abdul majeed Alkattan
  *
  * */
object XmlReader {

  def main(args: Array[String]) = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example ( File )")
      .getOrCreate()

    val inputFile = args(0)

    // Reading RDD using second method (our implementation)
    val rdd = loadRDDFromXML(sparkSession, inputFile, "rdf:Description", 4)
    println("count=", rdd.count())

    // Reading RDD using Jena's method
    val rddJena = getRDDfromXMLJena(sparkSession,  inputFile, 4)
    println("count=", rddJena.count())

  }

  /**
    * Loads an RDF/XML file into a Dataframe
    * @param session the Spark session
    * @param path    the path to the RDF/XML file
    * @param tag the tag of the form rdf:Description
    * @return the RDD of triples
    */
  def loadDataFrameFromXML(session: SparkSession, path: String, tag: String, numPartitions : Int): DataFrame ={
    val rdd = loadRDDFromXML(session, path, tag,  numPartitions)
    val rowsRDD = rdd.map {
      case triple => T(triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString()) }
    var triplesDF =  session.createDataFrame(rowsRDD)
    triplesDF
  }
  /**
    * Loads an RDF/XML file into an RDD.
    *
    * @param sparkSession the Spark session
    * @param path    the path to the N-Triples file(s)
    * @param tag the tag of the form rdf:Description
    * @param numPartitions the number of partitions
    * @return the RDD of triples
    */
  def loadRDDFromXML (sparkSession: SparkSession, path : String, tag: String, numPartitions : Int): RDD[Triple] ={
    val tagParts = tag.split(":")
    if(tagParts.size < 2){
      return null
    }
    val blocks = readFile(path, tagParts(0), tagParts(1))
    var time = System.nanoTime()
    val blocksRDD = sparkSession.sparkContext.parallelize(blocks, numPartitions)
    val prefixes = sparkSession.sparkContext.textFile(path).flatMap(line => prefixParse(line)).distinct()
    val pref = prefixes.reduce( (a,b) => a + "\n" + b)
    val res = blocksRDD.flatMap(block => blockParse(block, pref))
    var timeAfter = System.nanoTime()
    res
  }


  /**
    * Parse a single block using apache jena
    *
    * @param block An XML block
    * @param prefixes The prefixes string
    * @return the Array of triples
    */
  def blockParse(block : String, prefixes : String): Array[Triple] ={
    var triples = ArrayBuffer.empty[Triple]
    var model = ModelFactory.createDefaultModel();
    val modelText = "<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n"  +"<rdf:RDF " + prefixes + " >" + block + "</rdf:RDF>"
    model.read(new ByteArrayInputStream(modelText.getBytes()), "http://example.org");
    val iter = model.listStatements()
    while (iter.hasNext()) {
      var triple = iter.next().asTriple()
      triples :+= triple
    }
    triples.toArray
  }

  /**
    * Finds the prefixes in the file
    *
    * @param line A single line in the file
    * @return An array of the found prefixes
    */
  def prefixParse(line : String): Array[String] ={
    var temp = line
    val prefixRegex  = Pattern.compile("xmlns\\s*:\\s*[a-zA-Z][a-zA-Z0-9_]*\\s*=\\s*([\"'])(?:(?=(\\\\?))\\2.)*?\\1")
    var matcher = prefixRegex.matcher(temp)
    var vars = ArrayBuffer.empty[String]
    while (matcher.find()){
      vars += temp.substring(matcher.start(), matcher.end())
      temp = temp.substring(0, matcher.start()) + temp.substring(matcher.end(), temp.length())
      matcher = prefixRegex.matcher(temp)
    }
    val prefixes = vars.toArray
    prefixes
  }


  /**
    * Loads an RDF/XML file into a List
    * @param fileName An RDD of
    * @param prefix a string which represent a prefix
    * @param url a string which represents a url
    * @return A listBuffer of blocks in the file
    */
  def readFile(fileName: String, prefix : String, url: String): ListBuffer[String] = {
    var temp: String = ""
    var blocks = new ListBuffer[String]()
    var counter = 0
    var foundFirstBlock = false
    for (line <- Source.fromFile(fileName).getLines) {
      if (foundFirstBlock) {
        if (line.trim().length() > 11) {
          temp ++= line
          temp ++= "\n"
          if (line.trim() == "</"+prefix+":"+url+">") {
            counter = counter + 1
            if (counter == 10) {
              blocks += temp
              temp = ""
              counter = 0
            }
          }
        }
      }else{
        if(line.trim().startsWith("<"+prefix+":"+url)){
          temp ++= line
          temp ++= "\n"
          foundFirstBlock = true
        }
      }
    }
    if (temp != "") {
      blocks += temp
    }
    blocks
  }

  /**
    * Loads an RDF/XML file into an RDD (Jena solution)
    *
    * @param session spark Session
    * @param path The path to the file on the file system
    * @param numPartitions number of partitions for spark
    * @return A listBuffer of blocks in the file
    */

    def getRDDfromXMLJena(session : SparkSession, path : String, numPartitions : Int) : RDD[Triple] = {
      var time = System.nanoTime()

      var model = ModelFactory.createDefaultModel()
      var triples: Array[Triple] = Array()
      try {
        var in = new FileInputStream(path)
        if (in == null) {
          System.out.println("File not found")
        }
        model.read(in, "http://www.example.org")
        var iter = model.listStatements()
        while (iter.hasNext()) {
          var triple = iter.next().asTriple()
          triples :+= triple
        }
        val distData = session.sparkContext.parallelize(triples, numPartitions)
        var timeAfter = System.nanoTime()
        return distData
      }
      catch {
        case e: Exception => println(e.getMessage())
      }
      return null
  }

  /**
    * Loads an RDF/XML file into an DataFrame (Jena's solution)
    *
    * @param sparkSession spark Session
    * @param numPartitions number of partitions for spark
    * @return A listBuffer of blocks in the file
    */

  def getDataFrameFromXMLJena(sparkSession: SparkSession, path: String, numPartitions : Int): DataFrame ={
    val rdd = getRDDfromXMLJena(sparkSession, path, numPartitions)
    val rowsRDD = rdd.map {
      case triple => T(triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString()) }
    var triplesDF =  sparkSession.createDataFrame(rowsRDD)
    triplesDF
  }

}

// Case class or schema
case class T(s: String, p: String, o: String)







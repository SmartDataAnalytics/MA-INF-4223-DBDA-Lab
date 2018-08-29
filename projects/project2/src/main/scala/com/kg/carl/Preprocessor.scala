package com.kg.carl

import java.net.URI
import java.io._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable._
import java.nio.file.Files;
import java.nio.file.Paths
import com.kg.carl.Algorithm._
import org.apache.spark.util._
import com.kg.carl.Utils._

object Preprocessor {

  /**
   * @param args
   * by default run() is invoked
   */
  def main(args: Array[String]) {
    run()
  }

  def run(): Unit = {
    // Initialize spark
    val spark = SparkSession.builder
      .appName(s"CARL")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // import implicits to enable implicit conversions
    import spark.implicits._
    val sc = spark.sparkContext

    println("=============================================")
    println("|                CARL-Scala                 |")
    println("=============================================")

    // uncomment below to parse files from command
    // val files = input.split(" ")

    // default input files
    val triples = sc.textFile("../../../../resources/sample_triples.tsv")
    val cardinalities = sc.textFile("../../../../resources/sample_cardinalities.tsv")

    // read and map triples. Sort by predicate
    val parsedTriples = triples.map(parseTriples).sortBy(_.predicate)
    parsedTriples.cache()
    println("Number of Triples parsed: " + parsedTriples.count())
    println("=============================================")

    /*
     * read and map cardinalities. Sort by predicate.
     * reason for using sort is that after rdd is mapped the insertion order is
     * jumbled as the data is being inserted by several threads
     */
    val parsedCardinalities = cardinalities.map(parseCardinalities).sortBy(_.predicate)
    parsedCardinalities.cache()
    println("Number of Cardinalities parsed: " + parsedCardinalities.count())

    /*
     * using accumulators for storing data into collections as the rdds do not preserve
     * change in a variable declared before the rdds come into action
     * accumulators are gateway to preserve data and transforming as required.
     * all the below accumulators store the collections and are retrieved back as normal
     * collections after the rdd method ends.
     */
    val AccNodes = sc.collectionAccumulator[ArrayBuffer[String]]
    val AccIdNodes = sc.collectionAccumulator[LinkedHashMap[String, Int]]
    val AccPSO = sc.collectionAccumulator[LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]]]]
    val AccProperties = sc.collectionAccumulator[TreeSet[Int]]
    val AccECPV = sc.collectionAccumulator[LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]]]

    /*
     * read and map cardinalities. Sort by predicate.
     * the following inserts ids for all the entities creates a ID store so that algorithm
     * is applied on these ids. These ids are primitive data types and provides granular
     * access.
     */
    parsedTriples.take(parsedTriples.count().asInstanceOf[Int]).foreach { spo: Triples =>

      val s = getIdForNode(AccNodes, AccIdNodes, spo.subject)
      val p = getIdForNode(AccNodes, AccIdNodes, spo.predicate)
      val o = getIdForNode(AccNodes, AccIdNodes, spo.obj)

      val arr = ArrayBuffer[Int]()
      val arrBuf = ArrayBuffer(LinkedHashMap(s -> arr.+=:(o)))
      // insert into accumulator else append to the existing collection
      if (AccPSO.value.size() == 0)
        AccPSO.add(LinkedHashMap(p -> arrBuf))
      else {
        if (AccPSO.value.get(0).contains(p)) {
          if (AccPSO.value.get(0).apply(p).contains(s)) {
            val map = AccPSO.value.get(0).apply(p).apply(s)
            val arrInt = ArrayBuffer[Int]()
            map += s -> arrInt.+=:(o)
          } else {
            val map = AccPSO.value.get(0).apply(p)
            map += LinkedHashMap(s -> ArrayBuffer(o))
          }

        } else {
          AccPSO.value.get(0).put(p, arrBuf)
        }
      }

      if (AccProperties.value.size() == 0)
        AccProperties.add(TreeSet(p))
      else
        AccProperties.value.get(0) += p
    }
    // collection IDs for cardinalities
    println("=============================================")
    println("|           Cardinality Rule Mining         |")
    println("=============================================")
    parsedCardinalities.take(parsedCardinalities.count().asInstanceOf[Int]).foreach { spo: Cardinalities =>
      val s = getIdForNode(AccNodes, AccIdNodes, spo.subject)
      val p = getIdForNode(AccNodes, AccIdNodes, spo.predicate)

      if (AccECPV.value.size() == 0)
        AccECPV.add(LinkedHashMap(p -> ArrayBuffer(LinkedHashMap(s -> spo.cardinality))))
      else {
        if (AccECPV.value.get(0).contains(p)) {
          val map = AccECPV.value.get(0).apply(p)
          map += LinkedHashMap(s -> spo.cardinality)
        } else
          AccECPV.value.get(0).put(p, ArrayBuffer(LinkedHashMap(s -> spo.cardinality)))

      }
    }
    // getting back the original collections
    val nodes = AccNodes.value.get(0)
    val idNodes = AccIdNodes.value.get(0)
    val properties = AccProperties.value.get(0)
    val pso = AccPSO.value.get(0)
    val ecpv = AccECPV.value.get(0)

    // the main algorithm begins here with the entity IDs in place
    val outputRules = CARLAlgorithm(pso, nodes, idNodes, properties, ecpv, 1000)

    /*
     * post processing the rules output which has different parameters such as body support,
     * confidence etc
     */
    val file = new File("output.tsv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("p\tq\tr\tSupport\tBodySupport\tHeadCoverage\tStdConf\tPCAconf\tCompleteConf\tPrecision\tRecall"
      + "\tDirMetric\tDirCoef\n")
    // formatting output in the output file
    outputRules.foreach {
      rule =>
        bw.write(getNodeForId(nodes, rule.p) + "\t" + getNodeForId(nodes, rule.q)
          + "\t" + getNodeForId(nodes, rule.r) + "\t" + rule.support + "\t"
          + rule.bodySupport + "\t" + rule.headCoverage + "\t"
          + rule.standardConfidence + "\t" + rule.pcaConfidence + "\t"
          + rule.completenessConfidence + "\t"
          + rule.precision + "\t" + rule.recall + "\t"
          + rule.directionalMetric + "\t" + rule.directionalCoef + "\t" + "\n")
    }
    bw.close()
    spark.stop
  }

  /*
   * case class for scopt
   */
  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Preprocessor") {

    head("CARL")

    opt[String]('i', "input").required().valueName("<paths>").
      action((x, c) => c.copy(in = x)).
      text("2 tsv file paths required First file should contain triples and the second should contain the cardinalities ")

    help("help").text("for more info")
  }
}

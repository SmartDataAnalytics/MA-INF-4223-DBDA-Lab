package owl.spark.rdd

import owl.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object OWLXMLSyntaxOWLAxiomsRDDGenerator extends App {

  println("================================")
  println("|        OWL/XML Parser        |")
  println("================================")

  // define the syntax of OWL file
  val syntax = Syntax.OWLXML

  // get absolute path of the owl file
  val input: String = getClass.getResource("/univ-bench.owl").getPath

  /**
  Create a SparkSession, do so by first creating a SparkConf object to configure the application .
  'Local' is a special value that runs Spark on one thread on the local machine, without connecting to a cluster.
  An application name used to identify the application on the cluster manager’s UI.
    */
  val sparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("OWL/XML Parser")
    .getOrCreate()

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger(this.getClass).setLevel(Level.ERROR)

  // get axioms as RDD[Set[OwlAxiom]]
  val rdd = sparkSession.owl(syntax)(input)

  // print the contents of RDD
  rdd.foreach(println(_))

  sparkSession.stop

}

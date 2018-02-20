package net.sansa_stack.template.spark.rdf
import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object TripleReader {

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
      .appName(s"Triple reader example  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")

    val triplesRDD = NTripleReader.load(spark, URI.create(input))

    triplesRDD.take(5).foreach(println(_))

    //triplesRDD.saveAsTextFile(output)

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple reader example") {

    head(" Triple reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
      
    help("help").text("prints this usage text")
  }
}
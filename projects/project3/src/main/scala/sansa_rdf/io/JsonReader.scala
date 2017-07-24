package net.sansa_stack.rdf.spark.io

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.reflect.io.File

object JsonReader {

	def main(args: Array[String]): Unit = {

		val session = SparkSession.builder
			.master("local[*]")
			.appName("RDF SANSA READER")
			.getOrCreate()

		val sqlContext = new org.apache.spark.sql.SQLContext(session.sparkContext)
		val filename = args(0)
		var temp : String = ""
		for (line <- Source.fromFile(filename).getLines) {
			temp ++= line
		}
		val json = sqlContext.read.json(filename)
		json.printSchema()
		json.show(1)
		json.schema.foreach(s=>println(s))

	}
}


package ds
import java.net.URI

import scala.collection.mutable.HashMap

import net.sansa_stack.rdf.spark.io._

import net.sansa_stack.rdf.spark.model._

import org.apache.spark.sql.SparkSession

import org.apache.jena.riot.Lang

import scala.collection.mutable

import org.apache.jena.graph.NodeFactory

import scala.collection.mutable.Map

import scala.collection.mutable.ListBuffer
import ds._


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



    val lang = Lang.NTRIPLES



    val triples = spark.rdf(lang)(input)

    var dict:Map[String,String] = Map()

    var atomlist = triples.collect().toList

    var y = 0

    for(y <- 0 to atomlist.length-1){

      val x = atomlist(y).getPredicate.toString

      //println(x)

      if (x.toString contains "type"){
         dict += ((atomlist(y).getSubject.toString) -> (atomlist(y).getObject.toString))

      }

    }
  

     var triplesArray = new ListBuffer[ListBuffer[Atom]] 

     y=0

     for(y <- 0 to atomlist.length-1){

       var atomName1 = dict((atomlist(y).getSubject.toString))

       var atomnVars1 = 1

       var atomType1 = "Concept"

       var atomVars1 :ListBuffer[String] = ListBuffer(atomlist(y).getSubject.toString)

       val atom1 = new Atom(atomName1,atomType1,atomnVars1)

       atom1.dVars = atomVars1    

       var atomName2 = dict((atomlist(y).getPredicate.toString))

       var atomnVars2 = 2

       var atomType2 = "Role"

       var atomVars2 :ListBuffer[String] = ListBuffer(atomlist(y).getSubject.toString,atomlist(y).getObject.toString)

       val atom2 = new Atom(atomName2,atomType2,atomnVars2)

       atom2.dVars = atomVars2

       var atomName3 = dict((atomlist(y).getSubject.toString))

       var atomnVars3 = 1

       var atomType3 = "Concept"

       var atomVars3 :ListBuffer[String] = ListBuffer(atomlist(y).getObject.toString)

       val atom3 = new Atom(atomName3,atomType3,atomnVars3)

       atom3.dVars = atomVars3    

       

       var atomArray :ListBuffer[Atom] = ListBuffer(atom1,atom2,atom3)

       triplesArray += atomArray

     }

    

    println(triplesArray)    

    println("this is the dict ---->"+dict)



    //triplesRDD.saveAsTextFile(output)    

    println("Number of triples: " + triples.distinct.count())

    println("Number of subjects: " + triples.getSubjects.distinct.count())

    println("Number of predicates: " + triples.getPredicates.count())

    println("Number of objects: " + triples.getObjects.distinct.count())



    //val subjects = triples.filterSubjects(_.isURI()).collect.mkString("\n")

    val subjects = triples.getSubjects.distinct.collect.mkString("\n")

    println("print subject\n"+subjects)

    //val predicates = triples.filterPredicates(_.isVariable()).collect.mkString("\n")

    val predicates = triples.getPredicates.distinct.collect.mkString("\n")

    println("print predicate\n"+predicates)

    //val objects = triples.filterObjects(_.isLiteral()).collect.mkString("\n")

    val objects = triples.getObjects.distinct.collect.mkString("\n")

    println("print object\n"+objects)

    

    var n =  triples.distinct.count().toInt

  

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
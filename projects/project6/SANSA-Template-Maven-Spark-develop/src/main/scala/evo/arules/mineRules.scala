package evo.arules

import ds._
import evo.arules._

import scala.util.Random
import scala.collection.mutable.ListBuffer
import java.net.URI

import scala.collection.mutable
import org.apache.jena.graph.NodeFactory
import net.sansa_stack.rdf.spark.model._
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.model.OWLAxiom
import net.sansa_stack.owl.spark.owl._
import org.apache.spark.rdd.RDD

// This class run the main algorithm and loads the ontology. It parses the owl file. Create a spark session
// generated the required axioms.

object mineRules extends App {


  var a_names = ListBuffer[String]("A", "B", "C", "D", "E","F","G", "H","I","J","K","L","M","N","O","P")
  var a_types = ListBuffer[String]("Role", "Concept")

  var r = new Rule()
  //println("Hello World"+r)

  val ops = new Operations()
  //create_dataset()


  lazy val spark = SparkSession.builder().appName("MultiAssociation Rule Mining  $input").master("local[*]")
    .config(
      "spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()

  val syntax = Syntax.FUNCTIONAL

  val filePath = "/Users/autoninja/Desktop/project2/SANSA-Template-Maven-Spark-develop/src/main/resources/product.owl"//functionalformatfinanicial.owl"//this.getClass.getClassLoader.getResource("owlfile.owl").getPath
  println (filePath)


  val rdd = spark.owl(syntax)(filePath)
//  def rdd: OWLAxiomsRDD = {
//    if (_rdd == null) {
//      _rdd = spark.owl(syntax)(filePath)
//      _rdd.cache()
//    }
//    _rdd
//  }
  run_iterations()
  def generate_random_atoms():Atom={

    var atom: Atom = new Atom("A","Concept",1, vars = ListBuffer(1))
    val rand = new Random()
    val nameToUse = a_names.remove(rand.nextInt(a_names.length))
    val typeToUse = a_types(rand.nextInt(a_types.length))
    if (typeToUse=="Concept"){
      atom = new Atom(nameToUse,typeToUse, nvars = 1, vars = ListBuffer(1))
    }
    else{
      atom = new Atom(nameToUse,typeToUse, nvars = 2, vars = ListBuffer(1,2))
    }
    return atom
  }
  
  def make_atoms(aStringList:ListBuffer[String], aType:String):ListBuffer[Atom]={
    
    var aList = new ListBuffer[Atom]
    for( i <- aStringList){
      var atom: Atom = null
      var nameToUse = i
      var typeToUse = aType
      if (typeToUse=="Concept"){
        atom = new Atom(nameToUse,typeToUse, nvars = 1, vars = ListBuffer(1))
      }
      else{
        atom = new Atom(nameToUse,typeToUse, nvars = 2, vars = ListBuffer(1,2))
      }
      aList+=atom
    }
    return aList
  }

  // This function runs the main algorithm 
  def run_iterations()={
    
    val rddcount = rdd.count()
    //list having sublcass axioms
    val subclassRDD =  rdd.filter(axiom => axiom.isInstanceOf[OWLSubClassOfAxiom])
    // list that has all the classes names and its instances. It basically stores all the Class Assertion Axioms.
    var conceptlist = scala.collection.mutable.ListBuffer.empty[Map[String, String]]
    // list that has all the role names, the instance used by it and implied data. It basically stores all the Object Property Assertion Axioms and Data Property Axioms.
    var rolelist = scala.collection.mutable.ListBuffer.empty[Map[String, String]]
    // storing the count of the classes in the whole ontology
    var conceptCount = scala.collection.mutable.Map.empty[String, Int]
    // storing the count of the roles in the whole ontology.
    var roleCount = scala.collection.mutable.Map.empty[String, Int]
    // filtering the RDD with required axioms to create rules
    val filteredRDD = rdd.filter(axiom => axiom.isInstanceOf[OWLDataPropertyAssertionAxiom] || axiom.isInstanceOf[OWLClassAssertionAxiom] || axiom.isInstanceOf[OWLObjectPropertyAssertionAxiom])
    // println(filteredRDD.count)
    
    filteredRDD.collect.foreach(selectedRdd=>{
      val splittedArray= selectedRdd.toString().split("[<,>]")
      if (splittedArray(0) contains "Class"){
        val myMap = Map("Class"->splittedArray(1), "Object"->splittedArray(3))
        conceptlist += myMap
        if (conceptCount.contains(splittedArray(1))){
          val newcount = conceptCount(splittedArray(1)) + 1
          conceptCount(splittedArray(1)) = newcount
        }
        else{
          conceptCount(splittedArray(1)) = 1
        }
      }
      else{
        val myMap = Map("Role"->splittedArray(1), "Object"->splittedArray(3), "Data" -> splittedArray(4) )
        rolelist += myMap
        if (roleCount.contains(splittedArray(1))){
          val newcount = roleCount(splittedArray(1)) + 1
          roleCount(splittedArray(1)) = newcount
        }
        else{
          roleCount(splittedArray(1)) = 1
        }
      }
    })

    var conceptNames = scala.collection.mutable.ListBuffer.empty[String] 
    var roleNames = scala.collection.mutable.ListBuffer.empty[String] 
    for(x <- conceptlist){
      if (!(conceptNames contains x("Class")) ){
      conceptNames += x("Class")
      }
    }
    
     for(x <- rolelist){
       if (! (roleNames contains x("Role"))){
         roleNames += x("Role")  
       }
      
    }
      
    var A_f_role = new ListBuffer[Atom]
    var A_f_concept = new ListBuffer[Atom]
    var op = new Operations()
    
     
    op.a_f_Concept = conceptNames
    op.a_f_Role = roleNames

    
    val numIters = 3 // Number of iteration for the genetic algorithms
    val tau = 0.2 // Truncation parameter
    val N = 6    // Number of rules
    val tp = scala.math.floor(tau*N)
    //val tp = 1
    
    var ruleArray = new ListBuffer[Rule]
    for(i <- 1 to N){
      ruleArray+=op.createNewPattern()
    }
    
    var opRDDbroadcast = spark.sparkContext.broadcast(spark.sparkContext.parallelize(Seq(op)).collect())
    var ruleRDD = spark.sparkContext.parallelize(ruleArray)
    //ruleRDD.foreach(println)
    
    println("Evolutionary Rule Mining started..")
    for (iter<- 1 to numIters){

      ruleRDD.map(r => {
        r.hC = opRDDbroadcast.value(0).calculateHeadCoverage(r)
      })
      ruleRDD.sortBy(_.hC)//.collect()
      
     
      var topRulesRDD = ruleRDD.zipWithIndex().filter(_._2<tp).map(_._1)
      var bottomRulesRDD = ruleRDD.zipWithIndex().filter(_._2<N-tp).map(_._1)
      
      topRulesRDD.collect()
      bottomRulesRDD.collect()
      topRulesRDD.foreach(println)
      bottomRulesRDD.foreach(println)
      
      var jointRulesRDD = bottomRulesRDD.zipWithIndex().filter(t=>(t._2+1)%2!=0).map(_.swap).join(
        bottomRulesRDD.zipWithIndex().filter(t=>(t._2+1)%2==0).mapValues(f=>f-1).map(_.swap))
        .map(_._2)
      
      var processedRulesRDD = jointRulesRDD.map(x => {
        val rand = new Random()
        var r1 = x._1
        var r2 = x._2
        if(rand.nextFloat()<op.pcross){
          var k = opRDDbroadcast.value(0).recombine(x._1, x._2)
          r1 = k._1
          r2 = k._2
        }
        if(rand.nextFloat()<op.pmut){
          r1 = opRDDbroadcast.value(0).mutate(r1)
        }
        if(rand.nextFloat()<op.pmut){
          r2 = opRDDbroadcast.value(0).mutate(r2)
        }
        ListBuffer(r1,r2)
      }).flatMap(x => x)
      
      processedRulesRDD.collect()
      ruleRDD = topRulesRDD.union(processedRulesRDD)
    }
    //ruleRDD.foreach(println)
    ruleRDD.repartition(1).saveAsTextFile("output")
  }

}
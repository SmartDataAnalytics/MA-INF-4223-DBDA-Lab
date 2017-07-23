/* Author: Akhilesh Vyas
 *
 */

package net.sansa_stack.template.spark.rdf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j._

object  EntityTypeInformation extends App {
  

  Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)
  
  val inputDataSet = "src/main/resources/datset.nt"
  
  val conf = new SparkConf().setAppName("GraphXTest").setMaster("local")
  
  val sc = new SparkContext(conf)
   
  
  val spark = SparkSession.builder.master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("GraphX example").getOrCreate()
  
  val tripleRDD = spark.sparkContext.textFile(inputDataSet).map(TripleUtils.parsTriples)
  
  val tutleSubjectObject = tripleRDD.map { x => (x.subject, x.`object`) }
  
  type VertexId = Long
  
  val indexVertexID = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
     
  val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))
      
  val tuples = tripleRDD.keyBy(_.subject).join(indexVertexID).map(
        {
          case (k, (TripleUtils.Triples(s, p, o), si)) => (o, (si, p))
        })
       
       
  val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({ case (k, ((si, p), oi)) => Edge(si, oi, p) })
   
  val graph = Graph(vertices, edges.distinct())
     
      //Taking one predicate of FPC to know entities meanwhile
  //val predicateListFromFpc = List("http://data.semanticweb.org/ns/swc/ontology#heldBy")
   val predicateListFromFpc = graph.edges.flatMap(x=>List(x.attr)).collect().distinct.toList
    
  val t1 = System.nanoTime
  
  /*def searchEdge(edgeArg1:String, edgeArg2:String):Boolean={
     edgeArg1==edgeArg2
    }*/
  
  def searchEdge(edgeArg:String, edgeArgList:List[String]):Boolean={
    edgeArgList.contains(edgeArg)
    
  }
  def searchDesEntity(id:Long, iDlist:List[Long], edgeArgument:String, typeArgument:String):Boolean= {
      ((iDlist.contains(id))&&(edgeArgument==typeArgument))
     }
  
  def searchId(id:Long, idList:List[Long]):Boolean={
    
    idList.contains(id)
    
  }
    
  //for(aPredict <- predicateListFromFpc)
  
  //{
  
    val setOfEntity = graph.triplets.filter(triple=> searchEdge(triple.attr, predicateListFromFpc)).flatMap(triple=>List(triple.srcId)).cache()
   
    val setOfDistictEntity = setOfEntity.distinct().collect().toList
    //val printsetofdistictentity = setOfDistictEntity.foreach(println)
    
   
    val setOfTypeList = graph.triplets.filter(triple=>searchDesEntity(triple.srcId, setOfDistictEntity , triple.attr, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).map(x=>(x.dstId, 1)).reduceByKey((a,b)=>(a+b)).cache()
  
    val support_count = 10
    
    var freqSetOfTypeList = setOfTypeList.filter(feqEntity=>feqEntity._2 >= support_count).sortBy(_._2, false)
   
    println("")
   
    val k:Int = 4
  
    if(freqSetOfTypeList.count() > k ){
    /*List to RDD transformation Take top K FrequentType */
     freqSetOfTypeList = sc.parallelize(freqSetOfTypeList.take(k)) 
     }
    
    println("---------------------FreqSetOfTypeListWithVertexId-------------------")
    
    val freqsetoftypelistRdd =  freqSetOfTypeList.map(x=>(x._1)).foreach(println)
    
    val durationforFreqTypeList = (System.nanoTime - t1) / 1e9d
     println("\nDone--"+"Duration for FreqSetOfTypeList:"+ durationforFreqTypeList)
  
    println("\n---------------------FreqSetOfTypeListWithArgument-------------------")
    
    val freqsetoftypelistK = freqSetOfTypeList.map(x=>x._1).collect().toList
    
    val freqSetOfTypeListArg = graph.triplets.filter(x=> searchId(x.dstId, freqsetoftypelistK)).map(x=>(x.dstAttr)).distinct()
    freqSetOfTypeListArg.foreach(println)
 
    
    /* Adding TypeInformation to Real Graph */
    val t2 = System.nanoTime
  
    val edgeArray:Array[Edge[String]] = Array()
  
    for(i <- 0 to (freqSetOfTypeList.collect().toList.length - 1))
   {
     for(j <- 0 to (setOfDistictEntity.length - 1 ))
    {
       edgeArray:+Edge(setOfDistictEntity(j), freqSetOfTypeList.collect().toList(i)._1,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type" )
    }
   }
  
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)
  
    val graphUpdated = Graph(vertices, edges.union(edgeRDD).distinct())
    
    println("")
    
    println("-----New Updated Edges--------------")
    
    println(graphUpdated.edges.count())
    
    println("-----Old Updated Edges--------------")
    
    println(graph.edges.count())
  
    val durationforUpdatedGraph = (System.nanoTime - t2) / 1e9d  
    
    println("\nDone--"+"Duration for Updating Graph:"+ durationforUpdatedGraph)
  //}
    spark.stop
}

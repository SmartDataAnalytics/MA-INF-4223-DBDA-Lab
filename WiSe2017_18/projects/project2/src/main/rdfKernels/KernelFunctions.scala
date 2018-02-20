package rdfKernels

import java.lang.System
import java.net.URI
import net.sansa_stack.rdf.spark.io.ntriples.NTriplesDataSource
import net.sansa_stack.rdf.spark.graph._
import scala.collection.mutable._
import scala.util.Random
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.jena.graph.Triple

/**
 * Class containing methods for computing kernel functions for intersection
 * graphs and intersection trees. Designed to process RDF data packed in Spark
 * Resilient Distributed Datasets.
 */
class KernelFunctions {
 
  /**
 	* Calculates adjacency matrix for intersection of two RDF graphs.
 	* @param firstRDD Resilient Distributed Dataset containing entries of RDF triples.
 	* @param secondRDD Resilient Distributed Dataset containing entries of RDF triples.
 	* @return CoordinateMatrix containing '1' at the positions where edges exist and '0'
 	* otherwise.
 	*/
  def intersectionAdjacencyMatrix(firstRDD: RDD[Triple], secondRDD: RDD[Triple]) : CoordinateMatrix = {
   
    val intersectRDD = firstRDD.intersection(secondRDD)
    val graph = LoadGraph(intersectRDD)
    val neighbours = graph.collectNeighborIds(EdgeDirection.In)
    //calculate dims for adjacency matrix
    val maxKey = neighbours.max()(new Ordering[Tuple2[VertexId, Array[VertexId]]]() {
      override def compare(x: (VertexId, Array[VertexId]), y: (VertexId, Array[VertexId])): Int = 
        Ordering[VertexId].compare(x._1.toLong, y._1.toLong)
    })
    
    //create CoordinateMatrix:
		//neighbourTuples: RDD of (Long, Long) values which represent existing connections between graph nodes
		val neighbourTuples = neighbours.flatMap(neighbour => neighbour._2.map(neighbourId => (neighbour._1.longValue(), neighbourId)))
		val matrixEntries = neighbourTuples.map(tuple => MatrixEntry(tuple._1, tuple._2, 1))
		val adjacencyMatrix: CoordinateMatrix = new CoordinateMatrix(matrixEntries, maxKey._1.toLong+1, maxKey._1.toLong+1)
    
    return adjacencyMatrix
  }
  
  /**
 	* Calculates path kernel of a graph represented by adjacency matrix. Path kernel is calculated
 	* as a weighted sum of number of possible paths up to given length.
 	* @param adjacencyMatrix CoordinateMatrix representing the graph structure.
 	* @param depth maximal number of edges which a path can contain, i.e. maximal path length.
 	* @param lambda discount factor, which allows to weight long and short paths differently; values
 	* bigger than 1 contribute to long paths and otherwise.
 	* @return Double value of path kernel. 
 	*/
  def matrixPathKernel(adjacencyMatrix: CoordinateMatrix, depth: Int, lambda: Double) : Double = {
   
    var currentMatrix = new CoordinateMatrix(adjacencyMatrix.entries.map({case MatrixEntry(i, j, v) => if (i==j) MatrixEntry(i, j, 0) else MatrixEntry(i, j, v)}))
    var pathKernel = 0d
   
    for(iter <- 1 to depth){
       currentMatrix.entries.collect.foreach({case MatrixEntry(i, j, v) => pathKernel += v.*(scala.math.pow(lambda, iter))})     
       currentMatrix = coordinateMatrixMultiply(currentMatrix, adjacencyMatrix)
    }
    return pathKernel
  }
 
  /**
 	* Calculates walk kernel of a graph represented by adjacency matrix. Walk kernel is calculated
 	* as a weighted sum of number of possible walks up to given length. 
 	* @param adjacencyMatrix CoordinateMatrix representing the graph structure.
 	* @param depth maximal number of edges which a walk can contain, i.e. maximal walk length.
 	* @param lambda discount factor, which allows to weight long and short walks differently; values
 	* bigger than 1 contribute to long walks and otherwise.
 	* @return Double value of walk kernel. 
 	*/
  def matrixWalkKernel(adjacencyMatrix: CoordinateMatrix, depth: Int, lambda: Double) : Double = {
   
    var currentMatrix = adjacencyMatrix
    var walkKernel = 0d
   
    for(iter <- 1 to depth){
       currentMatrix.entries.collect.foreach({case MatrixEntry(i, j, v) => walkKernel += v.*(scala.math.pow(lambda, iter))})
       currentMatrix = coordinateMatrixMultiply(currentMatrix, adjacencyMatrix)
    }
    return walkKernel
  }
  
  /**
 	* Helper function for matrix multiplication.
 	* @param leftMatrix left product matrix of type CoordinateMatrix.
 	* @param rightMatrix right product matrix of type CoordinateMatrix.
 	* @return CoordinateMatrix which is a result of multiplication. 
 	*/
  def coordinateMatrixMultiply(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {
  
    val M = leftMatrix.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
    val N = rightMatrix.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

    val productEntries = M
        .join(N)
        .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
        .reduceByKey(_ + _)
        .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

    return new CoordinateMatrix(productEntries, leftMatrix.numRows(), rightMatrix.numCols())
  }
  
  /**
 	* Calculates path kernel of an intersection of two RDF datasets represented by RDDs. Kernel is calculated
 	* as a weighted sum of number of possible paths up to given length. Paths are constructed iteratively by
 	* propagating messages over intersection graph.
 	* @param firstRDD Resilient Distributed Dataset containing entries of RDF triples.
 	* @param secondRDD Resilient Distributed Dataset containing entries of RDF triples.
 	* @param depth maximal number of edges which a path can contain, i.e. maximal path length.
 	* @param lambda discount factor, which allows to weight long and short paths differently; values
 	* bigger than 1 contribute to long paths and otherwise.
 	* @return Double value of path kernel. 
 	*/
  def msgPathKernel(firstRDD: RDD[Triple], secondRDD: RDD[Triple], depth: Int, lambda: Double) : Double = {
    
    var kernel = 0d
    
    val intersectRDD = firstRDD.intersection(secondRDD)  
    val graph = LoadGraph(intersectRDD)
    
    var rec_paths = graph.mapVertices((id,attr) => (List(List(id)),List(List())))
    var walk_create: VertexRDD[(List[List[VertexId]], List[List[VertexId]])] = 
        rec_paths.aggregateMessages[(List[List[VertexId]],List[List[VertexId]])]( predic => 
        {predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
        (a, b) => (b._1,a._2 ++ b._2))
    var paths = walk_create.mapValues((id,attr) => 
        (attr._2.filter(!_.contains(id)).map(_ ++ List(id)) , List(List())))
    
    val numpaths = (paths.map(_._2._1.length).reduce(_ + _))
    kernel += lambda * numpaths
    
    for( i <- 2 to depth){
       rec_paths = rec_paths.joinVertices(paths)((id,old_attr, new_attr) => (new_attr._1, new_attr._2))
       walk_create = rec_paths.aggregateMessages[(List[List[VertexId]],List[List[VertexId]])]( predic => 
           {predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
           (a, b) => (b._1,a._2 ++ b._2))
       paths = walk_create.mapValues((id,attr) => 
           (attr._2.filter(!_.contains(id)).map(_ ++ List(id)) , List(List())))
      
       val numpaths = (paths.map(_._2._1.length).reduce(_ + _))
       kernel += scala.math.pow(lambda,depth)*numpaths
    }
    return kernel
  }
 
  /**
 	* Calculates walk kernel of an intersection of two RDF datasets represented by RDDs. Kernel is calculated
 	* as a weighted sum of number of possible walks up to given length. Walks are constructed iteratively by
 	* propagating messages over intersection graph.
 	* @param firstRDD Resilient Distributed Dataset containing entries of RDF triples.
 	* @param secondRDD Resilient Distributed Dataset containing entries of RDF triples.
 	* @param depth maximal number of edges which a path can contain, i.e. maximal path length.
 	* @param lambda discount factor, which allows to weight long and short paths differently; values
 	* bigger than 1 contribute to long paths and otherwise.
 	* @return Double value of walk kernel. 
 	*/
  def msgWalkKernel(firstRDD: RDD[Triple], secondRDD: RDD[Triple], depth: Int, lambda: Double) : Double = {
    
    var kernel = 0d
    
    val intersectRDD= firstRDD.intersection(secondRDD)   
    val graph = LoadGraph(intersectRDD)
    
    val init_paths = graph.mapVertices((id, attr) => (0, 0))
    var rec_paths = init_paths.mapVertices((id, attr) => (1,0))
    var paths: VertexRDD[(Int, Int)] = rec_paths.aggregateMessages[(Int, Int)]( predic => {predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
        (a, b) => (b._1,a._2 + b._2))
    var numpaths = paths.map(_._2._2).reduce(_ + _)
    kernel += lambda *numpaths 
    
    for( i <- 2 to depth){
       rec_paths =  init_paths.joinVertices(paths)((id, old_attr, new_attr) => (new_attr._2, 0))
       paths = rec_paths.aggregateMessages[(Int, Int)]( predic => {if(predic.srcAttr._1>0) predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
           (a, b) => (b._1,a._2 + b._2))
       
       numpaths = paths.map(_._2._2).reduce(_ + _)
     	 kernel += scala.math.pow(lambda,depth)*numpaths
    }
    return kernel
  }
  
  //Calculates and builds the intersection Tree, Return: Graph
    //Input: Graph, e1, e2, spark of type Type: Sparksession
  /**
 	* Calculates and builds the intersection Tree between two nodes of a given graph or null if no intersection
 	* tree exists within given depth.
 	* @param graphRDD Resilient Distributed Dataset containing entries of RDF graph triples.
 	* @param e1 ID of the first node.
 	* @param e2 ID of the second node.
 	* @param depth maximal number of hops from each node.
 	* @param spark instance of SparkSession.
 	* @return Graph which represents the intersection tree or null if such tree cannot be found. 
 	*/
  def constructIntersectionTree(graphRDD: RDD[Triple], e1: Long, e2: Long, depth:Int, spark: SparkSession) : Graph[Null, String] = {   
    
    val graph = LoadGraph(graphRDD)
    var rd = new Random()
    val init_neighbourhood = graph.collectNeighborIds(EdgeDirection.In)
    val graph_init = graph.mapVertices((id,attr) => (List[List[Long]](), List[List[Long]](),0,id))
    var rec_graph = graph_init
    var rec_tree = init_neighbourhood.mapValues((id,attr) 
        => if(attr.contains(e1) && attr.contains(e2))(List(List(id)), List[List[Long]](),1,id) else (List(),List(),0,id))
    var rec_paths = graph_init.joinVertices(rec_tree)((id,old_attr,new_attr) => (new_attr._1,new_attr._2,new_attr._3,new_attr._4))
    
    // Check for empty intersection
    if(rec_tree.map(_._2._3).reduce(_ + _) == 0)
       return null
    else {  
       var root = math.abs(rd.nextLong())
       var TreeNodes = List((root, (" ", " ")))
       var TreeEdges = List[Edge[String]]()
      
       // For triggering Side Effects
       def addToRoot(Id:Long): Long = {
          TreeNodes = TreeNodes ::: List((Id, (" ", " ")))
          val ed = new Edge(root,Id,"")
          TreeEdges = TreeEdges ::: List(ed)
          return Id
       }
      
       def addToTree(Id_anc:Long,Id_des:Long): Long = {
          TreeNodes = TreeNodes ::: List((Id_des, (" ", " ")))
          val ed = new Edge(Id_anc,Id_des,"")
          TreeEdges = TreeEdges ::: List(ed)
          return Id_des
       }
      
      // Add neighbours as child of root
      rec_tree = rec_tree.mapValues((id,args) => if(args._3 == 1) (List(List(args._4)),args._2,args._3, args._4) else (args._1,args._2,args._3, args._4))
      rec_tree.collect().foreach((a) => if(a._2._3 == 1) addToRoot(a._2._4)) 
      
      //Update Keys in arg_4:
      rec_tree = rec_tree.mapValues((id,args) => {
         rd = new Random(args._4)
         (args._1,args._2,0,math.abs(rd.nextLong()))
      })
      
      rec_graph = graph_init.joinVertices(rec_tree)((id,old_attr,new_attr) => new_attr)
       
      for(i <- 2 to depth){
         // Send Messages containing all List of parent calls + Constants for arbritary message order
         rec_tree = rec_graph.aggregateMessages[(List[List[Long]], List[List[Long]],Int,Long)] ( predic 
             =>  {if(!(predic.srcAttr._1.isEmpty)) predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1,1,predic.dstAttr._4)},
             (a, b) => (a._1, (a._2 ::: b._2), a._3, a._4))

         rec_tree = rec_tree.mapValues((id, attr) => (attr._2, List(List()), attr._3, attr._4))
         rec_tree.collect.foreach(
            (a) => {
               var Parents = a._2._1
               for (par <- Parents){
                  addToTree(par.last,a._2._4)
               }
            (a)}
         )        
      
         rec_tree = rec_tree.mapValues((id,attr) => 
         (attr._1.map(_ ++ List(attr._4)) , attr._2,attr._3,attr._4))
        
         
         rec_graph = rec_graph.mapVertices((id,attr)=> (attr._2,attr._2,0,attr._4))
         rec_graph = rec_graph.joinVertices(rec_tree)((id,old_attr,new_attr) => 
             (new_attr._1,List[List[Long]](),new_attr._3, new_attr._4))

         rec_graph = rec_graph.mapVertices((id,attr) => 
            {
               rd = new Random(attr._4)
               (attr._1, attr._2, 0, rd.nextLong()) 
            })
      }

      val edges = spark.sparkContext.parallelize(TreeEdges)
      return Graph.fromEdges(edges, null)
    }
  } 
}
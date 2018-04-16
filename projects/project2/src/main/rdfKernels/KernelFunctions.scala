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
import org.apache.jena.graph.Node

/**
 * Class containing methods for computing kernel functions for intersection
 * graphs and intersection trees. Designed to process RDF data packed in Spark
 * Resilient Distributed Datasets.
 */
object KernelFunctions {
 
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
    
    // Initialize Data Structure for storing all incoming paths of current and last
    // iteration
    var init_paths = graph.mapVertices((id,attr) => (List[List[VertexId]](),List(List())))
    var rec_paths = graph.mapVertices((id,attr) => (List(List(id)),List(List())))
    // Send first message for initalizing walk_create
    // Each node, sends its incoming edges from last iteration, collectcs the signals
    // and appends them to the list of currently ending paths
    var walk_create: VertexRDD[(List[List[VertexId]], List[List[VertexId]])] = 
        rec_paths.aggregateMessages[(List[List[VertexId]],List[List[VertexId]])]( predic => 
        {predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
        (a, b) => (a._1,a._2 ++ b._2))
    var paths = walk_create.mapValues((id,attr) => 
        (attr._2.filter(!_.contains(id)).map(_ ++ List(id)) , List(List())))
    
    val numpaths = (paths.map(_._2._1.length).reduce(_ + _))
    kernel += lambda * numpaths
    // Same as above but for higher iterations.
    for( i <- 2 to depth){
       rec_paths = init_paths.joinVertices(paths)((id,old_attr, new_attr) => (new_attr._1, new_attr._2))
       walk_create = rec_paths.aggregateMessages[(List[List[VertexId]],List[List[VertexId]])]( predic => 
           {predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
           (a, b) => (a._1,a._2 ++ b._2))
       paths = walk_create.mapValues((id,attr) => 
           (attr._2.filter(!_.contains(id)).map(_ ++ List(id)) , List(List())))
       val numpaths = (paths.map(_._2._1.length).reduce(_ + _))
       kernel += scala.math.pow(lambda,i)*numpaths
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
        (a, b) => (a._1,a._2 + b._2))
    var numpaths = paths.map(_._2._2).reduce(_ + _)
    kernel += lambda *numpaths 
    
    for( i <- 2 to depth){
       rec_paths =  init_paths.joinVertices(paths)((id, old_attr, new_attr) => (new_attr._2, 0))
       paths = rec_paths.aggregateMessages[(Int, Int)]( predic => {if(predic.srcAttr._1>0) predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1)},
           (a, b) => (a._1,a._2 + b._2))
       
       numpaths = paths.map(_._2._2).reduce(_ + _)
     	 kernel += scala.math.pow(lambda,i)*numpaths
    }
    return kernel
  }
  
  //Calculates and builds the intersection Tree, Return: Graph
    //Input: Graph, e1, e2, spark of type Type: Sparksession
  /**
 	* Calculates and builds the intersection Tree between two nodes of a given graph or null if no intersection
 	* tree exists within given depth.
 	* @param graph A Graph in the format outputed by LoadGraph() from the Sansa Stack
 	* @param e1 ID of the first node in graph.
 	* @param e2 ID of the second node in graph.
 	* @param depth maximal number of hops from each node.
 	* @param spark instance of SparkSession.
 	* @return Graph which represents the intersection tree or null if such tree cannot be found. 
 	*/
  def constructIntersectionTree(graph: Graph[Node,Node], e1: Long, e2: Long, depth:Int, spark: SparkSession) : Graph[Null, String] = {   
 
    // Initialize everything.
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
      rec_tree = rec_tree.mapValues((id,attr) => {
         rd = new Random(attr._4)
         (attr._1,attr._2,0,math.abs(rd.nextLong()))
      })
      
      rec_graph = graph_init.joinVertices(rec_tree)((id,old_attr,new_attr) => new_attr)
       
      for(i <- 2 to depth){
         // Send Messages containing all List of parent calls + Constants for arbritary message order
         rec_tree = rec_graph.aggregateMessages[(List[List[Long]], List[List[Long]],Int,Long)] ( predic 
             =>  {if(!(predic.srcAttr._1.isEmpty)) predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1,1,predic.dstAttr._4)},
             (a, b) => (a._1, (a._2 ::: b._2), a._3, a._4))

         // Update storage for next iteration
         rec_tree = rec_tree.mapValues((id, attr) => (attr._2, List(List()), attr._3, attr._4))
         
         // Map to each ending path a new random id, append it to the path list.
         rec_tree = rec_tree.mapValues((id,attr) =>
           {
             rd = new Random(attr._4)
             (attr._1.map(_ ++ List(rd.nextLong)), attr._2,attr._3,rd.nextLong())
           }
         )
         
         // Create all new Edges by collecting and appending them
         rec_tree.collect.foreach(
            (a) => {
               var Parents = a._2._1
               for (par <- Parents){
                  var x = par.reverse
                  addToTree(x(1),x(0))
               }
            (a)}
            
           )

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
  
  

  /**
 	* Calculates the Full Subtree Kernel Tree on an interscectionTree Graph
 	* @param g A Graph in the format outputed by constructIntersectionTree 
 	* @param depth: The depth to calculate. Use the same depth as for the intersection Tree.
 	* @param lambda: Scaling factor
 	* @return The Full Subtree Kernel for the parameters.
 	*/
 
  def FullSubtreeKernel(g: Graph[Null,String], depth: Int, lambda: Double) : Double = {
    // Initialize Kernel
    var kernel = 0d
    var rev = g.reverse
    val in = rev.inDegrees
    var rec_graph = rev.mapVertices[(Double,Double,Int,Int)]((id,attr)=>(0,0,0,0))
    rec_graph = rec_graph.joinVertices(in)((id,old_attr,new_attr) => (0,0,0,new_attr) )
    rec_graph = rec_graph.mapVertices((id,attr) => if(attr._4 == 0) (1,0,0,1) else (attr._1,attr._2,attr._3,attr._4))
    // Send all messages one round forward
    var one_step = rec_graph.aggregateMessages[(Double, Double, Int, Int)]( predic => {if(predic.srcAttr._1 != 0) predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1, 1 , predic.dstAttr._4)},
       (a, b) => (a._1, a._2 + b._2, a._3 + b._3, a._4))
       
    //Iterate until root is found
    for(i <- 1 to depth-1){
      rec_graph = rec_graph.mapVertices((id,attr) => (0, attr._2,attr._3,attr._4))
      rec_graph = rec_graph.joinVertices(one_step)((id,old_attr,new_attr) => (0d, new_attr._2 + old_attr._2, old_attr._3 + new_attr._3 , old_attr._4))
      rec_graph = rec_graph.mapVertices((id,attr) => if(attr._3 == attr._4) (1+lambda*attr._2,0,0,attr._4) else (0,attr._2,attr._3,attr._4))  
      one_step = rec_graph.aggregateMessages[(Double, Double, Int, Int)]( predic => {if(predic.srcAttr._1 > 0) predic.sendToDst(predic.dstAttr._1,predic.srcAttr._1,1,predic.dstAttr._4)},
       (a, b) => (a._1,a._2 + b._2, a._3 + b._3,a._4))


    }
    kernel = 1 + lambda *rec_graph.vertices.map(_._2._1).reduce(_ + _)
    return(kernel)
  }
  
  /**
 	* Calculates the Partial Subtree Kernel Tree on an interscectionTree Graph
 	* @param g A Graph in the format outputed by constructIntersectionTree 
 	* @param depth: The depth to calculate. Use the same depth as for the intersection Tree.
 	* @param lambda: Scaling factor
 	* @return The Full Subtree Kernel for the parameters.
 	*/
 
  def PartialSubtreeKernel(g: Graph[Null,String], depth: Int, lambda: Double) : Double = {
    // Initialize Kernel
    var kernel = 0d
    var rev = g.reverse
    val in = rev.inDegrees
    var rec_graph = rev.mapVertices[(Double,Double,Int,Int)]((id,attr)=>(0,1,0,0))
    rec_graph = rec_graph.joinVertices(in)((id,old_attr,new_attr) => (0,1,0,new_attr) )
    rec_graph = rec_graph.mapVertices((id,attr) => if(attr._4 == 0) (1,0,0,1) else (attr._1,attr._2,attr._3,attr._4))
    // Send all messages one round forward
    var one_step = rec_graph.aggregateMessages[(Double, Double, Int, Int)]( predic => {if(predic.srcAttr._1 != 0) predic.sendToDst(predic.dstAttr._1,1+lambda*predic.srcAttr._1, 1 , predic.dstAttr._4)},
       (a, b) => (a._1, a._2 * b._2, a._3 + b._3, a._4))
    
    // Iterate until root
    for(i <- 1 to depth-1){
      rec_graph = rec_graph.mapVertices((id,attr) => (0, attr._2,attr._3,attr._4))
      rec_graph = rec_graph.joinVertices(one_step)((id,old_attr,new_attr) => (0d, new_attr._2 * old_attr._2, old_attr._3 + new_attr._3 , old_attr._4))
      rec_graph = rec_graph.mapVertices((id,attr) => if(attr._3 == attr._4) (attr._2,0,0,attr._4) else (0,attr._2,attr._3,attr._4))  
      one_step = rec_graph.aggregateMessages[(Double, Double, Int, Int)]( predic => {if(predic.srcAttr._1 > 0) predic.sendToDst(predic.dstAttr._1,1+lambda*predic.srcAttr._1,1,predic.dstAttr._4)},
       (a, b) => (a._1,a._2 * b._2, a._3 + b._3,a._4))
    }
    kernel = rec_graph.vertices.map(_._2._1).reduce(_ + _)
    return(kernel)
  }
  
}
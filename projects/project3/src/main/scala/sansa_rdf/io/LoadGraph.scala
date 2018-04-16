package sansa_rdf.io

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.jena.graph.Node

/**
  * Constructs a GraphX graph from the given RDD of triples
  *
  */
object LoadGraph {


  /**
    * Constructs GraphX graph from RDD of triples
    * @param triples rdd of triples
    * @return object of LoadGraph which contains the constructed  ''graph'' and ''NodeToId'' maps
    * */
  def apply(triples : RDD[Triple]) = {

    def getNodeValue(node: Node): String = {
      if(node.isURI){
        node.getURI
      }
      else if(node.isBlank){
        node.getBlankNodeId.toString
      }
      else if(node.isLiteral){
        node.getLiteral.toString()
      }
      else{
        throw new IllegalArgumentException("Not a valid node for quads")
      }
    }

    val rs = triples.map(triple=>(getNodeValue(triple.getSubject),getNodeValue(triple.getPredicate),getNodeValue(triple.getObject)))
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithUniqueId()

    val vertices: RDD[(VertexId,String)] = indexedMap.map(x=>(x._2,x._1))
    val _nodeToId: RDD[(String,VertexId)] = indexedMap.map(x=>(x._1,x._2))


    val tuples = rs.keyBy(_._1).join(indexedMap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    org.apache.spark.graphx.Graph(vertices, edges)

    new {
      val graph = org.apache.spark.graphx.Graph(vertices, edges)

      // a mapping from value of vertex(value of jena Node ) to its VertexId
      val nodeToId = _nodeToId
    }
  }

  /**
    * Constructs GraphX graph from RDD of triples
    * @param triples rdd of triples of type String
  @return object of LoadGraph which have the constructed  ''graph'' and ''nodeToId'' map
    * */

  def makeGraph(triples : RDD[String]) = {

    val rs = triples.map(triple => triple.split(" ") ).map(t => (t(0),t(1),t(2)) )
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithUniqueId()

    val vertices: RDD[(VertexId,String)] = indexedMap.map(x=>(x._2,x._1))
    val _nodeToId: RDD[(String,VertexId)] = indexedMap.map(x=>(x._1,x._2))


    val tuples = rs.keyBy(_._1).join(indexedMap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    org.apache.spark.graphx.Graph(vertices, edges)

    new {
      val graph = org.apache.spark.graphx.Graph(vertices, edges)

      // a mapping from value of vertex to its VertexId
      val nodeToId = _nodeToId
    }

  }


/*  /**
    * Constructs GraphX graph from RDD of triples
    * @param triples rdd of triples
    * @return object of LoadGraph which have the constructed  ''graph'' and ''iriToId'' map
    * */
  def apply(triples : Array[Triple]) = {

    import scalax.collection.Graph
    import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._


    def getNodeValue(node: Node): String = {
      if(node.isURI){
        node.getURI
      }
      else if(node.isBlank){
        node.getBlankNodeId.toString
      }
      else if(node.isLiteral){
        node.getLiteral.toString()
      }
      else{
        throw new IllegalArgumentException("Not a valid node for quads")
      }
    }

    val rs = triples.map(quad=>(getNodeValue(quad.getSubject),getNodeValue(quad.getPredicate),getNodeValue(quad.getObject)))
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithIndex

    val vertices: Array[(VertexId,String)] = indexedMap.map(x=>(x._2.toLong,x._1))
    val _iriToId: Array[(String,VertexId)] = indexedMap.map(x=>(x._1,x._2.toLong))

    val tuples = rs.map( t=> ())
    val g1 = Graph(triples.map(t => getNodeValue(t.getSubject) ~> getNodeValue(t.getObject)))
    println(g1.edges.foreach(println))

    new {
      val graph = Graph(triples.map(t=>getNodeValue(t.getSubject) ~> getNodeValue(t.getObject)))
    }

  }*/

}

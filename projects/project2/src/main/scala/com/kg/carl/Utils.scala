package com.kg.carl

import java.net.URL
import org.apache.spark.util._
import scala.collection.SortedSet
import scala.collection.mutable._
import org.apache.spark.SparkContext
import scala.math

object Utils {
  /**
   * Given a node array return the node string.
   *
   * @param nodes - ArrayBuffer with all the nodes.
   * @param id - An identifier of type integer.
   *
   * @return - appropriate node string
   */
  def getNodeForId(
    nodes: ArrayBuffer[String],
    id:    Int): String = {

    return nodes(id)
  }
  /**
   * Return a unique id for a node entity
   *
   * @param nodes	- a string of array buffer as collection accumulator
   * @param idNodes	- map of node name and size as collection accumulator
   * @param node	- a new node as string
   *
   * @return  - unique id as Int
   */
  def getIdForNode(
    nodes:   CollectionAccumulator[ArrayBuffer[String]],
    idNodes: CollectionAccumulator[LinkedHashMap[String, Int]],
    node:    String): Int = {

    if (idNodes.value.size() != 0) {
      if (!(idNodes.value.get(0).contains(node))) {
        idNodes.value.get(0) += node -> nodes.value.get(0).size
        nodes.value.get(0) += node
      }
    } else {
      idNodes.add(LinkedHashMap(node -> 0))
      nodes.add(ArrayBuffer(node))
    }

    return idNodes.value.get(0)(node)
  }
  /**
   * Return a unique id for a node entity
   *
   * @param nodes	- a string of array buffer
   * @param idNodes	- map of node name and size
   * @param node	- a new node as string
   *
   * @return  - unique id as Int
   */
  def getIdForNode(
    nodes:   ListBuffer[String],
    idNodes: LinkedHashMap[String, Int],
    node:    String): Int = {

    if (!(idNodes.contains(node))) {
      idNodes(node) = nodes.size
      nodes += node
    }

    return idNodes(node)
  }
  /**
   * Return a unique id for a node entity
   *
   * @param nodes	- a string of array buffer
   *
   * @return  - Int as size of entities
   */
  def getNumberOfEntities(nodes: ArrayBuffer[String]): Int = {

    return nodes.size
  }
  /**
   * Return a boolean checking if the s,p,o exists in PredicateSubjectObject map
   *
   * @param pso	- PredicateSubjectObject map
   * @param nodes	- list of nodes
   * @param idNodes	- a string of array buffer
   * @param subject - subject string
   * @param predicate - predicate string
   * @param object - object string
   * @return  - true or false
   */
  def contains(
    pso:       LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]],
    nodes:     ListBuffer[String],
    idNodes:   LinkedHashMap[String, Int],
    subject:   String,
    predicate: String,
    `object`:  String): Boolean = {

    return contains(pso, getIdForNode(nodes, idNodes, subject),
      getIdForNode(nodes, idNodes, predicate),
      getIdForNode(nodes, idNodes, `object`))
  }
  /**
   * Return a boolean checking if the s,p,o exists in PredicateSubjectObject map
   *
   * @param pso	- PredicateSubjectObject map
   * @param nodes	- list of nodes
   * @param subject - subject string
   * @param predicate - predicate string
   * @param object - object string
   * @return  - true or false
   */
  def contains(
    pso:       LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]],
    subject:   Int,
    predicate: Int,
    `object`:  Int): Boolean = {

    return (pso(predicate).contains(subject) &&
      pso(predicate)(subject).contains(`object`))
  }
  /**
   * Return a boolean checking if the subject exists in Expected cardinality map
   *
   * @param ecpv - expected cardinality by property value map
   * @param s - subject id
   * @param p - predicate id
   * @return  - true or false
   */
  def hasExpectedCardinality(
    ecpv: LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]],
    s:    Int,
    p:    Int): Boolean = {

    val arrBuf = ecpv.get(p)
    arrBuf.foreach {
      iterator =>
        iterator.foreach {
          map =>
            if (map.contains(s)) {
              return true
            }
        }
    }
    return false
  }
  /**
   * Return a cardinality checking if the subject exists in Expected cardinality map
   *
   * @param ecpv - expected cardinality by property value map
   * @param nodes	- list of nodes
   * @param s - subject id
   * @param p - predicate id
   * @return  - Int
   */
  def getExpectedCardinality(
    ecpv: LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]],
    s:    Int,
    p:    Int): Int = {

    var default = 0
    val arrBuf = ecpv.get(p)
    arrBuf.foreach {
      iterator =>
        iterator.foreach {
          map =>
            if (map.contains(s)) {
              return map.apply(s)
            }
        }
    }
    return default
  }
  /**
   * Return a boolean checking if the subject exists in ArrayBuffer of maps
   *
   * @param arrBuf - ArrayBuffer of map
   * @param subject - subject id
   * @return  - true or false
   */
  def isExisting(
    arrBuf:  ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]],
    subject: Int): Boolean = {

    arrBuf.foreach {
      iterator =>
        if (iterator.contains(subject)) {
          return true
        }
    }
    return false
  }
  /**
   * Return a size checking if the subject exists in ArrayBuffer of maps
   *
   * @param arrBuf	- ArrayBuffer of map
   * @param subject - subject id
   * @return  - Int
   */
  def getSize(
    arrBuf:  ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]],
    subject: Int): Int = {

    arrBuf.foreach {
      iterator =>
        if (iterator.contains(subject)) {
          return iterator.get(subject).size
        }
    }
    return 0
  }
  /**
   * Return a objects checking if the element exists in ArrayBuffer of maps
   *
   * @param arrBuf - ArrayBuffer of map
   * @param subject - id
   * @return - ArrayBuffer of objects
   */
  def getObjects(
    arrBuf: ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]],
    q:      Int): ArrayBuffer[Int] = {
    
    val empty = ArrayBuffer[Int]()
    arrBuf.foreach {
      i =>
        if (i.contains(q)) {
          return i.apply(q)
        }
    }
    return empty
  }
  /**
   * generic function to update a map
   *
   * @param K - Key
   * @return V - Value
   */
  def addOrUpdate[K, V](m: LinkedHashMap[K, V], k: K, kv: (K, V),
                        f: V => V) {

    m.get(k) match {
      case Some(e) => m.update(k, f(e))
      case None    => m += kv
    }
  }
  /**
   * triple parsing function after reading the tsv file
   *
   * @param parsData - data to parse
   * @return Triples - returns a triple case class
   */
  def parseTriples(parsData: String): Triples = {

    val spo = parsData.split("\t")
    val subject = spo(0)
    val predicate = spo(1)
    val `object` = spo(2)

    Triples(subject, predicate, `object`)
  }
  /**
   * cardinality parsing function after reading the tsv file
   *
   * @param parsData - data to parse
   * @return Cardinalities - returns a cardinlalities case class
   */
  def parseCardinalities(parsData: String): Cardinalities = {

    val spo = parsData.split("\t")
    val indexOfDel = spo(0).indexOf('|')
    if (spo(1) == "hasExactCardinality") {
      val subject = spo(0).substring(0, indexOfDel)
      val predicate = spo(0).substring(indexOfDel + 1, spo(0).length())
      val cardinality = spo(2).toInt

      Cardinalities(subject, predicate, cardinality)
    } else {
      Cardinalities("NA", "NA", 0)
    }

  }
  /**
   * The case class for triples
   *
   * @param subject - subject string
   * @param predicate - predicate string
   * @param object - object string
   */
  case class Triples(
    subject:   String,
    predicate: String,
    obj:       String)
  /**
   * The case class for cardinalities
   *
   * @param subject - subject string
   * @param predicate - predicate string
   * @param object - object string
   */
  case class Cardinalities(
    subject:     String,
    predicate:   String,
    cardinality: Int)

}

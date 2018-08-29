package com.kg.carl

import java.net.URL
import scala.collection.SortedSet
import scala.collection.mutable._
import org.apache.spark.SparkContext
import scala.math
import com.kg.carl.Algorithm._

object IdStore {
  /*  trait Store {
    val nodes = ListBuffer[String]()
    val id_for_nodes = LinkedHashMap[String, Int]()
    val properties = Set[Int]()

    val pso = LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]]()
    val expected_cardinalities_by_property_value = LinkedHashMap[Int, LinkedHashMap[Int, Int]]()

    def getNodeForId(id: Int): String = {
      return nodes(id)
    }

    def getIdForNode(node: String): Int = {
      if (!(id_for_nodes.contains(node))) {
        id_for_nodes(node) = nodes.size
        nodes += node
      }
      return id_for_nodes(node)
    }

    def getNumberOfEntities(): Int = {
      return nodes.size
    }

    def getProperties(): Set[Int] = {
      return properties
    }

    def contains(subject: String, predicate: String, `object`: String): Boolean = {
      return contains(getIdForNode(subject), getIdForNode(predicate), getIdForNode(`object`))
    }

    def contains(subject: Int, predicate: Int, `object`: Int): Boolean = {
      return (pso(predicate).contains(subject) && pso(predicate)(subject).contains(`object`))
    }

    def hasExpectedCardinality(s: Int, p: Int): Boolean = {
      return expected_cardinalities_by_property_value(p).contains(s)
    }

    def getExpectedCardinality(s: Int, p: Int): Int = {

      expected_cardinalities_by_property_value.get(p) match {
        case Some(i) => return i.get(s).asInstanceOf[Int]
        case None    => return 0
      }
    }

  }

  def addOrUpdate[K, V](m: collection.mutable.LinkedHashMap[K, V], k: K, kv: (K, V),
                        f: V => V) {
    m.get(k) match {
      case Some(e) => m.update(k, f(e))
      case None    => m += kv
    }
  }
  *
  */
  def parseTriples(parsData: String): Triples = {
    val spo = parsData.split("\t")

    val subject = spo(0)
    val predicate = spo(1)
    val `object` = spo(2)

    Triples(subject, predicate, `object`)
  }

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
  case class Triples(subject: String, predicate: String, obj: String)
  case class Cardinalities(subject: String, predicate: String, cardinality: Int)

}

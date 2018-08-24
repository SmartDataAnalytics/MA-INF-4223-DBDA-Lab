package net.sansa_stack.template.spark.rdf

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession

object Preprocessing {

  var TCBS: RDD[ArrayBuffer[(String, List[String])]] = _

  /**
   * Simple Api to call semanticItemGeneration and commonBehaviourSetGeneration
   * The commonBehaviour is generated for simTH = 60%, 70% and 80%
   * Mining is for rule mining
   */
  def run(spark: SparkSession, triplesRDD: RDD[org.apache.jena.graph.Triple]) = {
    val semanticItem = semanticItemGeneration(triplesRDD)
    println("Rules in the format : (res: { antecedent => consequent } | support | confidence | lift) are as follows:-")
    println(" **************************** simTH = 60% ******************************************* ")
    TCBS = commonBehaviourSetGeneration(semanticItem, 0.60)
    Mining.run(spark, triplesRDD, TCBS, semanticItem)
    println(" **************************** simTH = 70% ******************************************* ")
    TCBS = commonBehaviourSetGeneration(semanticItem, 0.70)
    Mining.run(spark, triplesRDD, TCBS, semanticItem)
    println(" **************************** simTH = 80% ******************************************* ")
    TCBS = commonBehaviourSetGeneration(semanticItem, 0.80)
    Mining.run(spark, triplesRDD, TCBS, semanticItem)
  }

  /**
   * Generates semantic item
   * We are only considering all triples excluding rdf:type and rdfs:subClassOf,
   * then group all the subjects in a list with commmon predicate and objects from the triples.
   * @param triplesRDD contains RDF data in the form of triples
   * @return (predicate  object  List(subject1, subject2,...., subjectn))
   * ex:- (occupation  SongWriter, List(John_Lennon, George_Harrison))
   */
  def semanticItemGeneration(triplesRDD: RDD[org.apache.jena.graph.Triple]): RDD[(String, List[String])] = {
    return triplesRDD
      .distinct()
      .filter(f => !f.getPredicate.getURI.contains("#type"))
      .filter(f => !f.getPredicate.getURI.contains("#subClassOf"))
      .map(f => {
        val key = f.getPredicate + "  " + f.getObject
        val value = f.getSubject.getURI + " "
        (key, value)
      }).reduceByKey(_ + _) // group based on key
      .filter(f => f._2.split(" ").size > 1)
      .map(f => (f._1, f._2.split(" ").toList))
  }

  /**
   * Generates common behaviour items
   * Aggregates an ArrayBuffer of SemanticItems based on the similarity of entities in Element Sets.
   * From the semanticItem generated, we will find intersection and union among the lists of subjects in semanticItem and
   * compute Similarity Degree(SD) i.e. intersection/union.
   * When SD>simTH, we group those semantic items in an ArrayBuffer(returned as an RDD in TCBS)
   *
   * @param semanticItem generated from the semanticItemGeneration api, with simTh - Similarity Threshold for the input semantic Items
   * @return Returns an RDD of ArrayBuffer containing grouped semantic items
   * i.e. ArrayBuffer[(predicate1  object1  List(subject1, subject2)), (predicate2  object2  List(subject1, subject2))]
   * ex:- ArrayBuffer [(occupation  SongWriter, List(John_Lennon, George_Harrison))
   * (instrument  Guitar, List(George_Harrison,John_Lennon))]
   */
  def commonBehaviourSetGeneration(semanticItem: RDD[(String, List[String])], simTH: Double): RDD[ArrayBuffer[(String, List[String])]] = {
    var intersection = List(" ")
    var union = List(" ")
    val item = semanticItem.cartesian(semanticItem) //cartisian product of semantic items
      .map(f => {
        if (f._1._1 > (f._2._1)) {
          (f._2, f._1)
        } else {
          (f._1, f._2)
        }
      })
      .distinct()
      .filter(f => !(f._1._1.equals(f._2._1)))
      .map(f => {
        intersection = f._1._2.intersect(f._2._2)
        union = f._1._2.union(f._2._2).distinct
        (f, intersection, union)
      })

    return item.filter(f => f._2 != null && (f._2.length.toDouble / f._3.length.toDouble > simTH))
      .map(f => {
        var buffer = new ArrayBuffer[(String, List[String])]()
        buffer.append((f._1._1._1, f._1._1._2))
        buffer.append((f._1._2._1, f._1._2._2))
        (buffer)
      })
  }
}

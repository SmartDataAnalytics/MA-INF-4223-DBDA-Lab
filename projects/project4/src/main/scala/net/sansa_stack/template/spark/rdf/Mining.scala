package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object Mining {

  /**
   * Simple Api for function calls
   *
   * @param triplesRDD contains RDF data in the form of triples
   * @param semanticItem generated from the semanticItemGeneration api such as:- (predicate  object  List(subject1, subject2,...., subjectn))
   * @param TCBS has ArrayBuffer contains grouped semantic items based on list of subjects.
   * ex:- ArrayBuffer [(occupation  SongWriter, List(John_Lennon, George_Harrison))
   * (instrument  Guitar, List(George_Harrison,John_Lennon))]
   */
  def run(spark: SparkSession, triplesRDD: RDD[org.apache.jena.graph.Triple], TCBS: RDD[ArrayBuffer[(String, List[String])]], semanticItem: RDD[(String, List[String])]) = {
    val instance_rules = instance_level_ruleForming(TCBS)
    val temp = TCBS.flatMap(f => f).map(f => f._2).flatMap(f => f).distinct().collect().toList
    val type_triplesRDD = triplesRDD.filter(f => f.getPredicate.getURI.contains("#type") && temp.contains(f.getSubject.getURI))
    val rule_typerelation_subject = determine_subject_typesforrule(spark, type_triplesRDD, instance_rules)
    val subClassOf_tripleRDD = triplesRDD.filter(f => f.getPredicate.getURI.contains("#subClassOf"))
    val schema_ruleset = schema_level_ruleforming(spark, rule_typerelation_subject, subClassOf_tripleRDD)
    Evaluation.run(triplesRDD, semanticItem, schema_ruleset, instance_rules, rule_typerelation_subject)
  }
  /**
   * Mines instance level rules.
   * The semanticItem from TCBS ArrayBuffer are split into two halves.
   * The first half with the [predicate object] forms the antecedent
   * The other half with the [predicate object] forms the consequent
   * buffer forms the ArrayBuffer containing the distinct items in union on all list of subjects
   * intersection_subjects contains the distinct items in intersection of all list of subjects
   *
   * @return (ArrayBuffer[subject1,...,subjectn], antecedent, consequent, List[subject1,...,subjectm]), where m<=n
   * ex:- (ArrayBuffer(George_Harrison, John_Lennon, Linkin_Park), instrument  Guitar, occupation  SongWriter, List(George_Harrison, John_Lennon, Linkin_Park))
   */
  def instance_level_ruleForming(TCBS: RDD[ArrayBuffer[(String, List[String])]]): RDD[(ArrayBuffer[String], String, String, List[String])] = {

    return TCBS.map(f => {
      var antc = ""
      var cons = ""
      var intersection_subjects = f(0)._2
      var buffer = new ArrayBuffer[String]()
      for (i <- 0 to (f.length / 2) - 1) {
        antc = antc + f(i)._1
        buffer = buffer.union(f(i)._2)
        intersection_subjects = intersection_subjects.intersect(f(i)._2)
      }
      for (i <- (f.length / 2) to f.length - 1) {
        cons = cons + f(i)._1
        buffer = buffer.union(f(i)._2)
        intersection_subjects = intersection_subjects.intersect(f(i)._2)
      }
      buffer = buffer.distinct
      (buffer, antc, cons, intersection_subjects)
    })
  }

  /**
   * This api filters the subject class hierarchies. ex:- (JohnLennon, List[Person, Owl#Thing])
   * It further uses map partition broadcasting the smaller RDD subject_class_hirerchy, to find intersection among the List[class1, ...classn] for the subjects unioned in instance_rules.
   * ex:- For instance_rule (ArrayBuffer(George_Harrison, John_Lennon, Linkin_Park), instrument  Guitar, occupation  SongWriter, List(George_Harrison, John_Lennon, Linkin_Park))
   * subject_class_hirerchy used:-
   * (George_Harrison,List(MusicalArtist, Artist, Person, owl#Thing))
   * (Linkin_Park,List(owl#Thing, Person))
   * (John_Lennon,List(owl#Thing, Person))
   *  So intersection of lists would be  List[owl#Thing, Person]
   *
   * @param: instance_rules -  instance level rules mined in rule forming api
   *
   * return an RDD in the form (antecedent, consequent, List[class1, class2, ..., classn]) in rule_typerelation_subject
   * ex:- (instrument  Guitar , occupation  SongWriter,List(owl#Thing, Person))
   */
  def determine_subject_typesforrule(spark: SparkSession, type_triplesRDD: RDD[org.apache.jena.graph.Triple], instance_rules: RDD[(ArrayBuffer[String], String, String, List[String])]): RDD[(String, List[String])] = {
    val subject_class_hirerchy = type_triplesRDD.map(f => {
      val key = f.getSubject.getURI()
      val value = f.getObject + " "
      (key, value)
    })
      .reduceByKey(_ + _)
      .map(f => {
        (f._1, f._2.split(" ").toList)
      })
    val broadcastVar = spark.sparkContext.broadcast(subject_class_hirerchy.collect()) // broadcast here small RDD
    val y = instance_rules.mapPartitions({ f =>
      val k = broadcastVar.value
      for {
        x <- f
        z <- k
        if x._1.contains(z._1)
      } yield (z._2, x._2, x._3)

    })
    return y.distinct()
      .map(f => {
        val key = f._2 + " , " + f._3
        val value = f._1
        (key, value)
      }).reduceByKey((a, b) => a.intersect(b))
  }

  /**
   * Mines rule on schema level, checking the rdfs:subClassOf relation among the List of classes in rule_typerelation_subject
   * The rule_typerelation_subject, an RDD of form (instrument  Guitar , occupation  SongWriter,List(owl#Thing, Person))
   * From this list we find the lowest intersection class in the class hierarchy as per rdfs:subClassOf relation, this becomes the resultant classes
   *
   * @return schema_ruleset in the form (String, String, String)
   * ex:- (Person, instrument  Guitar, occupation  SongWriter)
   */
  def schema_level_ruleforming(spark: SparkSession, rule_typerelation_subject: RDD[(String, List[String])], subClass_Of_triple: RDD[org.apache.jena.graph.Triple]): RDD[(String, String, String)] = {
    val broadcastVar = spark.sparkContext.broadcast(rule_typerelation_subject.collect()) // broadcast here small RDD
    val y = subClass_Of_triple.mapPartitions({ f =>
      val k = broadcastVar.value
      for {
        x <- f
        z1 <- k
        if z1._2.contains(x.getSubject.getURI)
      } yield (x, z1._1, z1._2)
    })
    return y.map(f => {
      val key = (f._2, f._3)
      val value = f._1
      (key, value)
    }).reduceByKey((a, b) => if (a.getSubject.getURI != b.getObject.getURI) { a } else b).map(f => {
      (f._2.getSubject.getURI, f._1._1.split(" , ").head, f._1._1.split(" , ").last)
    })
  }
}
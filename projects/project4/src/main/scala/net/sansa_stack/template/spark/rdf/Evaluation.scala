package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import scala.math.BigDecimal

object Evaluation {

  /**
   * Evaluates quality of the association rules generated.
   * Quality measures: support, confidence and lift
   * Calculates average evaluation for each hierarchy (ex:- Artist, Writer are all taken under Person) and so average for those rules is calculated as per Person category.
   * This api does simple function calls. And we are displaying the structure of the rule in specific format with there support,
   * confidence and lift value.
   *
   * @param: tripleRDD containing all RDF triples, semanticItem generated at the Preprocessing step,
   * schema_ruleset containing the mined rules in the format (class, antecedent, consequent),
   * instance_rules with the (union of subjects for antecedent and consequent, antecedent, consequent, intersection among those subjects)
   * rule_typerelation_subject contains the rdf:type data i.e (subject, List[type1 class, type2class , ... typenclass]).
   * ex:- (John_Lennon, List[Artist, Person, Owl#Thing])
   * These params are further passed to all below api's
   * 
   * Result is in the format:- (res: { antecedent => consequent } | support | confidence | lift ) 
   * ex:- (Person: { instrument  Guitar => occupation  SongWriter } | 0.04 | 1.0 | 25.0)
   */
  def run(triplesRDD: RDD[org.apache.jena.graph.Triple], semanticItem: RDD[(String, List[String])],
          schema_ruleset: RDD[(String, String, String)], instance_rules: RDD[(ArrayBuffer[String], String, String, List[String])], rule_typerelation_subject: RDD[(String, List[String])]) = {
    val support = calculate_support(schema_ruleset, triplesRDD, semanticItem)
    val confidence = calculate_confidence(instance_rules, semanticItem, support)
    val lift = calculate_lift(semanticItem, confidence)
    val result_rules = lift.map(f => { ("(" + f._1 + ": { " + f._2 + " => " + f._3 + " } | " + f._4 + " | " + f._5 + " | " + f._6 + ")") })
    result_rules.foreach(println)
    calculate_average_evaluation(lift, triplesRDD, rule_typerelation_subject)
  }

  /**
   * We are calculating support in this api.
   * Rule format :- (res: { antecedent => consequent })
   * Formula for calculating support is: |res intersect antecedent_of_rule| / |Common_class|.
   *
   * @return append the support value to each rule mined in schema_ruleset i.e.(res:{ antecedent => consequent} | support)
   * The support value is mined upto two decimal places
   * ex:- (Person, instrument  Guitar, occupation  SongWriter, 0.04)
   */
  def calculate_support(schema_ruleset: RDD[(String, String, String)], triplesRDD: RDD[org.apache.jena.graph.Triple],
                        semanticItem: RDD[(String, List[String])]): RDD[(String, String, String, Double)] = {

    val support_RDD = schema_ruleset.map(f => {
      (f._2, (f._1, f._3))
    }).join(semanticItem).map(f => (f._2._1._1, (f._1, f._2._1._2, f._2._2)))

    val temp = schema_ruleset.map(f => f._1).distinct.collect().toList
    val type_triples_RDDforsupport = triplesRDD.filter(f1 => {
      f1.getPredicate.getURI.contains("#type") && temp.contains(f1.getObject.getURI)
    }).map(f => (f.getObject.getURI, f + " , ")).reduceByKey(_ + _).map(f => (f._1, f._2.split(" , ").length))

    return support_RDD.join(type_triples_RDDforsupport).map(f => {
      var sup = f._2._1._3.size.toDouble / f._2._2.toDouble
      sup = BigDecimal(sup).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (f._1, f._2._1._1, f._2._1._2, sup)
    })
  }

  /**
   * Here we are calculating the confidence value of the rules.
   * Formula: |res intersect antecedent intersect consequent| / |res intersect antecedent|
   *
   * @return: result in the form (res, antecedent, consequent, support_value, confidence_value)
   * Confidence values are rounded upto two decimal places
   * ex:- (Person, instrument  Guitar, occupation  SongWriter, 0.04, 1.0 )
   */
  def calculate_confidence(
    instance_rules: RDD[(ArrayBuffer[String], String, String, List[String])], semanticItem: RDD[(String, List[String])],
    support: RDD[(String, String, String, Double)]): RDD[(String, String, String, Double, Double)] = {

    return instance_rules.map(f => {
      ((f._2, f._3), (f._1, f._4))
    }).join(support.map(f1 => {
      ((f1._2, f1._3), (f1._1, f1._4))
    })).map(f => { (f._1._1, (f._2._2._1, f._1._2, f._2._1._2, f._2._2._2)) }).join(semanticItem).map(f => {
      var conf = f._2._1._3.length.toDouble / f._2._2.length.toDouble
      conf = BigDecimal(conf).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (f._2._1._1, f._1, f._2._1._2, f._2._1._4, conf)
    })
  }

  /**
   * In this Api we are calculating lift value of the rule.
   * Formula: (|res intersect antecedent intersect consequent| * |res|) /(|res intersect antecedent|^2)
   *
   * All these values are already calculated in our confidence and support. So we just need to follow from the formula.
   * Lift values are rounded upto two decimal places
   * @return: result as (res, antecedent, consequent, support_value, confidence_value, lift_value)
   * ex:- (Person, instrument  Guitar, occupation  SongWriter, 0.04, 1.0, 25.0)
   */
  def calculate_lift(semanticItem: RDD[(String, List[String])], confidence: RDD[(String, String, String, Double, Double)]): RDD[(String, String, String, Double, Double, Double)] = {
    return confidence.map(f => {
      val lift = BigDecimal(f._5 / f._4).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (f._1, f._2, f._3, f._4, f._5, lift)
    })
  }

  /**
   * In this Api we are calculating the average value of support, confidence and lift for all the major classes
   * (in our case Person, Work, Event, Organisation) present in the dataset.
   * minor_classes contains the list of unique res in our mined rules.
   * minor_majorRdd clubs the class hierarchies such as class1 belongs to(subclassOf) class2. ex:- (Artist, Person).
   *
   * @return: result in the form (major_class, avg_support, avg_confidence, avg_lift)
   * ex:- (Person, 0.11, 0.94, 12.08)
   */
  def calculate_average_evaluation(
    lift:       RDD[(String, String, String, Double, Double, Double)],
    triplesRDD: RDD[org.apache.jena.graph.Triple], rule_typerelation_subject: RDD[(String, List[String])]) = {
    val major_classes = triplesRDD.filter(f => { f.getPredicate.getURI.contains("#subClassOf") && f.getObject.getURI.contains("owl#Thing") })
      .map(f => (f.getSubject.getURI)).distinct().collect().toList
    val lift_rddMapped = lift.map(f => ((f._1), (f._2, f._3, f._4, f._5, f._6)))
    val minor_classes = lift.map(f => f._1).distinct.collect().toList
    val minor_majorRdd = rule_typerelation_subject.map(f => { (f._2) }).map(f => {
      (f.intersect(minor_classes).head, f.intersect(major_classes).head)
    }).distinct().join(lift_rddMapped)

    println("******************************* Class | Avg_Support | Avg_Confidence | Average_Lift *******************************")
    minor_majorRdd.map(f => {
      val key = f._2._1
      val value = (f._1, f._2._2._1, f._2._2._2, f._2._2._3, f._2._2._4, f._2._2._5, 1.0)
      (key, value)
    }).reduceByKey((a, b) => { (a._1, a._2, a._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7) })
      .map(f => {
        val avg_sup = BigDecimal(f._2._4 / f._2._7).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val avg_conf = BigDecimal(f._2._5 / f._2._7).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val avg_lift = BigDecimal(f._2._6 / f._2._7).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        (f._1, avg_sup, avg_conf, avg_lift)
      }).foreach(println)
  }
}
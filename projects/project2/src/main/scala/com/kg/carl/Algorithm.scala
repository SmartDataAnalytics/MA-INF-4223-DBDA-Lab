package com.kg.carl
import com.kg.carl.Utils._
import util.control.Breaks._
import scala.collection.mutable._
import com.kg.carl.Preprocessor._

object Algorithm {

  /**
   * the case class for scored rules.
   * all metrics declared will be evaluated/computed with the CARL algorithm
   * the rule can be viewed as p(x,y) ∧ q(y,z) -> r(x,z)
   *
   * @param p - ID for p node entity
   * @param q - ID for q node entity
   * @param r - ID for r node entity
   */
  case class ScoredRule(
    p: Int,
    q: Int,
    r: Int) {
    var support = 0
    var bodySupport = 0
    var headCoverage = 0.0
    var standardConfidence = 0.0
    var pcaConfidence = 0.0
    var completenessConfidence = 0.0
    var precision = 0.0
    var recall = 0.0
    var directionalMetric = 0.0
    var directionalCoef = 0.0
  }
  /**
   * declaring metric standards as mentioned in the paper
   */
  val MIN_HEAD_COVERAGE = 0.001
  val MIN_STANDARD_CONFIDENCE = 0.001
  val MIN_SUPPORT = 10
  val CONFIDENCE_INCOMPLETENESS_FACTOR = 0.5

  /**
   * Mining in KGs have high degree of incompleteness, which may provide
   * inaccurate quality of mined rules, The effort of this algorithm is to
   * expose that incompleteness by introducing aware scoring functions.
   * First the algorithm count number of triples per relations and number of
   * entities, then count the number of missing triples per relation and
   * compute support and other metrics for each possible rule and body.
   * Note: This algorithm is not dependent on any 3rd party library such AMIE.
   * Also, does not use methodology from any other Association Rule Learning.
   * It is primitive and implements the work mentioned in the paper.
   *
   * @param pso	- PredicateSubjectObject map
   * @param nodes	- list of nodes
   * @param properties	- a list of all ids
   * @param ecpv - expected cardinality by property value
   * @param numRules - max number of rules to be scored
   * @return  - list of scored rules
   */
  def CARLAlgorithm(
    pso:   LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]]],
    nodes: ArrayBuffer[String], id_for_nodes: LinkedHashMap[String, Int],
    properties: TreeSet[Int],
    ecpv:       LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]],
    numRules:   Int): ListBuffer[ScoredRule] = {

    val propertyInstances = LinkedHashMap[Int, Int]().withDefaultValue(0)
    val entityCount = getNumberOfEntities(nodes)
    /*
     * iterating and finding triple relations and entities
     */
    pso.foreach {
      pxy =>
        pxy._2.foreach {
          xy =>
            xy.foreach {
              iter =>
                val num = propertyInstances.get(iter._1)
                if (num == None) {
                  propertyInstances(pxy._1) = propertyInstances(pxy._1) + iter._2.size
                }
            }

        }
    }
    val expectedTriplePerRelation = Map[Int, Int]()
    /*
     * iterating and finding missing triples per relation.
     * properties here is the list of predicate ids
     */
    properties.foreach {
      kv =>
        /*
         * for a subject in all entities get the expected cardinality and the
         * actual cardinality and store the number of expected triples per
         * relation.
         */
        for (subject <- 0 to entityCount) {
          val expectedCardinality = getExpectedCardinality(ecpv, subject, kv)
          if (expectedCardinality != 0) {
            val actualCardinality = 0
            /*
             * isExisting and getSize methods iterate over a ArrayBuffer of maps.
             * Note: The reason having ArrayBuffer is to emulate the functionality of
             * MultiMaps. Normal maps cannot store duplicate keys and overwrite them.
             */
            if (isExisting(pso(kv), subject)) {
              val actualCardinality = getSize(pso(kv), subject)
            }
            if (expectedCardinality > actualCardinality) {
              if (expectedTriplePerRelation.contains(kv)) {
                val m = expectedTriplePerRelation.apply(kv) + (expectedCardinality - actualCardinality)
                expectedTriplePerRelation.put(kv, m)
              } else {
                expectedTriplePerRelation.put(kv, (expectedCardinality - actualCardinality))
              }
            }
          }
        }
    }
    /*
     * Will start computing metrics support etc.
     * The idea is to iterate all possible rule and body.
     */
    val rules = ListBuffer[ScoredRule]()
    /*
     * introducing breakable block to have a behaviour similar to break/continue.
     * scala by default does not have the keyword 'continue' but we can emulate the
     * continue functionality with placing breakable just after the loop starts and
     * add break wherever we want. This breaks and finds an exception handling, if it
     * does not find one it continues the loop for next iteration.
     */
    properties.foreach {
      p =>
        properties.foreach {
          q =>
            properties.foreach {
              r =>
                breakable {
                  val rule = new ScoredRule(p, q, r)
                  var pcaSupport = 0.0
                  /*
                   * having facts added by subjects with cardinality
                   */
                  val subjectsWithCardinality = LinkedHashMap[Int, Int]()
                  pso.get(p).foreach {
                    arrBuf =>
                      arrBuf.foreach {
                        map =>
                          val zAdded = ArrayBuffer[Int]()
                          map.foreach {
                            xy =>
                              val x = xy._1
                              xy._2.foreach {
                                y =>
                                  var newZ = getObjects(pso(q), y)
                                  zAdded ++= newZ
                              }
                              /*
                               * find z and if it has expected cardinality update the subjects with cardinality
                               * check if the actual z contains and increment the support. If the new z is added
                               * then add consider it as partial and update pcaSupport(Partial Closed Assumption)
                               */
                              if (!zAdded.isEmpty) {
                                val zActual = getObjects(pso(r), x)
                                val expectCardinality = hasExpectedCardinality(ecpv, x, r)
                                rule.bodySupport += zAdded.size
                                if (!zActual.isEmpty) {
                                  pcaSupport += zAdded.size
                                }
                                zAdded.foreach {
                                  z =>
                                    if (zActual.contains(z)) {
                                      rule.support = rule.support + 1
                                    } else if (expectCardinality) {
                                      if (subjectsWithCardinality.contains(x)) {
                                        val m = subjectsWithCardinality.apply(x) + 1
                                        subjectsWithCardinality.put(x, m)
                                      } else {
                                        subjectsWithCardinality.put(x, 1)
                                      }

                                    }
                                }

                              }
                          }
                      }
                  }
                  // continue if the support is less than the min support
                  if (rule.support < MIN_SUPPORT) {
                    break
                  }
                  // check for the headCoverage
                  rule.headCoverage = rule.support.asInstanceOf[Double] / propertyInstances(rule.r)
                  if (rule.headCoverage < MIN_HEAD_COVERAGE) {
                    break
                  }
                  // check for standard confidence
                  rule.standardConfidence = rule.support.asInstanceOf[Double] / rule.bodySupport
                  if (rule.standardConfidence < MIN_STANDARD_CONFIDENCE) {
                    break
                  }
                  rule.pcaConfidence = rule.support.asInstanceOf[Double] / pcaSupport
                  var tripleAddedToMissingPlaces = 0
                  var tripleAddedToCompletePlaces = 0
                  // update triples that were added to missing places and complete places
                  subjectsWithCardinality.foreach {
                    t =>
                      val expected_cardinality = getExpectedCardinality(ecpv, t._1, rule.r)
                      if (expected_cardinality.asInstanceOf[Int] != 0) {
                        val actualTriplesCount = getSize(pso(rule.r), t._1)
                        var missing_triples = 0
                        if (expected_cardinality > actualTriplesCount) {
                          missing_triples = expected_cardinality - actualTriplesCount
                        }
                        val triplesAddedByRule = t._2
                        if (triplesAddedByRule > missing_triples) {
                          tripleAddedToMissingPlaces += missing_triples
                          tripleAddedToCompletePlaces += triplesAddedByRule - missing_triples
                        } else {
                          tripleAddedToMissingPlaces += triplesAddedByRule
                        }
                      } else {
                        println("No cardinality exists but still stored the facts")
                      }
                  }
                  /*
                   * computing all metrics with the mentioned equations in the paper.
                   */
                  rule.completenessConfidence = rule.support / (rule.bodySupport - tripleAddedToMissingPlaces).asInstanceOf[Double]
                  rule.precision = 1 - tripleAddedToCompletePlaces.asInstanceOf[Double] / rule.bodySupport
                  if (expectedTriplePerRelation.contains(rule.r)) {
                    rule.recall = tripleAddedToMissingPlaces.asInstanceOf[Double] / expectedTriplePerRelation(rule.r)
                  } else {
                    rule.recall = Double.NaN
                  }
                  if ((tripleAddedToCompletePlaces + tripleAddedToMissingPlaces) != 0) {
                    rule.directionalMetric =
                      (tripleAddedToMissingPlaces - tripleAddedToCompletePlaces).asInstanceOf[Double] /
                        (2 * (tripleAddedToMissingPlaces + tripleAddedToCompletePlaces)) + 0.5
                  } else {
                    rule.directionalMetric = Double.NaN
                  }
                  /*
                   * As evaluation we compute the number of conplete and incomplete for given number of possible relations
                   */
                  val numPossibleRelations = entityCount * entityCount
                  var expectedIncomplete = 0
                  var expectedComplete = 0
                  if (expectedTriplePerRelation.contains(rule.r)) {
                    expectedIncomplete = expectedTriplePerRelation(rule.r) / numPossibleRelations
                    val tmp = (numPossibleRelations - expectedTriplePerRelation(rule.r) - propertyInstances(rule.r))
                    expectedComplete = tmp / numPossibleRelations
                  }
                  val complete = tripleAddedToCompletePlaces.asInstanceOf[Double] / rule.bodySupport
                  val incomplete = tripleAddedToMissingPlaces.asInstanceOf[Double] / rule.bodySupport
                  if (complete == 0 || expectedIncomplete == 0) {
                    rule.directionalCoef = Float.MaxValue
                  } else {
                    rule.directionalCoef = 0.5 * expectedComplete / complete + 0.5 * incomplete / expectedIncomplete
                  }
                  rules += rule
                }
            }
        }
    }
    // sort with completeness confidence
    rules.sortWith { (a: ScoredRule, b: ScoredRule) =>
      a.completenessConfidence > b.completenessConfidence
    }
    // copy the num rules mentioned or rules size: which ever is smallest
    val limit = scala.math.min(numRules, rules.size)
    val result = ListBuffer[ScoredRule]()
    for (i <- 0 until limit) {
      result += rules(i)
    }
    return result
  }

}

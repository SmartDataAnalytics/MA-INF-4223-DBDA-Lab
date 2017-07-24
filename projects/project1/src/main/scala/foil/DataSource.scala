package foil

import scala.io.Source
import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

/**
 * Getting a map of predicate name and tuples
 */
class DataSource (val clauses: java.util.List[Literal]){

  val tuples = getConstants.toArray
  val literals = getLiterals
  val predicateMap = getPredicateMap
  val tupleMap = getTupleMap
  val tupplesSize = getTuplesSize // p/n
  val numTuples = tuples.size
  val numRelations = literals.size

  private def getPredicateMap = {
    val predicateMap = Map.empty[String, Predicate]
    for (clause <- this.clauses){
      if (!predicateMap.contains(clause.predicate)) {
      	Predicate.addToList(predicateMap, clause.predicate, clause.tuples)
      }
    }
    predicateMap
  }
  
  private def getTupleMap = {
    val tupleMap = Map.empty[String, List[List[String]]]
    for (clause <- this.clauses){
      if (tupleMap.contains(clause.predicate)){
        tupleMap += clause.predicate -> (clause.tuples :: tupleMap(clause.predicate))
      }else{
        tupleMap += clause.predicate -> List(clause.tuples)
      }
    }
    tupleMap
  }

  private def getLiterals = {
    var set = Set.empty[String]
    for (clause <- this.clauses){
      set += clause.predicate
    }
    set
  }

  private def getTuplesSize = {
    this.clauses.head.tuples.length
  }
  
  private def getConstants = {
    var set = Set.empty[String]
    for (clause <- this.clauses if !clause.tuples.isEmpty){
      clause.tuples.foreach(s => set += s)
    }
    set
  }

}

object DataSource {

  def apply(path: String): DataSource = {
    val l = new util.ArrayList[Literal]()
    val res = Source.fromURL(getClass.getResource(path))
    for (line <- res.getLines() if !line.isEmpty) l add Literal.parse(line)
    new DataSource(l)
  }
}
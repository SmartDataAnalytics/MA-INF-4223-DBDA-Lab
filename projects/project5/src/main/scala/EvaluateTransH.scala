import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql._
import net.sansa_stack.rdf.spark.kge.triples._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.math._

import scala.util.control.Breaks._
import net.sansa_stack.rdf.spark.kge.triples.IntegerTriples
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.immutable.ListMap

/**
 * Evaluation the mean Ranks and hit@10 of already learned TransH model.
 */
object EvaluateTransH {

  val spark = SparkSession.builder.master("local")
    .appName("kgepredict").getOrCreate()

  def main(args: Array[String]): Unit = {
    
    /**
     * Read the data and also the already learned embeddings of entities, relations and normal vectors w
     */
    val entityIds = spark.sparkContext.textFile("FB15k/entity2id.txt").map( i => {
      var splitRow = i.split("\t")
      (splitRow(0), splitRow(1).toInt)
    }).collectAsMap()
    
    val relationIds = spark.sparkContext.textFile("FB15k/relation2id.txt").map( i => {
      var splitRow = i.split("\t")
      (splitRow(0), splitRow(1).toInt)
    }).collectAsMap()
    
    val e = spark.sparkContext.textFile("FB15k/trained/with001/e.txt").map( i => {
      var splitRow = i.split("\t").map(_.toFloat)
      splitRow
    }).collect()
    val r = spark.sparkContext.textFile("FB15k/trained/with001/r.txt").map( i => {
      var splitRow = i.split("\t").map(_.toFloat)
      splitRow
    }).collect()
    val wR = spark.sparkContext.textFile("FB15k/trained/with001/wR.txt").map( i => {
      var splitRow = i.split("\t").map(_.toFloat)
      splitRow
    }).collect()
    
    /**
     * Read the testing data into an RDD and zip with index
     */
    val testData = new Triples("FB15k/minitest500.txt", "\t", false, false, spark)

    import spark.implicits._
    
    val test = testData.triples.map ( i =>
        IntegerTriples(entityIds(i.Subject),relationIds(i.Object),entityIds(i.Predicate))
    )
    
    val testI = test.rdd.zipWithIndex().persist()
    
    val nE = entityIds.size
    val nR = relationIds.size
    val testSize = testI.count().toFloat
    
    /**
     * Broadcast the embeddings of entities, relations and normal vectors w
     */
    val bcE = spark.sparkContext.broadcast(e)
    val bcR = spark.sparkContext.broadcast(r)
    val bcWr = spark.sparkContext.broadcast(wR)
    
    println("Initialize map partitions")
    
    /**
     * We used map partitions because we found it better suited for this case and also it's faster than map function.
     * For each partition of the RDD containing the testing data we run our predict function:
     * 1) For each triplet replace the head and tail (one at a time) by each entity in the knowledge graph
     * 2) Calculate the dissimilarity score for each of those corrupted tuples and store them in a Map
     * 3) Sort those Maps in ascending order
     * 4) Calculate the rank (the index of the true testing tuple in those sorted Maps) and hit@10 = True if the rank is <= 10
     */
    val partitionsResRDD = testI.mapPartitions{
      iterator =>

        var retMap = ArrayBuffer[(Long, Int, Int, Int, Int)]()
        var meanRankHit10Map = Map[Long, (Int, Int)]()
        val indxArr = 0 until nE
        
        while(iterator.hasNext){
            val sampleTriple = iterator.next()
            println("Sample triple ID: " + sampleTriple._2)
            var tmpDistanceMap = Map[Long, Double]()
            var tmpDistanceCTailMap = Map[Long, Double]()
            indxArr.foreach(i => {
              tmpDistanceMap(i) = dist(IntegerTriples(i, sampleTriple._1.Predicate, sampleTriple._1.Object), bcE.value, bcR.value, bcWr.value)
              tmpDistanceCTailMap(i) = dist(IntegerTriples(sampleTriple._1.Subject, sampleTriple._1.Predicate, i), bcE.value, bcR.value, bcWr.value)
            })
            
            //Sort the maps containing distances in ascending order
            val sortedMap = ListMap(tmpDistanceMap.toSeq.sortWith(_._2 < _._2):_*)
            val sortedTailMap = ListMap(tmpDistanceCTailMap.toSeq.sortWith(_._2 < _._2):_*)
            
            //Find the index/rank and hit@10 of the true testing tuples on the sorted maps
            //Once for the head corruption and once for tail corruption
            var isInTop10 = 0
            var rankHeadC = 0
            var cntC = 0
            breakable { 
              for ((k,v) <- sortedMap){
                cntC += 1
                if(k == sampleTriple._1.Subject){
                  rankHeadC = cntC
                  if(rankHeadC <= 10 && rankHeadC > 1){
                    isInTop10 = 1
                  }
                  break
                }
              }
            }

            var isInTop10TailC = 0
            var rankTailC = 0
            var cntTailC = 0
            breakable { 
              for ((k,v) <- sortedTailMap){
                cntTailC += 1
                if(k == sampleTriple._1.Object){
                  rankTailC = cntTailC
                  if(rankTailC <= 10 && rankTailC > 1){
                    isInTop10TailC = 1
                  }
                  break
                }
              }
            }
            
            retMap += ((sampleTriple._2, rankHeadC, isInTop10, rankTailC, isInTop10TailC))
        }
      
      retMap.iterator
    }
    
    //Persist so we don't have to run the mapPartitions again on the following action functions
    partitionsResRDD.persist()
    
    //Get the sum of the ranks when the head of the tuple was corrupted
    val headCorruptRank = partitionsResRDD.map(i => i._2)
    val headCorruptRankSum = headCorruptRank.reduce(_+_)
    
    //Get the sum of the hit@10 when the head of the tuple was corrupted
    val headCorruptTop10 = partitionsResRDD.map(i => i._3)
    val headCorruptTop10Sum = headCorruptTop10.reduce(_+_)

    //Get the sum of the ranks when the tail of the tuple was corrupted
    val tailCorruptRank = partitionsResRDD.map(i => i._4)
    val tailCorruptRankSum = tailCorruptRank.reduce(_+_)
    
    //Get the sum of the hit@10 when the tail of the tuple was corrupted
    val tailCorruptTop10 = partitionsResRDD.map(i => i._5)
    val tailCorruptTop10Sum = tailCorruptTop10.reduce(_+_)
    
    println("Mean rank when corrupting the h entity: " + headCorruptRankSum.toFloat/testSize)
    println("Mean rank when corrupting the t entity: " + tailCorruptRankSum.toFloat/testSize)
    println("Hit@10 when corrupting the h entity: " + headCorruptTop10Sum.toFloat/testSize)
    println("Hit@10 when corrupting the t entity: " + tailCorruptTop10Sum.toFloat/testSize)

  }

  /**
   * Calculates the distance of a tuple based on TransH distance formula
   */
  def dist(tuple: IntegerTriples, e: Array[Array[Float]], r: Array[Array[Float]], wR: Array[Array[Float]]) : Double = {
    var tmp1 = 0f
    var tmp2 = 0f
    for (i <- 0 until 20) {
      tmp1 += wR(tuple.Predicate)(i) * e(tuple.Subject)(i)
      tmp2 += wR(tuple.Predicate)(i) * e(tuple.Object)(i)
    }
    
    var res = 0f
    for (i <- 0 until 20) {
      res += Math.abs(e(tuple.Object)(i) - tmp2*wR(tuple.Predicate)(i) - (e(tuple.Subject)(i) - tmp1*wR(tuple.Predicate)(i)) - r(tuple.Predicate)(i))
    }    
    res
  }

}
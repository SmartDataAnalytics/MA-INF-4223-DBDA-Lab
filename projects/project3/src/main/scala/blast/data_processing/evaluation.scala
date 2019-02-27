package blast.data_processing

import java.util
import java.util.HashSet
import scala.util.matching.Regex
import DataStructures.{EntityProfile, IdDuplicates}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
//this import is necessary to work with java.util objects (like hashset[idduplicate] here)
import collection.JavaConversions._


object evaluation {

  def parse_index(url: String): Int = {
    val pattern = "DS[12]([0-9]*):.*".r
    val pattern(indx) = url
    return indx.toInt
  }

  def evaluate(candidates: RDD[Tuple2[String, String]], groundtruth: String): Tuple2[Double, Double] = {

    //load groundtruth file
    var groundtruth_hash: util.HashSet[IdDuplicates] = read_GroundTruth.read_groundData(groundtruth)

    //for each candidate pair
    val candidates_pairs = candidates.collect()
    val num_candidates = candidates_pairs.size
    val all_duplicates = groundtruth_hash.size()
    var hits = 0.0

    for (pair <- candidates_pairs) {
      val candDup = new IdDuplicates(parse_index(pair._1), parse_index(pair._2))
      if (groundtruth_hash.contains(candDup)) {
        hits += 1
      }
    }

    // the ratio of detected duplicates to all existing duplicate |D| / |groundtruth|
    var recall: Double = (hits / all_duplicates).asInstanceOf[Double]
    //the ratio of detected dulicates to all executed comparisons. |D| / |B|
    var precision: Double = (hits / num_candidates).asInstanceOf[Double]
    println("Evaluation results ######################################")
    println("all duplicates in groundtruth : " + all_duplicates.asInstanceOf[Int])
    println("all comparisons made due to metablocking : " + num_candidates.asInstanceOf[Int])
    println("#of duplicates detected : " + hits.asInstanceOf[Int])
    val result: Tuple2[Double, Double] = Tuple2(recall, precision)
    return result


  }
}



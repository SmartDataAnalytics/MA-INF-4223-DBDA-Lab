package blast.main

import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hdfs.DFSClient
import DataStructures.{Attribute, DatasetReader, EntityProfile, IdDuplicates}
import blast.AttributeSchema.AttributeProfile
import blast.AttributeSchema.AttributeMatchInduction
import blast.Blocking.Blocker
import blast.Blocking.MetaBlocker
import blast.data_processing.read_GroundTruth
import blast.data_processing.evaluation
import scala.collection.JavaConverters._


/*
* Main Blast Class. Implements different functions.
* execute : executes blast with datasets
* convert : converts datasets in the format from http://sourceforge.net/projects/erframework/files/CleanCleanERDatasets/
*            to spark object files.
* */

object Main {

  def convertFile(spark: SparkSession, inputP: String, outputP: String): Unit = {
    val ds: RDD[EntityProfile] = spark.sparkContext.parallelize(DatasetReader.readDataset(inputP))
    ds.saveAsObjectFile(outputP)
  }

  def main(args: Array[String]) {
    args.foreach(println)
    println(args.size)


    if (args.size < 1) {
      println("must provide command:" +
        "\n convert input_file output_path" +
        "\n execute dataset1_path dataset2_path groundtruth(optional)")
      return
    } else {

      if (args(0) == "convert") {
        if (args.size < 3) {
          println("not enough arguments: convert origin_path destination_path")
        } else {
          val spark = SparkSession.builder
            .appName(s"Blast")
            .master("local[*]")
            .getOrCreate()
          convertFile(spark, args(1), args(2))
        }
      } else {
        if (args(0) == "execute") {
          if (args.size < 3) {
            println("not enough arguments: execute ds1_path ds2_path")
          } else {
            if (args.size == 3) {
              run_blast(args(1), args(2), None)
            } else {
              //provided groundtruth file
              run_blast(args(1), args(2), Some(args(3)))
            }
          }
        }
      }
    }
  }

  def run_blast(ds1path: String, ds2path: String, groundtruth: Option[String]) {

    //initializing spark
    val spark = SparkSession.builder
      .appName(s"Blast")
      .master("local[*]")
      .getOrCreate()

    val dataS1: RDD[EntityProfile] = spark.sparkContext.objectFile(ds1path)
    val dataS2: RDD[EntityProfile] = spark.sparkContext.objectFile(ds2path)


    //calculate information regarding attributes (entropies and tokens )
    val AProfileDS1 = new AttributeProfile(dataS1)
    val AProfileDS2 = new AttributeProfile(dataS2)


    println("DS1 size:", AProfileDS1._size, "\tDS2 size:", AProfileDS2._size)
    println("data loaded")


    //compute attribute clusters
    val a = new AttributeMatchInduction()
    val clusters = a.calculate(AProfileDS1, AProfileDS2)
    println("clusters are:")
    clusters.foreach(println)

    //blocking step
    val blocker = new Blocker()
    val (blocks: RDD[Tuple2[Tuple2[String, Int], List[String]]], profileIndex: RDD[(String, Set[(String, Int)])]) = blocker.block(AProfileDS1, AProfileDS2, clusters)
    println("#blocks :")
    println(blocks.count)

    //use attribute match induction's class to calculate the clusters entropies
    val clusterEntropies: Map[Int, Double] = a.entropies_from_clusters(AProfileDS1, AProfileDS2, clusters)
    //metablocking
    val mBlocker = new MetaBlocker(spark, clusterEntropies)
    //candidate edges in the graph that were not pruned
    val candidate_pairs = mBlocker.calculate(blocks, profileIndex, AProfileDS1, AProfileDS2)

    val groundtruthPath : String = groundtruth.getOrElse("")
    if(groundtruthPath != ""){
      //evaluation stage
      //candidate_pairs.saveAsObjectFile(intermediate)

      val (recall, precision) = evaluation.evaluate(candidate_pairs,groundtruthPath)

      //println("recall=",recal_precission._1,"\tprecision=",recal_precission._2)
      //rounding to 2 decimal point (percentage)
      println("Recall: " + (BigDecimal(recall).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble) * 100 + "%")
      println("Precision: " + (BigDecimal(precision).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble) * 100 + "%")
    }





  }
}




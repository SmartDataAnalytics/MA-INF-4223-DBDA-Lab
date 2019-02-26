package com.test.disdupProject
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.xml._
import shapeless.ops.tuple.Length


object MainObject {

  val pathOfDataFile: String = "C:\\Spark\\CommaSeperatedData.txt"
  val appName: String = "Disdup"
  val outputDirectory: String = "C:\\Spark\\Output"

  def main(args: Array[String]) =
    {
      try {

        var startTime = System.currentTimeMillis();
        NaiveDistinct() //Naive-deduplication.
        var endTime = System.currentTimeMillis();
        var processingTimeN = endTime - startTime

        startTime = System.currentTimeMillis();
        BlockBased() //Blocking based on attribute.
        endTime = System.currentTimeMillis();
        var processingTimeB = endTime - startTime

        startTime = System.currentTimeMillis();
        partitionEqually() //equal partitioning.
        endTime = System.currentTimeMillis();
        var processingTimeC = endTime - startTime

        println("Time taken Naive: " + processingTimeN)
        println("Time taken Custom Paritioning: " + processingTimeC)
        println("Time taken Blocking: " + processingTimeB)

      } catch {
        case e: Exception =>
          println(e)

          e.printStackTrace()
      }
    }

  def NaiveDistinct() {
    try {
      val spark = SparkSession.builder.appName("Disdup").master("local[*]").getOrCreate()
      val sc = spark.sparkContext

      val textFile = sc.textFile(pathOfDataFile, 2)

      //Naive functionality
      val testing = textFile.flatMap(record =>
        {
          record.split("\n")
        }).distinct()
      testing.saveAsTextFile(outputDirectory + "_Naive")

    } catch {
      case e: Exception =>
        println(e)
        e.printStackTrace()
    }
  }

  def partitionEqually() {
    try {
      val spark = SparkSession.builder.appName("appName").master("local[*]").getOrCreate()
      val sc = spark.sparkContext

      val textFile = sc.textFile(pathOfDataFile)

      val wordCounts = getCounts(textFile)

      //get total number of partitions.
      val totalPartitions = sc.getExecutorMemoryStatus.size
      val totalCount = textFile.count()
      val partitionsForBlocks = getPartitionForBlock(totalCount, wordCounts, totalPartitions)

      val customPartitioner = new CustomPartitioner(totalPartitions, partitionsForBlocks)

      val deduplicatedData = textFile.flatMap(lines => lines.split("\n"))
        .map(word => (word, None))
        .partitionBy(customPartitioner)
        .reduceByKey((a, b) => a)

      deduplicatedData.saveAsTextFile(outputDirectory + "_Custom")

    } catch {
      case e: Exception =>
        println(e)
        e.printStackTrace()
    }
  }

  def BlockBased() {
    try {
      val spark = SparkSession.builder.appName("BlockBased").master("local[*]").getOrCreate()
      val sc = spark.sparkContext
      val textFile = sc.textFile(pathOfDataFile)

      val testing = textFile.flatMap(line =>
        {
          val records = line.split("\n")
          val values = new Array[SingleRecord](records.length);
          var i = 0;
          for (record <- records) {
            values(i) = new SingleRecord(record) with Serializable
            i += 1
          }
          values
        })
        .distinct()

      testing.saveAsTextFile(outputDirectory + "_Blocking")

    } catch {
      case e: Exception =>
        println(e)
        e.printStackTrace()
    }
  }

  def getCounts(textFile: RDD[String]): RDD[(String, Int)] =
    {
      val counts = textFile.flatMap(line => line.split("\n"))
        .map(recordKey =>
          {
            (recordKey.takeWhile(_ != ','), 1)
          })
        .reduceByKey(_ + _).sortBy(_._2)

      return counts
    }

  def getPartitionForBlock(totalCount: Long, counts: RDD[(String, Int)], totalPartitions: Int): HashMap[String, ListBuffer[BlockPartitionInfo]] =
    {
      val partitionDecider = new PartitionDecider(totalCount, counts, totalPartitions)
      partitionDecider.decidePartitions()
      partitionDecider.partitionsForBlocks
    }
}

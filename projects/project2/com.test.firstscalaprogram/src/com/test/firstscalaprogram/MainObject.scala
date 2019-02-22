package com.test.disdupProject
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.xml._
import shapeless.ops.tuple.Length


object MainObject {
  
  val pathOfDataFile : String = "E:\\tmp\\Spark_Workspace\\CommaSeperatedData.txt"
  val appName : String = "Disdup"
  val outputDirectory : String = "E:\\tmp\\Spark_Workspace\\output"
  
  
 def main(args:Array[String]) =
 { 
   try
   {  
     NaiveDistinct()
     //BlockBased()
     //partitionEqually()
   }
   catch
   {
     case 
     e : Exception => println(e)
     e.printStackTrace()
   }
 }
 
 
 def NaiveDistinct()
 {
   try
   {
    val conf:SparkConf = new SparkConf().setAppName("Disdup").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    
    val startTime = System.currentTimeMillis();
    val textFile = sc.textFile("E:\\tmp\\Spark_Workspace\\CommaSeperatedData.txt", 2)
    
    ////////////////////Naive Distinct Function////////////////
    
    val testing = textFile.flatMap(record => 
      {
        record.split("\n")  
      }
      ).distinct()
    testing.saveAsTextFile("E:\\tmp\\Spark_Workspace\\output")
    
    ////////////////////////////////////////////////////////
    
    val endTime = System.currentTimeMillis();
    val processingTime = endTime - startTime
    println("Time taken: "+processingTime)
   
   }
   catch
   {
     case 
     e : Exception => println(e)
     e.printStackTrace()
   }
 }
 
 
 def partitionEqually()
 {
   try
   {
    val conf:SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    
    val startTime = System.currentTimeMillis();
    val textFile = sc.textFile(pathOfDataFile)
    
    val wordCounts = getCounts(textFile)
    
    val totalPartitions = 2
    val totalCount = textFile.count()
    val partitionsForBlocks = getPartitionForBlock(totalCount, wordCounts, totalPartitions)
    
    val customPartitioner = new CustomPartitioner(totalPartitions, partitionsForBlocks)
    
    val deduplicatedData = textFile.flatMap(lines => lines.split("\n"))
                 .map(word => (word, None))
                 .partitionBy(customPartitioner)
                 .reduceByKey((a, b) => a)
    
    deduplicatedData.saveAsTextFile(outputDirectory)
   
    val endTime = System.currentTimeMillis();
    val processingTime = endTime - startTime
    println("Time taken: "+processingTime)
   }
   catch
   {
     case 
     e : Exception => println(e)
     e.printStackTrace()
   }
 }
 
 def partitionEquallyAndBlocking()
 {
   try
   {
    val conf:SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    
    val startTime = System.currentTimeMillis();
    val textFile = sc.textFile(pathOfDataFile)
    
    val wordCounts = getCounts(textFile)
    
    val totalPartitions = 2
    val totalCount = textFile.count()
    val partitionsForBlocks = getPartitionForBlock(totalCount, wordCounts, totalPartitions)
    
    val customPartitioner = new CustomPartitioner(totalPartitions, partitionsForBlocks)
    
    val deduplicatedData = textFile.flatMap(records => 
      {
        records.split("\n")
        
      }).map(word => (word, None))
        .partitionBy(customPartitioner)
        .reduceByKey((a, b) => a)
    
    deduplicatedData.saveAsTextFile(outputDirectory)
   
    val endTime = System.currentTimeMillis();
    val processingTime = endTime - startTime
    println("Time taken: "+processingTime)
   }
   catch
   {
     case 
     e : Exception => println(e)
     e.printStackTrace()
   }
 }

  def BlockBased()
 {
   try
   {
    val conf:SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    
    val startTime = System.currentTimeMillis();
    val textFile = sc.textFile(pathOfDataFile)
    
    
   val testing = textFile.flatMap(line => 
     {
       val records = line.split("\n")
       val values = new Array[SingleRecord](records.length);
       var i = 0;
       for(record <- records)
       {
         values(i) = new SingleRecord(record) with Serializable
         i+=1
       }
       values
     }
    )
    .distinct()
    
   testing.saveAsTextFile(outputDirectory)
    
    val endTime = System.currentTimeMillis();
    val processingTime = endTime - startTime
    println("Time taken: "+processingTime)
   
   }
   catch
   {
     case 
     e : Exception => println(e)
     e.printStackTrace()
   }
}

def getCounts(textFile : RDD[String]) : RDD[(String, Int)] =
{
       val counts = textFile.flatMap(line => line.split("\n"))
                 .map(recordKey => 
                   {
                     (recordKey.takeWhile(_ != ','), 1)
                   }
                   )
                 .reduceByKey(_ + _).sortBy(_._2)
       
       return counts
 }
 
 def getPartitionForBlock(totalCount : Long, counts : RDD[(String, Int)], totalPartitions : Int) : HashMap[String, ListBuffer[BlockPartitionInfo]] =
 {
   val partitionDecider = new PartitionDecider(totalCount, counts, totalPartitions)
   partitionDecider.decidePartitions()
   partitionDecider.partitionsForBlocks
 }
}
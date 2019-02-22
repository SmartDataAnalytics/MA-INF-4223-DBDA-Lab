package com.test.disdupProject
import org.apache.spark._
import scala.collection.mutable.HashMap
import java.io.FileWriter
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer


class CustomPartitioner(totalPartitions: Int, blocksPartitionInfo : HashMap[String, ListBuffer[BlockPartitionInfo]]) extends Partitioner {
 
 override def numPartitions: Int = totalPartitions
 
 //Override the existing computation 
 override def getPartition(key: Any): Int =
 {

   val partition = blocksPartitionInfo.get(key.toString.takeWhile(_ != ',')).get
   def blockPartitionInfo : BlockPartitionInfo = partition(0)
   val partitionId = blockPartitionInfo.partitionId.toInt
   val remainingSpace = blockPartitionInfo.onItemAdded() 
   if(remainingSpace < 1)
   {
     partition.remove(0, 1)
   }
   
   return partitionId
 }
}

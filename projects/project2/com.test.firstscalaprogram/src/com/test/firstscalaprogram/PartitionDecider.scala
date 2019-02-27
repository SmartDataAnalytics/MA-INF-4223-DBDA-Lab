package com.test.disdupProject
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer


@SerialVersionUID(100L)
class PartitionDecider(totalItems : Double, blocksSortedByCounts: RDD[(String, Int)], totalPartitions : Int)  extends Serializable {
  var partitionsForBlocks = new HashMap[String, ListBuffer[BlockPartitionInfo]]  //BlockId and partitioning info
  var spaceLeftInPartition = new HashMap[String, Double]   // Id of partition and remaining space in that partition
  var noOfItems = totalItems
  var noOfPartitions = totalPartitions
  
  def decidePartitions()
  {
    val partitionSpaceLimit = Math.ceil(noOfItems / noOfPartitions)
 
    for(i <- 0 to noOfPartitions-1)
    {
      spaceLeftInPartition += (i.toString -> partitionSpaceLimit)
    }
    
    var i = -1;
    var spaceLeft = 0.toDouble
    for(block <- blocksSortedByCounts.collectAsMap)
    {
        //Since the blocks are sorted by items count. Firstly all if statements would be true and then all false
        if(block._2 <= partitionSpaceLimit)
        { 
          while(spaceLeft < 1)  //Until we reach the next block with some space left in it
          {
            i += 1
            i %= noOfPartitions
     
            spaceLeft = spaceLeftInPartition.get(i.toString).get
         }
          
          partitionsForBlocks += (block._1 -> ListBuffer(new BlockPartitionInfo(i.toString, block._2)))  
          
          val remainingSpace = spaceLeft - block._2
          spaceLeftInPartition.update(i.toString, remainingSpace)
          spaceLeft = remainingSpace
        }
        else
        {
          partitionsForBlocks += (block._1 -> new ListBuffer())
          
          var blockSizeUnassigned = block._2
          var lastAssignedPartition = i.toString
          
          do
          {
            var spaceLeft = 0.toDouble
            var tries = 0 // if tries gets equal to number of partitions. It means that all the partitions are full now so remaining of this block can be assigned fully to a partition.
            val lastAssignedPartition = i
            while(spaceLeft < 1)  //Until we reach the next block with some capacity in it or the tries complete
            {
              i += 1
            	i %= noOfPartitions
            	spaceLeft = spaceLeftInPartition.get(i.toString).get
              tries += 1
              if(tries == noOfPartitions)
              {
                i = lastAssignedPartition
                spaceLeft = blockSizeUnassigned
              }
            }
            
            var blockPartitionInfoList = partitionsForBlocks.get(block._1).get
            blockPartitionInfoList += new BlockPartitionInfo(i.toString, spaceLeft.toLong)
            partitionsForBlocks.update(block._1, blockPartitionInfoList)
            blockSizeUnassigned -= spaceLeft.toInt //Safe to cast to Int64 because else would only we executed when block size is greater than the size remaining in partition
            
          }while(blockSizeUnassigned > 0)
        }
    }
  }
  
  /*class CustomRDD(sta : RDD[String]) extends RDD[String](sta)
{
  override def distinct ()
  {
    //return new RRD[String]()
  }
}*/
}
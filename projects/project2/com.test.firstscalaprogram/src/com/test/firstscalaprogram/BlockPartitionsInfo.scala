package com.test.disdupProject

@SerialVersionUID(100L)
class BlockPartitionInfo(partitionID : String, totalItems : Long) extends Serializable {
  var partitionId = partitionID
  var itemsSpaceLeft = totalItems   //Space left for this block in current partition
  
  def onItemAdded() : Long =
  {
    itemsSpaceLeft -= 1
    return itemsSpaceLeft;
  }
}
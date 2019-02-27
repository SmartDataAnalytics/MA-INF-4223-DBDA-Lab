package com.test.disdupProject

import jodd.util.HashCode

//This class overrides hashCode and Equals methods which are being used for blocking
@SerialVersionUID(100L)
class SingleRecord(data : String) extends Serializable {
  
  //HashCode based on the key attribute(s) only
  override def hashCode = 
  { 
    if(!data.equals(""))
    {
      data.takeWhile(_ != ',').hashCode()
    }
    else
      "".hashCode
  }
  
  //Match the whole record
  override def equals(singleRecord: Any): Boolean = {
    singleRecord match {
      case p: SingleRecord => data.equals(singleRecord.toString())
      case _ => false
    }
  }
  
  override def toString = data.toString()
}
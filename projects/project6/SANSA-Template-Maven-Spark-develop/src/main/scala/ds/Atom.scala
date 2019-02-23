package ds
import scala.collection.mutable.ListBuffer

// This class creates a concept and a role atom

class Atom(name: String, atype: String, nvars: Int, vars: ListBuffer[Int]) extends java.io.Serializable {
  var atomName = name
  var atomType = atype
  var atomNVars = nvars

  var atomVars = ListBuffer[Int]()
  atomVars++=vars

  //println(atomVars)
  var dVars = ListBuffer[String]()

  override def toString():String={
    if (atomType=="Concept"){
      return "%s(%d)".format(atomName,atomVars(0))
    }
    else if (atomType=="Role"){
      return "%s(%s, %d)".format(atomName, atomVars(0), atomVars(1))
    }
    return ""
    }
    def main(args: Array[String]) {
    println("Hello World")
  }
}

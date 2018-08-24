package ds
import scala.collection.mutable.ListBuffer

class Atom(name: String, atype: String, nvars: Int) {
  var atomName = name
  var atomType = atype
  var atomNVars = nvars
  var atomVars = ListBuffer[Int]()
  var dVars = ListBuffer[String]()
  
  def main(args: Array[String]) {
    println("Hello World")
  }
}

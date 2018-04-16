package foil

import scala.io.Source
import java.util.ArrayList
import scala.collection.mutable.Map

object Main extends App {

  var DEBUG: Boolean = false
  
  override def main(args: Array[String]) = {
    this.DEBUG = false
    val start = System.nanoTime()
    KnowledgeBase.load
  	KnowledgeBase.print
  	KnowledgeBase.foil
    val end = System.nanoTime()
    println("Elapsed time: " + (end - start) / 1000000000.0 + "s")
  }

	def debug(out: String) {
	   if (DEBUG)
	      println(out)
	}
}





















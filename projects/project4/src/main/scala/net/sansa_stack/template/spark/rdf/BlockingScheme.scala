package net.sansa_stack.template.spark.rdf

import scala.collection.mutable

object BlockingSchemeCalculator {

  /** Calculates Blocking Scheme value (true or false) for a given feature vector.
    *
    *  @param scheme blocking scheme.
    */
  def calculate(scheme: String): Boolean = {

    var finalScheme = scheme.replaceAll(" and ", "&")
    finalScheme = finalScheme.replaceAll(" or ", "|")

    val operators = mutable.Stack[Char]()
    val values = mutable.Stack[Char]()

    for (i <- 0 to finalScheme.length - 1) {

      val c = finalScheme.charAt(i)

      if (c == '&' || c == '|') {
        operators.push(c)
      } else if (c == '1' || c == '0') {
        values.push(c)
      } else if (c == ')' && values.length > 1) {
        val op = operators.pop()
        var res = 0
        val first = values.pop()
        val second = values.pop()

        if (op == '&') {
          res = first.toInt & second.toInt
        } else if (op == '|') {
          res = first.toInt | second.toInt
        }

        values.push(res.toChar)
      }
    }

    val finalResult = values.pop()

    if (finalResult == '1') {
      return true
    } else {
      return false
    }
  }
}



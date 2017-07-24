package foil

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Literal representation
 */
class Literal (val predicate: String, val tuples: List[String]) {
  require (!tuples.isEmpty)

  def apply(i: Int) = {
    if (i <= 0) 
      predicate
    else
      tuples(i-1)
  }

  def len = tuples.length

  override def toString = {
    predicate + "(" + tuples.mkString(",") + ")"
  }

}

object Literal extends JavaTokenParsers {

  private def literal: Parser[Literal] = {
    predicate~tupples ^^ (x => new Literal(x._1, x._2))
  }

  private def predicate: Parser[String] = ident

  private def tupples: Parser[List[String]] = "("~>repsep(ident, ",")<~")"

  def parse(s: String): Literal = {
    parseAll(literal, s).get
  }

  def apply(s: String): Literal = parse(s)
}

package foil

import java.util.ArrayList
import java.io._

@SerialVersionUID(100L)
class Term(var name:String) extends Serializable{
   override def toString() : String = {
    name
  }
  
  def canEqual(that: Any) = that.isInstanceOf[Term]
  def apply_bindings(bindings: Map[String, List[List[String]]]) : Term = {
    this
  }
}

object Term {
  	
	/*
	 * Finds target variable position in the right-side predicate variables list
	 */
	def findTargetVariblePosition(targetVariable: Term, rightSideRuleVariables: List[Term]) = { 
    // find target variable position in the variables list of right-side predicate
    val result = (rightSideRuleVariables.indexOf(targetVariable), targetVariable) 
    //debug(targetVariable + " " + position)
    result
	}
	
	/*
	 * we store target position and body variable position
	 * term object can be variable or atom
	 */
	def positionList(target: (String, ArrayList[Term]), predicateVars: List[Term]) = {
	  val targetVars = target._2 // get variables list for the target predicate
    
    val positionList = new ArrayList[(Int, (Int, Term))]
    for (index <- 0 until targetVars.size()) {
      val targetVar = targetVars.get(index)
      val varPosition = Term.findTargetVariblePosition(targetVar, predicateVars) 
      
      if (varPosition._1 > -1) { // target variable exists on the right side 
        // they both must match in body predicate and target predicate tuples
        positionList.add((index, varPosition))
      }
    }
	  positionList
	}
}

@SerialVersionUID(100L)
class Atom(name:String) extends Term(name:String) with Serializable  {
    
  override def hashCode = {
    val hash =  if (name.isEmpty()) 0 else name.hashCode
    super.hashCode + hash
  }
  override def equals(other: Any): Boolean = other match { 
    case that: Atom => that.canEqual(this) && this.name == that.name
    case _ => false 
  }
  
  // TODO: check
  override def apply_bindings(bindings: Map[String, List[List[String]]]) : Term = {
  //TODO: check
    //if bindings .has_key(self): return bindings[self]
    //else: return self
    this
  }
}

@SerialVersionUID(100L)
class Var(name:String) extends Term(name:String) with Serializable  {

  var scope: List[String] = null
  
  def this(name: String, scope: List[String]) {
    this(name)
    this.scope = scope
  }
  
  override def equals(other: Any): Boolean = other match { 
    case other: Var => other.canEqual(this) && this.name == other.name
    case _ => false 
  }
  
  // TODO: check
  override def apply_bindings(bindings: Map[String, List[List[String]]]) : Term = {
    /*if bindings.has_key(self): return bindings[self]
    else: return self*/
    this
  }
}
  
object Var {
  var unique_count: Int = 0

  def get_unique(variable: Var) : Var = {
    Var.unique_count += 1
    new Var("@_" + Var.unique_count + "_" + variable.name)
  }
}

@SerialVersionUID(100L)
class Rule (predicates: ArrayList[Predicate]) extends Serializable{
  
  
  def getPredicates = {predicates}
}











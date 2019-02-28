package evo.arules

import ds._
import scala.util.Random
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap


// This class generates all the frequent atoms and broadcasts them on every node. It contains the major operations and wrapper
// for triggering rule operations which preserve the language bias 


class Operations extends java.io.Serializable {

  var maxRuleLength = 10
  val pcross = 0.6
  val pmut = 0.4
  val theta_mut = 0.2

  var a_f_Concept = new ListBuffer[String]()
  var a_f_Role = new ListBuffer[String]()

  var cList = new ListBuffer[Atom]()
  
  def makeAtom(aString:String, aType:String):Atom={
    
    var atom: Atom = null
    var nameToUse = aString
    var typeToUse = aType
    if (typeToUse=="Concept"){
      atom = new Atom(nameToUse,typeToUse, nvars = 1, vars = ListBuffer(1))
    }
    else{
      atom = new Atom(nameToUse,typeToUse, nvars = 2, vars = ListBuffer(1,2))
    }
    return atom
  }
  
  def binomialRoleOrConcept():Atom ={
    val rand = new Random()
    var k:Int = -1
    var aString:String = ""
    var aType:String = ""
    val prob = rand.nextFloat()
    if(prob > 0.5){
      if(a_f_Concept.length==1)
        k = 0
      else
        k = new Random().nextInt(a_f_Concept.length-1)
      aType = "Concept"
      aString = a_f_Concept(k)
    }
    else{
      k = new Random().nextInt(a_f_Role.length-1)
      aType = "Role"
      aString = a_f_Role(k)
    }
    var randAtom = makeAtom(aString, aType)
    return randAtom
  }
  
  def createNewPattern(): Rule ={
   
    var rule:Rule = new Rule()
    var n = new Random().nextInt(maxRuleLength-2)+2

    var randAtom = binomialRoleOrConcept()
    rule.registerAtomWithBindings(randAtom) 

    for (i <- 1 to n){
      rule = specialize(rule)
    }
    return rule
  }
  
  
  def mutate(r: Rule): Rule ={
    var r_new = new Rule()

    if (calculateHeadCoverage(r) > theta_mut) {
      if (r.size() < maxRuleLength)
        r_new = specialize(r)
      else
        r_new = createNewPattern()
    }
    else{
      if (r.size()>2)
        r_new = generalize(r)
      else
        r_new = createNewPattern()
    }
    return r_new
  }
  
  
  def generalize(r:Rule):Rule ={
    if (r.size()!=0) {
      r.removeLast()
    }
    return r
  }
  
  
  def randomAtom(a_f: ListBuffer[String], aType: String): Atom ={
    if (a_f.length-1==0)
      return makeAtom(a_f(0),aType)
    val x= new Random().nextInt(a_f.length-1)
    return makeAtom(a_f(x), aType)
  }
  
  
  def specialize(r:Rule): Rule ={
    val x= new Random().nextFloat()
    if (x<0.5){
      var atom = randomAtom(a_f_Concept, "Concept")
      enforceLanguageBiasSpecialize(r,atom,"Concept")
    }
    else{
      var atom = randomAtom(a_f_Role, "Role")
      if (x<0.75){
        enforceLanguageBiasSpecialize(r,atom,"Fresh")
      }
      else{
        enforceLanguageBiasSpecialize(r,atom,"All")
      }
    }
    return r
  }
  

  def mergeBodies(p:Rule, r:Rule): Rule = {

    var mergedAtomPool = new Rule()
    for(i <- 1 to p.size()){
      mergedAtomPool.addAtom(makeAtom(p.atomList(i).atomName, p.atomList(i).atomType))
    }
    for(i <- 1 to r.size()) {
      mergedAtomPool.addAtom(makeAtom(r.atomList(i).atomName, r.atomList(i).atomType))
    }
    return mergedAtomPool
  }

  
  def mergeBodies2(p:Rule, r:Rule): ListBuffer[Tuple2[String, String]] = {

    var mergedAtomPool = new ListBuffer[Tuple2[String, String]]()
    for(i <- 1 to p.size()){
      mergedAtomPool.append((p.atomList(i).atomName, p.atomList(i).atomType))
    }
    for(i <- 1 to r.size()) {
      mergedAtomPool.append((r.atomList(i).atomName, r.atomList(i).atomType))
    }
    return mergedAtomPool
  }  

  
  def recombine(p:Rule, r:Rule): (Rule,Rule) ={
    var p_old_head = makeAtom(p.atomList(0).atomName, p.atomList(0).atomType)
    var r_old_head = makeAtom(r.atomList(0).atomName, r.atomList(0).atomType)

    var L:ListBuffer[Tuple2[String, String]] = mergeBodies2(p,r)
    val rand = new Random()
    val RuleLength = L.length
    val len_pHat = rand.nextInt(RuleLength-2)+2
    val len_rHat = RuleLength-len_pHat

   
    var p_hat = new Rule()
    p_hat = enforceLanguageBiasCombine(p_hat, p_old_head)
    while(p_hat.size() < len_pHat){
      var a = L.remove(rand.nextInt(L.length))
      p_hat = enforceLanguageBiasCombine(p_hat,makeAtom(a._1, a._2))
      
    }
    
    var r_hat = new Rule()
    r_hat = enforceLanguageBiasCombine(r_hat, r_old_head)
    while(r_hat.size() < len_rHat){
      var a = L.remove(rand.nextInt(L.length))
      r_hat = enforceLanguageBiasCombine(r_hat,makeAtom(a._1, a._2))
      
    }
    return (p_hat,r_hat)
  }
 
  
  def enforceLanguageBiasCombine(r: Rule, atom:Atom):Rule ={
    if(atom.atomType=="Concept"){
      r.bindConceptVariables(atom)
    }
    else{
      r.bindRoleVariables(atom, "All")
    }
    return r
  }
  
  def enforceLanguageBiasSpecialize(r:Rule, atom:Atom, cType:String):Rule ={
   
    if (cType=="Concept"){
      r.bindConceptVariables(atom)
    }
    else{
      r.bindRoleVariables(atom, cType)
    }
    return r
  }
  
  def calculateHeadCoverage(r:Rule):Float = {
    var sigma_h = 0
    var e_h = 0
    
    return new Random().nextFloat()
  }


  
  
}
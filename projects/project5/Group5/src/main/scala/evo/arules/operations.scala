package evo.arules

import ds._
import scala.util.Random
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

class operations {
  val theta_mut = 0.1
  val maxRuleLength = 10
  var FreqConc = new ListBuffer[String]()
  var FreqRole = new ListBuffer[String]()
  var af = new ListBuffer[Atom]()
  var cList = new ListBuffer[Atom]()
  var TRIPLES = new ListBuffer[ListBuffer[Atom]]()
  
  def createNewPattern(): Rule ={
    var rule:Rule = new Rule()
    var n = new Random().nextInt(maxRuleLength-2)+2
    var randAtom = af(new Random().nextInt(af.length))
    rule.addAtom(randAtom)
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
    return r
  }
  
  
  def generalize(r:Rule):Rule ={
    r.removeLast()
    return r
  }
  
  
  def randomAtom(aType:String): Atom ={
    if (aType=="Concept"){
      val x= new Random().nextInt(TRIPLES.length-1)
      return TRIPLES(x)(2)
    }
    else{
      val x= new Random().nextInt(TRIPLES.length-1)
      return TRIPLES(x)(1)
    }
  }
  
  
  def specialize(r:Rule): Rule ={
    val x= new Random().nextFloat()
    var r_new = new Rule()
    if (x<0.5){
      var atom = randomAtom("Concept")
      enforceLanguageBiasSpecialize(r,atom,"Concept")
    }
    else{
      var atom = randomAtom("Role")
      if (x<0.75){
        enforceLanguageBiasSpecialize(r,atom,"Fresh")
      }
      else{
        enforceLanguageBiasSpecialize(r,atom,"All")
      }
    }
    return r
  }
  
  def recombine(p:Rule, r:Rule): (Rule,Rule) ={
    var L:Rule = p.mergeRules(r)
    
    val rand = new Random()
    val len_pHat = rand.nextInt(maxRuleLength-2)+2
    val len_rHat = rand.nextInt(maxRuleLength-2)+2
    
    var p_hat = new Rule()
    while(p_hat.size() < len_pHat){
      var a = L.remove(rand.nextInt(L.size()))
      p_hat = enforceLanguageBiasCombine(p_hat,a)
      //p_hat.addAtom(a)
      // Always last position
    }
    
    var r_hat = new Rule()
    while(r_hat.size() < len_pHat){
      var a = L.remove(rand.nextInt(L.size()))
      r_hat = enforceLanguageBiasCombine(r_hat,a)
      //p_hat.addAtom(a)
      // Always last position
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
      if (cType=="All"){
        r.bindRoleVariables(atom, cType)
      }
      else{
        r.bindRoleVariables(atom, "Fresh")
      }
    }
    return r
  }
  
  def calculateHeadCoverage(r:Rule):Float = {
    var sigma_h = 0
    var e_h = 0
    //var conf = new SparkConf().setMaster("local").setAppName("DistAR")
    //var sc = new SparkContext(conf)
    
    //var customTuples = new ListBuffer[(Atom, Atom,Atom)]()
    //customTuples += (new Atom("Concept","Man",1),new Atom("Concept","Man",2),new Atom("Concept","Man",2))
    
    //var rdd = sc.parallelize(customTuples, 1)
    
//    var m = new HashMap[Int, HashSet[String]]()
//    for(i <- 0 to r.size()-1){
//      for (j<- 1 to TRIPLES.length-1){
//        if(r.atomList(i).atomType=="Concept"){
//          
//          if(TRIPLES(j)(0).atomName==r.atomList(i).atomName){
//            if(!m.contains(r.atomList(i).atomVars(0))){
//              m+= (r.atomList(i).atomVars(0) -> new HashSet[String]())
//            }
//            m(r.atomList(i).atomVars(0))+=TRIPLES(j)(0).dVars(0)
//          }
//          if(TRIPLES(j)(2).atomName==r.atomList(i).atomName){
//            if(!m.contains(r.atomList(i).atomVars(0))){
//              m+= (r.atomList(i).atomVars(0) -> new HashSet[String]())
//            }
//            m(r.atomList(i).atomVars(0))+=TRIPLES(j)(2).dVars(0)
//          }
//        }
//      }
//    }
    
    return new Random().nextFloat()
  }
  
  
}
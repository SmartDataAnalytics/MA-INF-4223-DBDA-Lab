package ds
import scala.collection.mutable.ListBuffer
import scala.util.Random

class Rule {
  var atomList = ListBuffer[Atom]()
  
  var avaBinding = ListBuffer.range(1,20)
  var useBinding = ListBuffer[Int]()
  var priBinding = ListBuffer[Int]()
  
  def addAtom(atom: Atom)={
     atomList+=atom
   }
  
  def addRule(rule: Rule)={
    atomList++=rule.atomList 
  } 
  // This might not run. Verify it.
  
  def mergeRules(rule: Rule): Rule = {
    var r_new = new Rule()
    r_new.atomList=atomList
    r_new.atomList++=rule.atomList
    return r_new
  }
  
  def size():Int={
    return atomList.length
  }
  
  def inRule(atom:Atom):Int={
    //if(atomList.length<2)
    //  return -1
    for(i <- 0 to atomList.length){
      if (atom.atomName==atomList(i)){
        return i
      }
    }
    return -1
  }
  
  def removeLast()={
    remove(atomList.length-1)
  }
  
  def remove(id:Int): Atom={
    var t = atomList.remove(id)
    return t
  }
  
  def _avaBinding():Int ={
    var bind = -1
    bind = avaBinding.remove(0)
    priBinding+=bind
    return bind
  }
  
  def safeBindingNewConcept(): Int={
    var bind:Int = -1
    val rand = new Random()
    if (useBinding.length!=0){
      bind = useBinding(rand.nextInt(useBinding.length))
    }
    else{
      return _avaBinding()
    }
    return bind
  }
  
  def safeBindingExistingConcept(): Int={
    var bind:Int = -1
    if (priBinding.length!=0){
      bind = priBinding.remove(new Random().nextInt(priBinding.length))
      useBinding+=bind    
    }
    else {
      println("\n Could not use Existing Binding: Using Fresh binding")
      bind = _avaBinding()
    }
    return bind
  }
  
  def safeBindingRole(bType:String):(Int,Int)={
    var bind0:Int = -1
    var bind1:Int = -1
    val rand = new Random()
    
    if (priBinding.length!=0){
      bind0 = priBinding.remove(rand.nextInt(priBinding.length))
      useBinding+=bind0
    }
    else{
      if (useBinding.length!=0){
        bind0 = useBinding(rand.nextInt(useBinding.length))
      }
      else{
        println("\n Could not use Existing Binding: Using Fresh binding")
        bind0 = _avaBinding()
      }
    }
    
    if (bType =="all"){
      if (priBinding.length!=0){
      bind1 = priBinding.remove(rand.nextInt(priBinding.length))
      useBinding+=bind0
      }
      else{
        if (useBinding.length!=0){
          bind1 = useBinding(rand.nextInt(useBinding.length))
        }
        else{
          println("\n Could not use Existing Binding: Using Fresh binding")
          bind1 = _avaBinding()
        }
      }      
     }
    else{
      bind1 = _avaBinding()
    }
    return (bind0, bind1)
  }
  
  
  def bindConceptVariables(atom:Atom)={
    if (inRule(atom)!=(-1)){
      atom.atomVars(0)=safeBindingExistingConcept()
    }
    else{
      atom.atomVars(0)=safeBindingNewConcept()
    }
    addAtom(atom)
  }
  
  
  def bindRoleVariables(atom:Atom, bType:String)={
    var t=safeBindingRole(bType)
    atom.atomVars(0)=t._1
    atom.atomVars(1)=t._2
    addAtom(atom)
  }
  
}
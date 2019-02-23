package ds
import scala.collection.mutable.ListBuffer
import scala.util.Random

// This class is for generation of SWRL rules and has a lot of operation regarding rules like rule safety and
//language bias operations


class Rule extends java.io.Serializable {
  var atomList = ListBuffer[Atom]()
  var hC:Double = 0.0

  var availableBinding = ListBuffer.range(1,20)
  var usedBinding = ListBuffer[Int]()
  var priorityBinding = ListBuffer[Int]()

  def addAtom(atom: Atom)={
     atomList+=atom
   }

  def registerAtomWithBindings(atom: Atom)={
    if (atom.atomType == "Concept"){
        bindConceptVariables(atom)
      }
    else{
      bindRoleVariables(atom, "All")
    }
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
    return atomList.length-1
  }

  def inRule(atom:Atom):Int={
    if(atomList.length<1)
      return -1
    //print(atomList.length)
    for(i <- 0 to this.size()){
      if (atom.atomName==atomList(i).atomName){
        return i
      }
    }
    return -1
  }

  def removeLast() :Atom ={
    var t = remove(atomList.length-1)
    return t
  }

  def remove(id:Int): Atom={
    var t = atomList.remove(id)
    return t
  }

  def _avaBinding():Int ={
    var bind = availableBinding.remove(0)
    //priorityBinding+=bind
    return bind
  }

  def safeBindingExistingConcept(existingBinding: Int): Int={
    var bind:Int = -1
    val rand = new Random()

    if (!(priorityBinding.contains(existingBinding) && priorityBinding.length-1==0) && priorityBinding.length!=0){
     
      if(priorityBinding.length==1){
            bind = priorityBinding(0)
          }
      else do{
        bind = priorityBinding(rand.nextInt(priorityBinding.length))
      } while(bind==existingBinding)
        println(existingBinding, bind, priorityBinding.indexOf(bind))
        bind = priorityBinding.remove(priorityBinding.indexOf(bind))
      usedBinding+=bind
    }
    else if (!(usedBinding.contains(existingBinding) && usedBinding.length-1==0) && usedBinding.length!=0){
      if(usedBinding.length==1){
         bind = usedBinding(0)
       }
      else do{
        bind = usedBinding(rand.nextInt(usedBinding.length))
      } while(bind==existingBinding)
     
    }
    else{
     
      bind = _avaBinding()
      priorityBinding+=bind
    }
    print("Safe by concept leaves")
    return bind
  }


  def safeBindingNewConcept(): Int={
    var bind:Int = -1
    if (priorityBinding.length!=0){
      if(priorityBinding.length==1)
        bind = priorityBinding.remove(0)
      else
        bind = priorityBinding.remove(new Random().nextInt(priorityBinding.length))
      usedBinding+=bind
    }
    else if(usedBinding.length!=0){
      if(usedBinding.length==1)
        bind = usedBinding(0)
      else
        bind = usedBinding(new Random().nextInt(usedBinding.length))
    }
    else {
     
      bind = _avaBinding()
      priorityBinding+=bind
    }
    return bind
  }


  def safeBindingRole(bType:String):(Int,Int)={
    var bind0:Int = -1
    var bind1:Int = -1
    val rand = new Random()

    if (priorityBinding.length!=0){
      bind0 = priorityBinding.remove(rand.nextInt(priorityBinding.length))
      usedBinding+=bind0
    }
    else if (usedBinding.length!=0){
      bind0 = usedBinding(rand.nextInt(usedBinding.length))
      }
    else{
      bind0 = _avaBinding()
      priorityBinding+=bind0
    }

    if (bType =="All"){
      if (!(priorityBinding.contains(bind0) && priorityBinding.length-1==0) && priorityBinding.length!=0){
        if(priorityBinding.length==1){
            bind1 = priorityBinding(0)
          }
        else do {
          bind1 = priorityBinding(rand.nextInt(priorityBinding.length))
        } while(bind0==bind1)
        priorityBinding.remove(priorityBinding.indexOf(bind1))
        usedBinding+=bind1
      }
      else{
        if (!(usedBinding.contains(bind0) && usedBinding.length-1==0) && usedBinding.length!=0){
          if(usedBinding.length==1){
              bind1 = usedBinding(0)
            }
          else do {
            bind1 = usedBinding(rand.nextInt(usedBinding.length))
          }while(bind0==bind1)
        }
        else{
          bind1 = _avaBinding()
          priorityBinding+=bind1
        }
      }      
     }
    else{ // IF BINDING TYPE IS 'FREE'!!
      bind1 = _avaBinding()
      priorityBinding+=bind1
    }
    return (bind0, bind1)
  }
  
  
  def bindConceptVariables(atom:Atom)={
    val check = inRule(atom)
    if (check!=(-1)){
      var existingBinding = atomList(check).atomVars(0)
      atom.atomVars(0)=safeBindingExistingConcept(existingBinding)
    }
    else{
      atom.atomVars(0)=safeBindingNewConcept()
    }
    addAtom(atom)
  }
  
  
  def bindRoleVariables(atom:Atom, bType:String)={
    var t=safeBindingRole(bType)
    //print(atom)
    atom.atomVars(0)=t._1
    atom.atomVars(1)=t._2
    addAtom(atom)
  }


  override def toString():String={
    var res = ""
    for (i <- 0 to this.size()){
      //println(atomList(i).atomName)
      if (i==1){
        res+=" <- "
      }
      res+=atomList(i).toString()
      if (i>=1 && i!=this.size()){
        res+=" and "
      }
    }
    return res
  }
  
}

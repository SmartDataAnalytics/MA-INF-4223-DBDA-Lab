import scala.collection.Seq

class Particle_PSO(dimension:Int) extends Serializable  {
  
  var p_position = (Seq.fill(dimension)(math.random)).toArray
  var p_velocity = (Seq.fill(dimension)(math.random)).toArray
  var p_best = (Seq.fill(dimension)(math.random)).toArray
  
  def update_position(p:Particle_PSO): Particle_PSO = {
    for(i <- 0 to dimension-1){
        p.p_position(i) = p.p_position(i) + p.p_velocity(i)
      }
    return p
  }
  
  def update_velocity(p:Particle_PSO,gbest_position:Array[Double]): Particle_PSO ={
     for(i <- 0 to dimension-1){
        var toward_pbest= math.random*(p.p_best(i)-p.p_position(i))
        var toward_gbest =  2*math.random*(gbest_position(i)-p.p_position(i))
        p.p_velocity(i) = 0.5*p.p_velocity(i) + toward_pbest + toward_gbest
      }
     return p
  }
  
  def pbest_calc(p:Particle_PSO) : Particle_PSO = {
     if(obj_func(p.p_position) < obj_func(p.p_best)){
        p.p_best = p.p_position
      }
      return p    
  }
  
   //val obj_func = (x:Array[Double] ) => 5 + (1/(math.pow(x(0),2) + math.pow(x(1),2)))
    def obj_func(x:Array[Double]):Double = {
      var temp:Double  =0
      for(dim <- 0 to x.length-1 )
      {
        temp =temp + (math.pow(x(dim),2))
      }
      return temp
    }
}
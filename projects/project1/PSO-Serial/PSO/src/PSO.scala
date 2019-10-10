import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level


object PSO {
  
  var dimension = 20
  var no_of_Particle_PSOs = 1500
  var no_of_iteration = 10000
  var gbest_position=Array.fill(dimension)(math.random)
  
    
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf=new SparkConf().setAppName("Serial : "+dimension+"-"+no_of_Particle_PSOs+"-"+no_of_iteration).setMaster("spark://abhi8569:7077").set("spark.eventLog.enabled","true")
              .set("spark.eventLog.dir","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
              .set("spark.history.fs.logDirectory","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
    val sc = new SparkContext(conf)
    
    println("Serial")
    
    var swarm = ArrayBuffer[Particle_PSO]()
    for(i <- 0 to no_of_Particle_PSOs-1){
      swarm += new Particle_PSO(dimension) with Serializable
    }

    swarm.foreach(f => global_best_position(f.p_position))
    swarm.foreach(f => init_pbest(f))
        
    var updated_velocity_swarm = ArrayBuffer[Particle_PSO]()
    
    for(iteration <- 0 to no_of_iteration){
      swarm.foreach{x => update_Particle_PSO(x)}     
      println("Iteration number "+iteration +" : "+obj_func(gbest_position))
    }
    
    def update_Particle_PSO(p:Particle_PSO):Particle_PSO={
      
      for(i <- 0 to dimension-1){
        var toward_pbest= math.random*(p.p_best(i)-p.p_position(i))
        var toward_gbest =  2*math.random*(gbest_position(i)-p.p_position(i))
        p.p_velocity(i) = 0.5*p.p_velocity(i) + toward_pbest + toward_gbest
      }
      for(i <- 0 to dimension-1){
        p.p_position(i) = p.p_position(i) + p.p_velocity(i)
      }
      
      if(obj_func(p.p_position) < obj_func(p.p_best)){
        p.p_best = p.p_position
      }
      
      if(obj_func(p.p_best) < obj_func(gbest_position)){
        gbest_position=p.p_best
        //println("Best value after each Particle_PSO update : ",obj_func(gbest_position))
      }
      return p
    }
    sc.stop()
  }
  
  //val obj_func = (x:Array[Double] ) => math.pow(x(0),2) + math.pow(x(1),2)
  
  def obj_func(x:Array[Double]):Double = {
      var temp:Double  =0
      for(dim <- 0 to x.length-1 )
      {
        temp =temp + (math.pow(x(dim),2))
      }
      return temp
    }
  
 def global_best_position(pos:Array[Double])={
    if(obj_func(pos) < obj_func(gbest_position)){
      gbest_position=pos
    }
  }  
  
  def init_pbest(p:Particle_PSO)={
    p.p_best = p.p_position
  }  
}
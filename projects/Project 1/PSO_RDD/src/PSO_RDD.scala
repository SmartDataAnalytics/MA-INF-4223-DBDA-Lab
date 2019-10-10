import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PSO_RDD {
  
  var dimension = 2
  var no_of_particles = 1000
  var no_of_iteration_External = 10    //No. of times gbest has to be synced accross nodes
  var no_of_iteration_Internal = 1000             //No. of times pbest has to be evaluated for each particle
  var c1 = 1
  var c2 = 2
  var w = 0.5
  var g_best=Array.fill(dimension)(math.random)
  def main(args:Array[String]): Unit ={
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("2W4C3G :: "+dimension+"-"+no_of_particles+"-"+no_of_iteration_External+"-"+no_of_iteration_Internal)
                            .setMaster("spark://abhi8569:7077")
                            .set("spark.eventLog.enabled","true")
                            .set("spark.eventLog.dir","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
              //.set("spark.history.fs.logDirectory","file:///home/abishek/Downloads/spark-2.4.3-bin-hadoop2.7/history/")
    val sc = new SparkContext(conf)
    
    var swarm = ArrayBuffer[Particle]()
    for(i <- 0 to no_of_particles-1){
      swarm += new Particle(dimension,i) with Serializable   //Initialize particle and add to array buffer
    }
    var swarm_rdd = sc.parallelize(swarm,8)  //No. of partitions, we usually keep it as (no of worker*no of core)

    swarm_rdd.foreach(f => init_pbest(f))      //Initialize particles p_best
    swarm_rdd.foreach(f => f.compute_pbest_fitness()) //compute p_best fitness value
    swarm_rdd.foreach(f => f.compute_pfitness())  //compute current fitness value
    swarm_rdd.foreach(f => global_best_position(f))  //compute global best position
    
    var temp_rdd: RDD[Particle]= sc.emptyRDD[Particle]
    temp_rdd=swarm_rdd
    var updated_velocity_swarm: RDD[Particle]= sc.emptyRDD[Particle]
    
    var bCast = sc.broadcast(g_best)   //broadcast current global best
    
    println(obj_func(g_best))
    for(iteration <- 0 to no_of_iteration_External){
      updated_velocity_swarm = temp_rdd.map{x => update_particle(x)}  //update each particle
      temp_rdd = updated_velocity_swarm
      temp_rdd.collect().foreach(f => global_best_position(f))  //compute new global best
      println(obj_func(g_best))
      bCast.unpersist(blocking=true)  //remove old global best from worker nodes
      bCast = sc.broadcast(g_best) //broadcast updated global best 
    }
    
    def update_particle(p:Particle):Particle={
      g_best = bCast.value            //initialize global best to the broadcasted value
     for(iter <- 0 to no_of_iteration_Internal){        //update the particle
      for(i <- 0 to dimension-1){                        //update velocity
        var toward_pbest= c1*math.random*(p.p_best(i)-p.p_position(i))
        var toward_gbest =  c2*math.random*(g_best(i)-p.p_position(i))
        p.p_velocity(i) = w*p.p_velocity(i) + toward_pbest + toward_gbest
      }
      for(i <- 0 to dimension-1){                        //update position
        p.p_position(i) = p.p_position(i) + p.p_velocity(i)
      }
      
      p.compute_pfitness()          //compute p_fitness
      
      if(p.p_fitness < p.pbest_fitness){  //compute p_best position
        p.p_best = p.p_position
      }
      
      p.compute_pbest_fitness()      //computr p_best fitness
      
      if(p.pbest_fitness < obj_func(g_best)){      //compute gbest
        g_best=p.p_position
      }
      //println("Iteration Number : "+iter+" || Particle Number :  "+p.p_id+" || Pbest : "+obj_func(p.p_best)+" || Gbest : "+obj_func(gbest_position))
      }
      return p  //return updated particle
    }
    sc.stop()
  }
  
  //val obj_func = (x:Array[Double] ) => 1 + (1/(math.pow(x(0),2) + math.pow(x(1),2)))
  
  def obj_func(x:Array[Double]):Double = {   //Sphere Function
      var temp:Double  =0
      for(dim <- 0 to x.length-1 )
      {
        temp =temp + (math.pow(x(dim),2))
      }
      return temp
    }
  
//  def obj_func(x:Array[Double]):Double = {   //Rosenbrock Function
//      var temp:Double  =0
//      for(dim <- 0 to x.length-2 )
//      {
//        temp =temp + 100*math.pow((x(dim+1)-math.pow(x(dim),2)),2) + math.pow((x(dim)-1),2)
//      }
//      return temp
//    }
  
  def global_best_position(p:Particle)={     
    if(p.pbest_fitness < obj_func(g_best)){
      g_best=p.p_best
    }
  }  
  
  def init_pbest(p:Particle)={
    p.p_best = p.p_position
  }  
}
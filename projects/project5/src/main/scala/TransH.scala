import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.math._

import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import scala.util._
import util.control.Breaks._
import com.intel.analytics.bigdl.nn.Power
import org.apache.spark.sql._
import com.intel.analytics.bigdl.utils.T
import net.sansa_stack.rdf.spark.kge.triples.IntegerTriples
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.{ArrayBuffer, Map}
import java.io.PrintWriter

/**
 * TransH model
 * 
 * @param train The training data tuples
 * @param ne The number of entities
 * @param nr The number of relations
 * @param batch The size of the minibatch for SGD
 * @param k The length of the embedding vectors to be learned
 * @param margin The margin parameter needed to calculate the loss
 * @param epochs The number of epochs to be run
 * @param sk SparkSession
 */
class TransH(train: Dataset[IntegerTriples], ne: Int, nr: Int, batch: Int, k: Int, margin: Float, epochs: Int, rate: Float, sk: SparkSession) 
extends Models(ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession){
  
  //Calculate the number of batches to be run in an epoch based on minibatch size and size of training data 
  val nBatches = (train.count() / batch).toInt
  
  //Initialize Tensor containing normal vectors w for each relation
  var wR: Tensor[Float] = normalizeTensor(initialize(nr), true)
  
  //We tried to use Adam Method from bigdl library to learn our parameters but we faced difficulties
  //by the way it was constructed, the adam method was a parameter of a Module Class and creating a custom module
  //was strongly coupled to a neural network approach which was not compatible for our TransH model
  //var opt = new Adam(learningRate = rate)
  
  def run(): Unit = {

    for (i <- 1 to epochs) {
      
      println("Start epoch " + i)
      //Accumulator to store the total loss
      var ac_res = sk.sparkContext.longAccumulator
      //Starting time of an epoch
      var start = System.currentTimeMillis()

      //Loop 1 to number of batches for an epoch
      for(batch <- 1 to 50){
        
        //Broadcast the embeddings of entities, relations and normal vectors w
        val bcE = sk.sparkContext.broadcast(e)
        val bcR = sk.sparkContext.broadcast(r)
        val bcWr = sk.sparkContext.broadcast(wR)
        
//        if(batch%10 == 0)
//          println(batch)
        
        //Get a sample/minibatch of the training data
        val sample = subset(train).rdd
        
        /**
         * We used map partitions because we found it better suited for this case and also it's faster than map function.
         * For each partition of the RDD containing the sample data we run the TransH learning algorithm:
         * 1) For each tuple corrupt the head and tail (one at a time) by a random entity in the knowledge graph
         * 2) Calculate the loss/error from a true tuple and the corresponding corrupted tuple
         * 3) If the loss/error is greater than 0 then optimize the parameters (entity, relation, normal vector w embeddings)
         * 4) After optimizing for a tuple, make sure the TransH model constraints hold:
         * 		- L2 norm of an embedding of an entity <= 1
         * 		- the embeddings of entities must be orthogonal to the normal vectors w (dot product = 0 theoretically)
         * 		- L2 norm of the normal vectors w = 1
         */
        val partitions = sample.mapPartitions{
          iterator =>
            
            //Create maps to contain the embeddings of entities, relations, normal vectors w in each partition
            var ePerPartition = Map[Int,Tensor[Float]]()
            var rPerPartition = Map[Int,Tensor[Float]]()
            var wrPerPartition = Map[Int,Tensor[Float]]()
            var retMap = ArrayBuffer[(Map[Int,Tensor[Float]],Map[Int,Tensor[Float]], Map[Int,Tensor[Float]])]()
           
            while(iterator.hasNext){
                val sampleTriple = iterator.next()
                //For each tuple add the corresponding embeddings in the maps per partition
                if(!ePerPartition.contains(sampleTriple.Subject)){
                  ePerPartition(sampleTriple.Subject) = bcE.value(sampleTriple.Subject)
                }
                if(!rPerPartition.contains(sampleTriple.Predicate)){
                  rPerPartition(sampleTriple.Predicate) = bcR.value(sampleTriple.Predicate)
                  wrPerPartition(sampleTriple.Predicate) = bcWr.value(sampleTriple.Predicate)
                }
                if(!ePerPartition.contains(sampleTriple.Object)){
                  ePerPartition(sampleTriple.Object) = bcE.value(sampleTriple.Object)
                }
                
                /**
                 * Corrupt a tuple by replacing the head or tail with another entity from the knowledge graph.
                 * However since it's distributed we couldn't find an efficient way to check if the corrupted tuple
                 * doesn't exist in the training data
                 */
                var corruptedTriple = tuple(sampleTriple)
                val corruptedIndex = if (sampleTriple.Subject == corruptedTriple.Subject) corruptedTriple.Object else corruptedTriple.Subject
                //Add the embeddings of the corrupted entity to the map 
                if(!ePerPartition.contains(corruptedIndex)){
                  ePerPartition(corruptedIndex) = bcE.value(corruptedIndex)
                }
                
                //Calculate the TransH margin-based error
                var error = margin + dist(sampleTriple, ePerPartition, rPerPartition, wrPerPartition) - dist(corruptedTriple, ePerPartition, rPerPartition, wrPerPartition);
                
                /**
                 * We are only interested in the case where the error is >0 because that would mean that the distance for a 
                 * correct tuple was higher than the distance of a corrupted tuple (we want it the other way so the distance
                 * of a correct tuple to be smaller then the distance of the corrupted tuple)
                 */
                if (error > 0) {
                  ac_res.add(error.toLong) //add error to the accumulator
                  /**
                   * Optimize the tuples: the correct tuple's embedding vector direction should go into the opposite way 
                   * of the gradient, while the corrupt one should continue in the direction of the gradient, 
                   * so we minimize the loss for a correct tuple and maximize for the corrupted one 
                   */
                  optimizeTuple(sampleTriple, 1, ePerPartition, rPerPartition, wrPerPartition)
                  optimizeTuple(corruptedTriple, -1, ePerPartition, rPerPartition, wrPerPartition)
                }
                
                /**
                 * Apply the 3 constrains of the TransH model
                 */
                ePerPartition(sampleTriple.Subject) = normalizeTensor(ePerPartition(sampleTriple.Subject), false)
                ePerPartition(sampleTriple.Object) = normalizeTensor(ePerPartition(sampleTriple.Object), false)
                ePerPartition(corruptedIndex) = normalizeTensor(ePerPartition(corruptedIndex), false)
                var (xS, yS) = forceOrthogonality(ePerPartition(sampleTriple.Subject), wrPerPartition(sampleTriple.Predicate))
                ePerPartition(sampleTriple.Subject) = xS
                wrPerPartition(sampleTriple.Predicate) = yS
                var (xO, yO) = forceOrthogonality(ePerPartition(sampleTriple.Object), wrPerPartition(sampleTriple.Predicate))
                ePerPartition(sampleTriple.Object) = xO
                wrPerPartition(sampleTriple.Predicate) = yO
                var (xC, yC) = forceOrthogonality(ePerPartition(corruptedIndex), wrPerPartition(sampleTriple.Predicate))
                ePerPartition(corruptedIndex) = xC
                wrPerPartition(sampleTriple.Predicate) = yC
                
            }
            retMap += ((ePerPartition,rPerPartition, wrPerPartition))
            retMap.iterator
        }
        /**
         * After finishing the map partitions collect the result and update the embedding Tensors
         * of entities, relations and normal vectors w
         */
        val partitionsCollect = partitions.collect()
        partitionsCollect.foreach{
          p=>
            p._1.foreach{
              kv=>
                e(kv._1) = kv._2
            }
            p._2.foreach{
              kv=>
                r(kv._1) = kv._2
            }
            p._3.foreach{
              kv=>
                wR(kv._1) = kv._2
            }
        }
      }

      var end = System.currentTimeMillis()
      println("End epoch " + i + " | error: " + ac_res.value)
      println("End epoch " + i + " | time: " + (end-start)/1000.0)
      //println(i+","+ac_res.value+","+(end-start)/1000.0)

      /**
       * After each epoch we save the last embeddings of entities, relations and normal vectors to files.
       */
      val out1 = new PrintWriter("FB15k/trained/e.txt")
      val out2 = new PrintWriter("FB15k/trained/r.txt")
      val out3 = new PrintWriter("FB15k/trained/wR.txt")
      for(i <- 1 to e.size(1)){
        for(j <- 1 to k){
          out1.print(e(i)(j).value())
          out1.print("\t")
        }
        out1.println()
      }
      for(i <- 1 to r.size(1)){
        for(j <- 1 to k){
          out2.print(r(i)(j).value())
          out2.print("\t")
        }
        out2.println()
      }
      for(i <- 1 to wR.size(1)){
        for(j <- 1 to k){
          out3.print(wR(i)(j).value())
          out3.print("\t")
        }
        out3.println()
      }
      
      out1.close()
      out2.close()
      out3.close()
    }
  }
  
  /**
   * Normalize each row of a Tensor (an embedding vector) to one
   * (Get the l2 norm of the vector and divide each element of the vector with the norm)
   * 
   * @param data A Tensor containing embeddings 
   * @param norm2One if false will not normalize vectors whose norm is smaller than 1 else normalize all
   * 
   * @return Normalized Tensor rows
   */
  def normalizeTensor(data: Tensor[Float], norm2One: Boolean): Tensor[Float] = {
    for(i <- 1 to data.size(1)){
      val norm = data(i).norm(2)
      if(norm > 1 && norm2One == false)
        data(i) = data(i)./(norm)
      else if (norm2One == true)
        data(i) = data(i)./(norm)
    }
    data
  }
  
  /**
   * This function tries to force orthogonality between two vectors/ the embeddings of an entity/relation with the normal vectors w
   * 
   * @param x The embedding of an entity/relation (1D Tensor)
   * @param wR The corresponding normal vector w (1D Tensor)
   * 
   * @return (cx, cwR) Tuple with the updated vectors (1D Tensor, 1D Tensor)
   */
  def forceOrthogonality(x: Tensor[Float], wR: Tensor[Float]): (Tensor[Float],  Tensor[Float]) = {
    var cx = x.clone()
    var cwR = wR.clone()
    cwR = normalizeTensor(cwR, true)

    breakable { 
      while(true){
        cwR = normalizeTensor(cwR, true)
        var dotProd = (cx * cwR).value()
        
        if(dotProd > 0.1){    //If dot product > 1 subtract learning rate from the vectors
           cx.sub(rate, cwR)
           cwR.sub(rate, cx)
        }else 
          break
      }
    }
  
    cwR = normalizeTensor(cwR, true)
    (cx, cwR)
  }
  
  /**
   * Calculate the distance of a tuple base on TransH formula
   *  
   *  @param tuple The tuple (headID, relationID, tailID)
   *  @param bcE Map containing keys entityIds and values corresponding embedding
   *  @param bcR Map containing keys relationIds and values corresponding embedding
   *  @param bcWr Map containing keys normal vecor w Ids and values corresponding embedding
   *  
   *  @return The distance score
   */
  def dist(tuple: IntegerTriples, bcE: Map[Int,Tensor[Float]], bcR: Map[Int,Tensor[Float]], bcWr: Map[Int,Tensor[Float]]) : Double = {
    var hE = bcE(tuple.Subject)         //head embedding
    var tE = bcE(tuple.Object)          //tail embedding
    var rE = bcR(tuple.Predicate)       //relation embedding
    var wrE = bcWr(tuple.Predicate)     //wr orthogonal vector hyperplane
    var wrExtE = (wrE * tE).value()
    var wrExhE = (wrE * hE).value()
    var res = (tE - (wrE * wrExtE) - hE + (wrE * wrExhE) - rE).abs().sum()
    
    res
  }
  
  /**
   * This function optimizes the parameters/embeddings for a tuple and re-apply constrains 
   * 
   * @param tuple The tuple to fit to TransH model
   * @param direction 1 for gradient descent for correct tuple, -1 o/w
   * @param ePerPartition Map containing entity embeddings in the current partition
   * @param rPerPartition  Map containing relations embeddings in the current partition
   * @param wrPerPartition  Map containing normal vectors w embeddings in the current partition
   */
  def optimizeTuple(tuple: IntegerTriples, direction: Float, 
    ePerPartition:  Map[Int,Tensor[Float]],
    rPerPartition:  Map[Int,Tensor[Float]],
    wrPerPartition:  Map[Int,Tensor[Float]]
    ): Unit = {
    
    var hE = ePerPartition(tuple.Subject)         //head embedding
    var tE = ePerPartition(tuple.Object)          //tail embedding
    var rE = rPerPartition(tuple.Predicate)       //relation embedding
    var wrE = wrPerPartition(tuple.Predicate)     //wr orthogonal vector hyperplane
    
    var wrExtE = (wrE * tE).value()
    var wrExhE = (wrE * hE).value()

    //Calculate the gradient
    var gradient = (tE - (wrE * wrExtE) - hE + (wrE * wrExhE) - rE) // * 2 but won't matter for L1 distance
    
    //Use L1 distance as said in the paper
    gradient = gradient.apply1(i => signum(i))
    var sumGr = (gradient * wrE).value()
    
    //update the parameters
    var gxdxr = gradient * direction * rate
    rPerPartition(tuple.Predicate).sub(gxdxr)
    ePerPartition(tuple.Subject).sub(gxdxr)
    ePerPartition(tuple.Object).add(gxdxr)
    wrPerPartition(tuple.Predicate).add(gxdxr * wrExtE)
    wrPerPartition(tuple.Predicate).sub(gxdxr * wrExhE)
    wrPerPartition(tuple.Predicate).add(hE * direction * rate * sumGr)
    wrPerPartition(tuple.Predicate).sub(tE * direction * rate * sumGr)
    
    //re-apply constrains
    ePerPartition(tuple.Subject) = normalizeTensor(ePerPartition(tuple.Subject), false)
    ePerPartition(tuple.Object) = normalizeTensor(ePerPartition(tuple.Object), false)
    rPerPartition(tuple.Predicate) = normalizeTensor(rPerPartition(tuple.Predicate), false)
    wrPerPartition(tuple.Predicate) = normalizeTensor(wrPerPartition(tuple.Predicate), true)
    var (x, y) = forceOrthogonality(rPerPartition(tuple.Predicate), wrPerPartition(tuple.Predicate))
    rPerPartition(tuple.Predicate) = x
    wrPerPartition(tuple.Predicate) = y
  }
  
  
  
//  def optimizeTuple(tuple: IntegerTriples, direction: Float, 
//      ePerPartition:  Map[Int,Tensor[Float]],
//      rPerPartition:  Map[Int,Tensor[Float]],
//      wrPerPartition:  Map[Int,Tensor[Float]],
//      error: Float
//      ): Unit = {
//    
//    var dir = direction
//    var updateNormalVectorWr = false
//    var sumGradientContraint = 0
//    
//    def funcEval(x: Tensor[Float]) = {
//      var hE = ePerPartition(tuple.Subject)         //head embedding
//      var tE = ePerPartition(tuple.Object)          //tail embedding
//      var rE = rPerPartition(tuple.Predicate)       //relation embedding
//      var wrE = wrPerPartition(tuple.Predicate)     //wr orthogonal vector hyperplane
//      var gradient = tE - wrE.*((wrE.t().dot(tE))) - hE- wrE.*((wrE.t().dot(hE))) - rE //*2 but won't matter if we use L1
//      
//      if(!updateNormalVectorWr){
//        gradient = gradient.apply1(i => dir*signum(i)) //L1 direction (-) means gradient descent for correct triples
//      }else{
//        if(dir == -1){
//          gradient = gradient.apply1(i => dir*signum(i)*(wrE.t().dot(tE)))
//        }else{
//          gradient = gradient.apply1(i => -1*dir*signum(i)*(wrE.t().dot(hE)))
//        }
//      }
//
//      (error.toFloat, gradient)
//    }
//    
//    var (newS, xs) = opt.optimize(funcEval, ePerPartition(tuple.Subject)) 
//    ePerPartition(tuple.Subject) = newS
//    var (newR, xr) = opt.optimize(funcEval, rPerPartition(tuple.Predicate))
//    rPerPartition(tuple.Predicate) = newR
//    updateNormalVectorWr = true
//    var (newWr, xwr) = opt.optimize(funcEval, wrPerPartition(tuple.Predicate))
//    wrPerPartition(tuple.Predicate) = newWr
//    dir *= -1
//    var (newWr2, xwr2) = opt.optimize(funcEval, wrPerPartition(tuple.Predicate))
//    wrPerPartition(tuple.Predicate) = newWr2
//    updateNormalVectorWr = false
//    var (newT, xt) = opt.optimize(funcEval, ePerPartition(tuple.Object)) 
//    ePerPartition(tuple.Object) = newT
//    
//    ePerPartition(tuple.Subject) = normT(ePerPartition(tuple.Subject))
//    ePerPartition(tuple.Object) = normT(ePerPartition(tuple.Object))
//    rPerPartition(tuple.Predicate) = normT(rPerPartition(tuple.Predicate))
//    wrPerPartition(tuple.Predicate) = normalize2one(wrPerPartition(tuple.Predicate))
//    //norm r ; wr
//  }
  
}
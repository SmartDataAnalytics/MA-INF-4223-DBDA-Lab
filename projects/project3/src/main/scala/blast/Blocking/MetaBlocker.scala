package blast.Blocking
import blast.AttributeSchema.AttributeProfile
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

class MetaBlocker(spark : SparkSession, clusterEntropies : Map[Int, Double]){

  private val _spark = spark
  private val PriClusterEntropies = clusterEntropies
  def calculate(blocks : RDD[Tuple2[Tuple2[String, Int],List[String]]], profileIndex : RDD[Tuple2[String, Set[Tuple2[String,Int]]]] , DS1Attr : AttributeProfile, DS2Attr : AttributeProfile) : RDD[Tuple2[String,String]] = {
    //tuples (entity id, entity profile)
    val entityByKeys = DS1Attr.getEntityProfiles.map(x => ("DS1"+x.getEntityUrl, x)) ++ DS2Attr.getEntityProfiles.map(x=> ("DS2"+x.getEntityUrl, x))

    //generates directed edges from blocks. one edge is added in each direction
    val edges = createEdges2( profileIndex, blocks.count().toInt )

    println("generating edges")
    println(edges.count())
    println("edges generated")

    //need to get the occurences of blocking keys for each of the nodes



    //calculates the maximum weighted edge departing from each edge and calculate node-theta from it
    val c = 2.0 // c embedded into aggregator function
    val thetaPerNodeDS1 = edges.aggregateByKey(0.0)((mv : Double , tup : Tuple2[String, Double]) => math.max(mv/c, tup._2), (a: Double, b : Double) => math.max(a,b))
    val thetaPerNodeDS2 = edges.map{case (profileA,(profileB, weight)) => (profileB,(profileA, weight))}.aggregateByKey(0.0)((mv : Double , tup : Tuple2[String, Double]) => math.max(mv/c, tup._2), (a: Double, b : Double) => math.max(a,b))
    val thetaPerNode = thetaPerNodeDS1 ++ thetaPerNodeDS2
    println("calculated thetaas")

    //we collect the theta values and broadcast them avoiding many network operations
    //https://stackoverflow.com/a/17690254
    val thetasPerNodeBrd = spark.sparkContext.broadcast(thetaPerNode.collectAsMap())

    //remove those edges that have weight less than threshold according to blast strategy and those
    //where the id of the first is > of the second. Previously we included edges in both direction to facilitate
    //the computation of the threshold we dont neet it anymore
    //then, remaps to (id1, id2) removing the weight
    val filteredEdges : RDD[Tuple2[String,String]] = edges.filter{case (a,(b, weight)) =>
      val tethaH = thetasPerNodeBrd.value //gets broadcasted hash
      val newT = (tethaH.get(a).get+ tethaH.get(b).get)/2.0
      weight >= newT && a < b}.map{case (a,(b, weight)) => (a,b)}
    filteredEdges.persist()
    println("pruned graph")

    //filteredEdges.take(10).foreach(println)
  return filteredEdges



  }


//  def processPair( A :Tuple2[String, Set[Tuple2[String,Int]]] , B: Tuple2[String, Set[Tuple2[String,Int]]]) :

  //uses index profile_id => blocks
  def createEdges2(profileIndex :RDD[Tuple2[String, Set[Tuple2[String,Int]]]], num_blocks : Int ): RDD[Tuple2[String,Tuple2[ String, Double]]] = {

    val profileIndexDS1 = profileIndex.filter{case x =>(x._1.startsWith("DS1")) } //gets profiles in dataset 1

    val profileIndexDS2 = profileIndex.filter{case x =>(x._1.startsWith("DS2")) } //gets profiles in dataset 2


    val clusterEntropies = spark.sparkContext.broadcast(PriClusterEntropies)
    val edges=  profileIndexDS1.cartesian(profileIndexDS2).flatMap{ case ((profileA, setBlocksA), (profileB, setBlocksB)) =>
       //if (profileA < profileB) {
       val sharedBlocks = setBlocksA.intersect(setBlocksB)
       if( sharedBlocks.size == 0 ) {
         None
       } else {
         //profileA = u , profileB = v
         val n11 = sharedBlocks.size
         val n12 = setBlocksA.size - n11
         //u has and not v
         val n1plus = setBlocksA.size //num blocks u is in
         val n2plus = num_blocks - n1plus //num blocks u is not in
         val nplus1 = setBlocksB.size
         val nplus2 = num_blocks - nplus1
         val n21 = setBlocksB.size - n11 //v has and not u
         val n22 = num_blocks - n11 - n12 - n21
         //neither have n22 = total num blocks - n11 -n12 -n21
         val mu11 = n1plus * nplus1 / num_blocks.toDouble

         val mu12 = n1plus * nplus2 / num_blocks.toDouble
         val mu21 = n2plus * nplus1 / num_blocks.toDouble
         val mu22 = n2plus * nplus2 / num_blocks.toDouble
         var xi = (n11 - mu11) / mu11
         xi = xi +(n12 - mu12) / mu12
         xi = xi + (n21 - mu21) / mu21
         xi = xi + (n22 - mu22) / mu22

         //val sharedBlocksEntropy = 0.0
         def popVal(s : Tuple2[String,Int]) :Double = {
          return clusterEntropies.value.getOrElse(s._2,0.0)
         }
         val sharedBlocksList= sharedBlocks.map(popVal).toList

         val sharedBlocksEntropy : Double =sharedBlocksList.sum
           //.sum / sharedBlocks.size
         val weight = xi * xi * sharedBlocksEntropy
         Some((profileA, (profileB, weight)))
       }

     }



    return edges

  }

}

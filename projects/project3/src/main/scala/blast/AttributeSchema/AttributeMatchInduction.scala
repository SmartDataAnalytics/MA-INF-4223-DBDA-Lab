package blast.AttributeSchema


import org.apache.spark.rdd.RDD

import scala.collection.mutable

class AttributeMatchInduction() {

  /* calculates attribute match induction*/
  def calculate(AP1 : AttributeProfile, AP2 : AttributeProfile) :  Seq[Tuple3[Int, List[String], List[String]]] = {


  //assembles into pairs
    val ds1 = AP1.getAttributeTokens
    val ds2 = AP2.getAttributeTokens
    val pairs = ds1.cartesian(ds2)
    val pairsNames  = pairs.map{case (a,b) => (a._1,b._1) }
    val pairsList : Seq[Tuple2[String,String]] = pairsNames.collect()

    //calculates similarities of pairs
    val sims : RDD[Tuple2[Tuple2[String,String],Double]]= pairs.map{case (a,b) => ((a._1,b._1),AttributeProfile.similarity(a._2,b._2)) }

    //maximum similarity for each attribute in dataset 1
    val maxDS1 = sims.map{case ((a,_),b) => (a, b)}.reduceByKey(math.max(_,_)).collectAsMap()
    //maximum similarity for each attribute in dataset 2
    val maxDS2 = sims.map{case ((_,a),b) => (a, b)}.reduceByKey(math.max(_,_)).collectAsMap()

    //get candidates pairs within alpha of maximum
    val alpha = 0.9
    val candidatesDS1 : RDD[Tuple2[Tuple2[String,String],Double]] = sims.filter{ case ((ds1at, ds2at), sim) => sim > alpha*maxDS1(ds1at)}
    val candidatesDS2 : RDD[Tuple2[Tuple2[String,String],Double]]= sims.filter{ case ((ds1at, ds2at), sim) => sim > alpha*maxDS2(ds2at)}
    //candidate pairs with their respective similarities ((attr1, attr2), sim)
    val candidatesWithSim :Set[Tuple2[Tuple2[String,String],Double]] = (candidatesDS1.collect() ++ candidatesDS2.collect()).toSet

    //finding connected edges

    //attribute names
    val attributesDS1 = maxDS1.keySet.toList
    val attributesDS2 = maxDS2.keySet.toList
    //attribute arbitrary unique cluster ids

    //maps attributes to cluster ids
    var clusterIdsDS1  = (1 to attributesDS1.size).toList
    var clusterIdsDS2 = (attributesDS1.size+1 to  attributesDS1.size+attributesDS2.size).toList
    //attribute names and indexes in tuples
    var attrNameIndexDS1 = (attributesDS1 zip (0 to attributesDS1.size-1 ).toList).toMap
    var attrNameIndexDS2 = (attributesDS2 zip (0 to attributesDS2.size-1 ).toList).toMap

    //finds connected components in graphs. assigns keys to each connected maximal subgraph.
    for( candidate <- candidatesWithSim ){
      val attrNameD1 = candidate._1._1
      val attrD1Cluster = clusterIdsDS1(attrNameIndexDS1(attrNameD1))
      val attrNameD2 = candidate._1._2
      val attrD2Cluster = clusterIdsDS2(attrNameIndexDS2(attrNameD2))

      val minCluster = math.min(attrD1Cluster, attrD2Cluster)
      //replace every instance of old cluster ids by minID
      clusterIdsDS1 = clusterIdsDS1.map{x => if (x== attrD1Cluster) minCluster else x}
      clusterIdsDS2 = clusterIdsDS2.map{x => if (x == attrD2Cluster) minCluster else x}

    }

    return remapClusters(attributesDS1, attributesDS2, clusterIdsDS1, clusterIdsDS2)

  }

  def entropies_from_clusters(AP1 : AttributeProfile, AP2 : AttributeProfile, list_clusters : Seq[Tuple3[Int, List[String], List[String]]]) : Map[Int, Double] ={

    val DS1Ent = AP1.getAttributeEntropies.collectAsMap()
    val DS2Ent = AP2.getAttributeEntropies.collectAsMap()

    def calculate_one_cluster(cluster : Tuple3[Int, List[String], List[String]]) : Double = {

      val entropy_attrds1 = cluster._2.map(x => DS1Ent.getOrElse(x,0.0)).sum
      val entropy_attrds2 = cluster._3.map(x => DS2Ent.getOrElse(x,0.0)).sum

      return ( entropy_attrds1 + entropy_attrds2 )/(cluster._2.size + cluster._3.size).toDouble
    }

    val entropies : Seq[(Int, Double)]  =  list_clusters.map{ case x => (x._1, calculate_one_cluster(x))}

    return entropies.toMap

  }

 def remapClusters(attributesDS1 : Seq[String], attributesDS2 : Seq[String], clusterIdsDS1 : Seq[Int], clusterIdsDS2 : Seq[Int] ) : Seq[Tuple3[Int, List[String], List[String]]]= {
   var clustersCount : Map[Int, Int]= (1 to attributesDS1.size+attributesDS2.size).toList.map{x => (x,0)}.toMap
   for(cId <- clusterIdsDS1) {
     clustersCount = clustersCount.updated(cId, clustersCount(cId)+1 )

   }

   for(cId <- clusterIdsDS2) {
     clustersCount = clustersCount.updated(cId, clustersCount(cId)+1 )
   }

   //removing empty clusters
   clustersCount = clustersCount.filter{case (k,v)=>v>0}

   //clusters of size 1 must be mapped to a special cluster (0)
   val singleAttrClustersTup = clustersCount.filter{case (k,v) => v==1}.keys.map{x => (x,0)}
   //clusters with more than one attribute :
   val multipleAttrClustersList = clustersCount.filter{case (k,v) => v>1}.keys
   val multipleAttrClustersTup = multipleAttrClustersList zip (1 to multipleAttrClustersList.size)

   //maps old cluster ids to new ones
   val newClusterIdsMap : Map[Int, Int]= (singleAttrClustersTup ++ multipleAttrClustersTup).toMap
   //modifies clusterIdsDSx to have new cluster ids. maps to (new cluster id, attrname)
   val clusterToAttrDS1 =  (clusterIdsDS1.map{x => newClusterIdsMap(x)} zip attributesDS1.map{x => List(x)}).groupBy(_._1).mapValues(seq => seq.reduce{(x,y) => (x._1, x._2 ++ y._2)}._2).toMap
   val clusterToAttrDS2 = (clusterIdsDS2.map{x => newClusterIdsMap(x)} zip attributesDS2.map{x => List(x)}).groupBy(_._1).mapValues(seq => seq.reduce{(x,y) => (x._1, x._2 ++ y._2)}._2).toMap
   //calculate output
   val clusterIds = newClusterIdsMap.values.toSet
   return  clusterIds.map{x => (x, clusterToAttrDS1.getOrElse(x,List()), clusterToAttrDS2.getOrElse(x,List()))}.toList

 }
}
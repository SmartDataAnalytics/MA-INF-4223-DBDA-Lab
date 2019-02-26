package net.sansa_stack.template.spark.rdf

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import java.time.LocalDateTime
import org.apache.spark.sql.SQLContext
import java.time.ZoneId

object finalTripleReader {
  val sc = new SparkContext("local[*]", "LoadBalancedMapReduce") 
  val kc = sc.broadcast(2)
  val threshold = sc.broadcast(0.2)
  val partno = sc.accumulator(0)
  var flag = 1
  val sqlContext = new SQLContext(sc)
  
  def main(args: Array[String]) {
    //preprocessing steps for DataSet
    println("-------- Start of Data Preprocessing --------")
    val preprocessingTimeA = LocalDateTime.now()
    println(preprocessingTimeA)
    val dataset = dataPreprocessing()
    val preprocessingTimeB = LocalDateTime.now()
    println(preprocessingTimeB)
    println("Preprocessing Time :" + (preprocessingTimeB.atZone(ZoneId.systemDefault()).toEpochSecond() - preprocessingTimeA.atZone(ZoneId.systemDefault()).toEpochSecond()))
    println("-------- End of Data Preprocessing --------")
    println()
    //println("Entities and blocking keys")
    //dataset.foreach(println)
    //println()
    
    //Combination Based Blocking
    println("-------- Start of Combination Based Blocking --------")
    val combinationTimeA = LocalDateTime.now()
    println(combinationTimeA)
    val combinationResults = combinationBasedBlocking(dataset)
    val combinationTimeB = LocalDateTime.now()
    println(combinationTimeB)
    println("Combination Based Blocking Time :" + (combinationTimeB.atZone(ZoneId.systemDefault()).toEpochSecond() - combinationTimeA.atZone(ZoneId.systemDefault()).toEpochSecond()))
    println("-------- End of Combination Based Blocking --------")
    println()
    //println("Result from Combinations Based Blocking Phase:")
    //combinationResults.foreach(println)
    //println()
    
    //Load-Balanced Matching
    println("-------- Start of Load-Balanced Matching --------")
    val loadTimeA = LocalDateTime.now()
    println(loadTimeA)
    val result = loadBalancedMatching(combinationResults,dataset)
    val loadTimeB = LocalDateTime.now()
    println(loadTimeB)
    println("Load-Balanced Matching Time :" + (loadTimeB.atZone(ZoneId.systemDefault()).toEpochSecond() - loadTimeA.atZone(ZoneId.systemDefault()).toEpochSecond()))
    println("-------- End of Load-Balanced Matching --------")
    println()
    
    //Printing the Results
    println("Result:")
    result.foreach(x => {println(x._1.toString + ","  + x._2.toString)})

  } 
  
  def dataPreprocessing() : RDD[(String,List[String])] = {
    
    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag","NEW_HEADER").load("/main/resources/inputDataset.xml")
    df.createOrReplaceTempView("articles")
    val sqlDF = sqlContext.sql("SELECT pubnum,title FROM articles")
    val data = sqlDF.rdd
    val stopWords = List( "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "it", "it's", "its", "itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll", "she's", "should", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've", "were", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves" )
    val parsedDataset = data.map(x => (x(0), x(1))).filter(x => x._1 !=null && x._2 !=null)
    val dataset = parsedDataset.map(x => (x._1.toString(), x._2.toString.split(" ").toList))
    val finalDataset = dataset.map(x => (x._1,x._2.filter(!stopWords.contains(_)))).map(x => {
      x._2.foreach(s => s.replaceAll("""[\p{Punct}&&[^.]]""", ""))
      (x._1,x._2) 
      })
    /*val lines = sc.textFile("/main/resources/sample.txt")
    val firstRdd = lines.map(x => {
     val word = x.split("-")
     (word(0),word(1))
    })
    val secondRdd = firstRdd.groupByKey()
    val finalDataset = secondRdd.map(x => (x._1, x._2.toList))*/
    return finalDataset
  }
  
  
  def combinationBasedBlocking(dataset : RDD[(String,List[String])]) : RDD[(List[String],List[String])] = {
    val sortedInputs = dataset.sortBy(_._1).map(x => (x._2,x._1))
    val combinationMapResults = sortedInputs.flatMap(parseCombinationMap)
    val combinationReduceResults = combinationMapResults.reduceByKey((x,y) => x+"@//@"+y)
    val finalCombinationResults = combinationReduceResults.map(x => (x._1, x._2.split("@//@").toList)).filter(x => x._2.size > 1)
    return finalCombinationResults

  }
  
  def loadBalancedMatching(combinationResults:RDD[(List[String],List[String])],dataset:RDD[(String,List[String])]):RDD[(String,String)]={
    val loadMapResults1 = combinationResults.flatMap(parseLoadMap)
    val loadMapResults = loadMapResults1.distinct()
    //println("Result from map:")
    //loadMapResults.take(5).foreach(println)
 
    val parseloadMapUniqueResults = loadMapResults.map(f => {
      val x = f.mkString(",")
      val y = x.split(",")
      (y(0), y(1))
    })
    val combineWithPart = parseloadMapUniqueResults.map(f => {
      partno += 1
      (partno.localValue, (f._1, f._2))
    })
    combineWithPart.persist()
    
    val oddPartitioner = combineWithPart.filter(x => (x._1 % 2 != 0))
    val evenPartitioner = combineWithPart.filter(x => (x._1 % 2 == 0))
    val oddResult = jaccardSimilarity(oddPartitioner,dataset)
    val evenResult = jaccardSimilarity(evenPartitioner,dataset)
    val finalOddResult = oddResult.filter(x => (x._1 != "null"))
    val finalEvenResult = evenResult.filter(x => (x._1 != "null"))
    val results = finalOddResult.union(finalEvenResult)

    return results

  }
  
  def jaccardSimilarity (partitionedData : RDD[(Int,(String,String))], dataset: RDD[(String,List[String])]) : RDD[(String,String)]={
    val datasetMap= dataset.collect.toMap
    val result = partitionedData.map(x => {
      if(datasetMap.contains(x._2._1) && datasetMap.contains(x._2._2)) {
        val y = (datasetMap.get(x._2._1).get, datasetMap.get(x._2._2).get)
        val a = y._1.intersect(y._2).distinct
        val b = y._1.union(y._2).distinct
        val resVal = ((a.length.toDouble / b.length.toDouble)).toDouble
        if(resVal > threshold.value)
          (x._2._1, x._2._2) 
        else
          ("null", "null")
      }
      else
        ("null", "null")
    })
    return result

  }
  
  def parseLoadMap(mapResults : (List[String], List[String])) : List[List[String]] = {
    val newRdd = mapResults._2.combinations(2)
    var parseMap:List[List[String]] = List()
    newRdd.foreach(x => { 
      parseMap = parseMap :+ x 
    })
    return parseMap.distinct

  }
  
  
  def parseCombinationMap(line : (List[String],String)):  List[(List[String], String)] = {
    val newRdd = line._1.combinations(kc.value)
    var parseMap : List[(List[String],String)] = List()
    newRdd.foreach(x => { val temp1 = (x, line._2) 
      parseMap = temp1 +: parseMap
      })
    return parseMap

  }

}


package entity_matching

import org.apache.jena.riot.Lang
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StopWordsRemover

//imports needed for Stanford CoreNLP Lemmatizer
import java.util.List
import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.util.CoreMap
import scala.collection.convert.wrapAll._


object EntityMatching {
  
 case class Triples(ID: Long, subject: String, predicate: String, `object`: String)
 case class Block(tokenID: Long, token: String, entitiyIDsKG1: Seq[Int], entitiyIDsKG2: Seq[Int], numBlockComparison : Int )

 def main(args: Array[String]) {
  
  //Define the configurations for spark session
  val spark = SparkSession.builder
              .appName(s"Entity Matching")
              .master("local[*]")
              .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .config("spark.executor.memory", "2g")
              .config("spark.driver.memory", "2g")
              .config("spark.executor.cores",4)
              .config("spark.default.parallelism",4)
              .config("spark.kryoserializer.buffer.max", "512m") 
              .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  
         
  //Needed for dataframe creation
  import spark.implicits._

  //Input the RDFXML files for KG1, KG2 and the true matches
  val input1 : String = "src/main/resources/Marvel.xml"
  val input2 : String = "src/main/resources/DC.xml"
  val filePathTrueMatches = "src/main/resources/Marvel_DC.xml"
 
  /*val input1 : String = "src/main/resources/MemoryAlpha.xml"
  val input2 : String = "src/main/resources/MemoryBeta.xml"
  val filePathTrueMatches = "src/main/resources/MemoryAlpha_MemoryBeta.xml"*/
  
  /*val input1 : String = "src/main/resources/DarkScape.xml"
  val input2 : String = "src/main/resources/OldSchoolRuneScape.xml"
  val filePathTrueMatches = "src/main/resources/DarkScape_OldSchoolRuneScape.xml"*/
  
  //Reading the input KG1 and adding indices to each row
  val lang = Lang.RDFXML
  val dataset1 = spark.rdf(lang)(input1).zipWithIndex()

  
  //The last ID of KG1 is used to distinguish if a triple belongs to KG1 or KG2, based on its ID
  val sizeDataset1 = dataset1.count() 
  val lastIDDataset1 = sizeDataset1-1                             
  
  //Reading the input KG2 and adding indices to each row
  val dataset2 = spark.rdf(lang)(input2).zipWithIndex()
                                        .map {case (rec, index) =>  (rec, index + lastIDDataset1 + 1)}  
  val sizeDataset2 = dataset2.count()
  
  //Merging KG1 and KG2
  val mergedTriples = (dataset1 ++ dataset2).map{case (rec, index) => Triples(index, rec.getSubject.toString(), rec.getPredicate.toString(), rec.getObject.toString())}

  //The last ID of the merged KG (same as the last ID of KG2)
  val lastIDMerged = sizeDataset1 + sizeDataset2 - 1
  
  //Converting the merged KG into a dataframe
  val mergedTriplesDF = mergedTriples.toDF("ID","Subject","Predicate","Object")
  
  //Reading the RDFXML containing true matches
  val trueResults = spark.rdf(lang)(filePathTrueMatches)
                         .map{rec => (rec.getSubject.toString(), rec.getPredicate.toString(), rec.getObject.toString())}
  
  //Extracting the names of the true matched entities
  val trueEntityMatchesNames = GetTrueEntityMatchesNames(trueResults)
  

  //Get the triples corresponding to the the true matched entities (either as a Subject or an Object)
  val trueMatchedEntityTriples = GetTrueEntityMatchesTriples (trueEntityMatchesNames, mergedTriplesDF, spark) 
  
  //Get a fraction of the complete dataset (filtered for token blocking)
  //and the maximum possible entity matches that can be predicted (for block purging)
  val (mergedTriplesFiltered,maxPossibleEntityMatches)  = GetFractionOfDataset(dataset1, dataset2, trueMatchedEntityTriples)
  
  mergedTriplesFiltered.cache()
  
  //Get the set of triple indices corresponding to KG1 and KG2
  val firstDatasetIDs = 0 to lastIDDataset1.toInt toSet
  val secondDatasetIDs = (lastIDDataset1.toInt + 1) to (lastIDMerged.toInt) toSet
   
  //Call the TokenBlocking procedure to get blocks of Entity IDs that share a token
  val tokenBlocks = TokenBlocking(mergedTriplesFiltered, firstDatasetIDs, secondDatasetIDs, maxPossibleEntityMatches)
  
  //Block Split
  val newTokenBlocks = BlockSplit(tokenBlocks).repartition(8)

  //Generate candidate pairs of entities from each block
  val candidates = CandidateGeneration(newTokenBlocks)
    
  //Create a dataframe of the merged KGs for joining
  val mergedTriplesFilteredDF = mergedTriplesFiltered.toDF("ID","Subject","Predicate","Object")
  
  mergedTriplesFiltered.unpersist()
  mergedTriplesFilteredDF.cache()
  
  //Create a map for ID, Subject, Predicate values for each candidate entity to be used in subsequent filtering and matching processes
  val candidatesMap = GenerateCandidateEntityMap(candidates, mergedTriplesFilteredDF, spark)
  
  //Filter out candidate entity pairs that do not have the same predicate (for the object literal that they have in common)
  //Filter out candidate entity pairs that do not have the same subject category (Ex. resource, property, class etc.)
  val filteredCandidates = FilteringCandidates(candidates, candidatesMap)
  
  //Matching candidate entity pairs based on high similarity values (Jaccard Index for respective Subject URIs)
  val predictedEntityMatchesNames = MatchingCandidates(filteredCandidates, candidatesMap, spark)

  println("\n\n-----PREDICTED MATCHES-----\n")
  predictedEntityMatchesNames.foreach(println(_))
  println("\tNumber Of Predicted Entity Matches : " + predictedEntityMatchesNames.count())
  
  println("\n\n-----TRUE MATCHES-----\n")
  trueEntityMatchesNames.foreach(println(_))
  println("\tNumber Of True Entity Matches : " + trueEntityMatchesNames.count())
  //trueEntityMatchesNames.foreach(println(_))
  
  //Calculate and print evaluation metrics
  Evaluation(predictedEntityMatchesNames,trueEntityMatchesNames)
  
  spark.stop

}//End of main()



 
 /*
 * Used by BlockSplit()
 * Finds the smallest integer value to split blocks larger than the average size
 * @param setOfEntitiesSize1 : Number of entities in the block from KG1
 * @param setOfEntitiesSize2 : Number of entities in the block from KG2
 * @param avgBlockSize : Average block size
 * @return : An integer value 
 */
 implicit def findDivisor(setOfEntitiesSize1 : Int, setOfEntitiesSize2 : Int, avgBlockSize : Double) : Int = {
    
    var value = 1
    do(value = value + 1) while((setOfEntitiesSize1 * setOfEntitiesSize2)/value.toDouble > avgBlockSize )
    value
  }
 
 
 
 
 
/*
 * Splits blocks larger than the average block size into (almost) equal sized blocks
 * This is to remove any skewness in the block sizes
 * @param tokenBlocks : RDD of Blocks
 * @return : A new RDD of Blocks with no block size larger than the average block size of the input tokenBlocks RDD
 */
def BlockSplit (tokenBlocks : RDD[Block]) : RDD[Block] = {
  
  
  //Find the average number of potential comparisons in each block 
  //This will be referred to as 'block size' rather the number of entityIDs in each block
  val blockComparisons = tokenBlocks.map(block => block.numBlockComparison).collect()
  val sumBlockComparisons = blockComparisons.sum.toDouble
  val avgNoBlockComparisons = sumBlockComparisons/(blockComparisons.length).toDouble
  
  //Filter out blocks that have sizes lesser than the average value
  val smallBlocks = tokenBlocks.filter(block => block.numBlockComparison <= avgNoBlockComparisons)

  //Split the larger blocks into (almost) equal sized blocks so that none of the split blocks have sizes larger than the average value
  val largeBlocks = tokenBlocks.filter(block => block.numBlockComparison > avgNoBlockComparisons)
                               .flatMap(block => {
                                
                                    val value = findDivisor(block.entitiyIDsKG1.size, block.entitiyIDsKG2.size, avgNoBlockComparisons)
                                
                                    if (block.entitiyIDsKG1.size <= block.entitiyIDsKG2.size) {
                                  
                                       //val noOfSplits = Math.ceil(block.entitiyIDsKG2.size.toDouble/value).toInt
                                       val noOfSplits = block.entitiyIDsKG2.size/value
                                       block.entitiyIDsKG2.grouped(noOfSplits)
                                                          .map(splitEntityIDs =>
                                                             Block(block.tokenID, block.token, block.entitiyIDsKG1, splitEntityIDs, block.entitiyIDsKG1.size * splitEntityIDs.size))
                                    }
                                    else {
                                       //val noOfSplits = Math.ceil(block.entitiyIDsKG1.size.toDouble/value).toInt
                                       val noOfSplits = block.entitiyIDsKG1.size/value
                                       block.entitiyIDsKG1.grouped(noOfSplits)
                                                          .map(splitEntityIDs =>
                                                            Block(block.tokenID, block.token, splitEntityIDs, block.entitiyIDsKG2, block.entitiyIDsKG2.size * splitEntityIDs.size))
                                   }
                                 })
 
 //Create a new merged RDD of blocks
 val newTokenBlocks = smallBlocks ++ largeBlocks

 newTokenBlocks

}
 

 
/*
 * Used by GetTrueEntityMatchesTriples()
 * Extracts true entity matches of the form (Entity Name 1, Entity Name 2) from trueResults RDD 
 * @param trueResults : RDD of  RDF tuples (subject, predicate, object) as read from the true results file
 * @return : RDD of the form (Entity Name 1, Entity Name 2) from either datasets
 */
 def GetTrueEntityMatchesNames (trueResults : RDD[(String, String, String)] ) : RDD[(String,String)] = {
     
   trueResults.map(x => x)
              .groupBy(x => x._1)
              .values
              .filter(x => hasEqualToRelation(x))
              .map(x => x.toArray.filter(y => y._2.equals("http://knowledgeweb.semanticweb.org/heterogeneity/alignmententity1") ||
                                              y._2.equals("http://knowledgeweb.semanticweb.org/heterogeneity/alignmententity2")))
              .map{x => (x(0)._3, x(1)._3)}
 }
 
 
 
 
 
 /*
 * Used by GetTrueEntityMatchesNames()
 * Used to check if a triple from the true results RDFXML file contains a "=" object corresponding to the "alignmentrelation" predicate. 
 * Such triples will be used to further extract the entity names that match across KG1 and KG2
 * @param iterableOfTriples : Iterable of the form (Subject, Predicate, Object) 
 * @return : TRUE if there is a "=" object corresponding to the "alignmentrelation" predicate, FALSE otherwise
 */
def hasEqualToRelation(iterableOfTriples : Iterable[(String, String, String)]) : Boolean = {
  
  if(iterableOfTriples.count(x => x._2.equals("http://knowledgeweb.semanticweb.org/heterogeneity/alignmentrelation") && x._3.equals("\"=\"")) > 0)
    return true
  else
    return false
}

 


/*
 * Extracts the triples of the true entity matches to be included in the reduced dataset 
 * @param trueEntityMatches : RDD of the form (Entity Name 1, Entity Name 2) from either datasets, containing true entity matches
 * @param mergedTriplesDF : Dataframe of the two merged datasets
 * @param spark : SparkSession object required to call .toDF()
 * @return : RDD of triples corresponding to the entities that have true matches
 */
 def GetTrueEntityMatchesTriples (trueEntityMatches : RDD[(String, String)] , mergedTriplesDF : DataFrame, spark : SparkSession ) : RDD[Triples] = {
     
  import spark.implicits._
  val trueEntityMatchesList = trueEntityMatches.map(x => x._1) ++ trueEntityMatches.map(x => x._2)
  val trueEntityMatchesDF = trueEntityMatchesList.toDF("EntityNames")
  val triples = mergedTriplesDF.as("d1")
                               .join(broadcast(trueEntityMatchesDF).as("d2"), $"d1.Subject" === $"d2.EntityNames" || $"d1.Object" === $"d2.EntityNames" ,"left_semi")
                               .rdd
                               .map(x => Triples(x.getAs[Long]("ID"),x.getAs[String]("Subject"),x.getAs[String]("Predicate"),x.getAs[String]("Object")))
 triples
 }

 
 
 
 
 /*
 * Extracts a fraction of the complete merged KG (KG1 + KG2)
 * @param dataset1 : RDD of triples with index (KG1)
 * @param dataset2 : RDD of triples with index (KG2)
 * @param trueMatchedEntityTriples : RDD of triples for only entities have true matches
 * @param percentageOfData : The percentage of the original KGs to use (in decimal). Default = 0.01  
 * @param useOnlyTrueMatches : If FALSE, use both randomly picked triples and triples corresponding to entities with true matches
 * 																TRUE, use only randomly picked triples
 * 														 Default = FALSE
 * @param numPartitions : Default = 4
 * @return : Merged, reduced, filtered RDD of triples 
 * 					 The maximum no. of entity matches possible (size of the smaller KG)
 */
def GetFractionOfDataset (dataset1 : RDD[(Triple,Long)], dataset2 : RDD[(Triple,Long)], trueMatchedEntityTriples : RDD[Triples], percentageOfData : Double = 0.01, useOnlyTrueMatches : Boolean =  false, numPartitions : Int = 4)   : (RDD[Triples], Long) = {
  
  val (mergedTriples,maxPossibleEntityMatches) = useOnlyTrueMatches match {
    
    //Use only the triples corresponding to true entity matches
    case true => (trueMatchedEntityTriples, trueMatchedEntityTriples.count)
    
    //Use the triples corresponding to true entity matches and randomly select a percentage of triples from KG1 and KG2
    case false => {
      
        //To randomly select equal proportion of triples from KG1 and KG2 
        val dataset1Reduced = dataset1.sample(false, percentageOfData)
        val dataset2Reduced = dataset2.sample(false, percentageOfData)

        val dataset = (
                        trueMatchedEntityTriples ++ 
                        (dataset1Reduced ++ dataset2Reduced).map{case (rec, index) => Triples(index, rec.getSubject.toString(), rec.getPredicate.toString(), rec.getObject.toString())}
                      ).distinct
        (dataset, Math.min(dataset1Reduced.count,dataset2Reduced.count))
    }
      
  }

  //For token blocking, filter out all triples having another entity as object and triples having numeric literals as objects
  //This ensures only textual and alphanumeric literals are used for token blocking
  //Increase paritions from 2 to 4
  val mergedTriplesFiltered = mergedTriples.filter(x => !x.`object`.startsWith("http://"))
                                           .filter(x => !x.`object`.contains("http://www.w3.org/2001/XMLSchema#integer"))
                                           .repartition(numPartitions)
                                           
  (mergedTriplesFiltered,maxPossibleEntityMatches) 
}





/*
 * Given a RDD of Triples with tuples (Entity ID, Subject, Predicate, Object),
 * generates a RDD of Blocks with tuples (Token ID, TokenName, List of entityIDs)
 * @param triples : RDD of Triples
 * @param firstDatasetIDs : Set of entity IDs belonging to the first dataset
 * @param secondDatasetIDs : Set of entity IDs belonging to the first dataset
 * @param maxPossibleEntityMatches : Maximum no. of predicted entity matches possible
 * @param fraction : fraction of maxPossibleEntityMatches that will be used as a threshold for block purging
 * @param keepNumericLiterals : whether or not to include numeric literals during token blocking. Default = true
 * @return : a RDD of tuples (EntityID1, EntityID2) which correspond to the IDs of matched entities from either dataset
 */
def TokenBlocking(triples : RDD[Triples], firstDatasetIDs : Set[Int], secondDatasetIDs : Set[Int], maxPossibleEntityMatches : Long, fraction : Double = 0.02, keepNumericLiterals : Boolean = true) : RDD[Block] = {
  
  
  //For each entity, return the list of tokens
  val triplesIDObject = triples.map {x => (x.ID , x.`object`)}
  val tokensPerEntity = TokenizeObjLiterals(triplesIDObject, keepNumericLiterals)  
 
  //Split the token string by separator ('|'). For each token, return (Token, Entity ID)
  val tokenEntityIDTuple = tokensPerEntity.map{case (id, tokenString) => 
                                                     tokenString.split("\\|")
                                                                .map(token => (token,id))
                                              }.flatMap(x=>x)
  

  //GroupByKey with token name as key and return (Token, List of entity IDs) if it has more than one entity ID matched to it                                           
  val entityIDsPerToken = tokenEntityIDTuple.groupByKey(numPartitions = 4).filter(_._2.size > 1)
  //val entityIDsPerToken = tokenEntityIDTuple.map {case (x,y)=>(x, List(y))}.reduceByKey(_++_).filter(_._2.size > 1)
  

  //Clean-Clean ER : Keep only those tokens that have Entity IDs from both KGs
  //Block purging : Remove blocks bigger than a certain size. In this case a fraction of the maximum possible entity matches
  val cleanEntityIDsPerToken = entityIDsPerToken.map{case(token,entityIDs)  => 
                                                            {
                                                              val entityIDsSet = entityIDs.map(x => x.toInt).toSet
                                                              val firstDatasetEntities = entityIDsSet.intersect(firstDatasetIDs)
                                                              val secondDatasetEntities = entityIDsSet.diff(firstDatasetEntities)
                                                              (token,(firstDatasetEntities, secondDatasetEntities))
                                                            }
                                                       }//This splits the entityIDs list into entityIDs from either KG
                                                .filter{case(token,(entityIDsKG1, entityIDsKG2)) => 
                                                            {
                                                              (entityIDsKG1.size != 0 && entityIDsKG2.size != 0) && 
                                                              (Math.min(entityIDsKG1.size, entityIDsKG2.size) < fraction*maxPossibleEntityMatches)
                                                            }
                                                       }
 
 
  //Create tokenIDs for the blocks that remain. Tuples formed will be of the form (TokenID, Token, EntityIDsKG1, EntityIDsKG2, Maximum number of possible comparisons)
  val tokenBlock = cleanEntityIDsPerToken.zipWithIndex().map{case ((token,(entityIDsKG1, entityIDsKG2)),index) => Block(index, token, entityIDsKG1.toSeq, entityIDsKG2.toSeq, entityIDsKG1.size*entityIDsKG2.size)}
  
  tokenBlock
  
}




   
/*
 * Used by TokenBlocking()
 * Given a RDD of tuples (entity ID, literal as a string),
 * generates the tokenized string for the literal
 * The following steps are performed:
 * 1. Convert all literal strings to lowercase
 * 2. Split the literals by whitespace/underscore/punctuation into individual tokens
 * 3. Remove stop words (English)
 * 4. Remove extra spaces
 * 5. Remove single characters
 * 6. Remove duplicated tokens in a single object string
 * 7. Lemmatize tokens
 * @param idObject : RDD of tuple (entity ID, object literal)
 * @param keepNumericLiterals : whether or not to include numeric literals during token blocking
 * @return : RDD of tuple (Entity ID, Set of tokens separated by '|' as a string)
 */
def TokenizeObjLiterals(idObject : RDD[(Long, String)], keepNumericLiterals : Boolean) : RDD[(Long, String)] = {
  
  //Load the stopwords and add a few recurring (but insignificant) words into the stopwords list
  val stopwords = StopWordsRemover.loadDefaultStopWords("english")
  
  //Perform necessary filtering for generating significant tokens out of each literal input
  val filters =     
    idObject.map{case (id, lit) => 
                (id, lit.toLowerCase()
                        .replaceAll("-", "")
                        .split("[\\W_]")
                        .filter(_.trim.length > 1)
                        .filter(!stopwords.contains(_))
                        .distinct
                 )}
 
  //Keep numeric literals
  if(keepNumericLiterals) {
    filters.map {case (id, lit) => 
                (id, LemmatizeString(lit.mkString(" "))
                )}
  }
  //Remove numeric literals
  else {
    filters.map {case (id, lit) => 
                (id, LemmatizeString(lit.filter(!_.matches("[0-9]+")).mkString(" "))
                )}    
  }

}





/*
 * Used by TokenizeObjLiterals()
 * Takes a string and converts each token into its lemmatized version
 * Uses the lemmatizer from Stanford CoreNLP library
 * @param string : The string to be lemmatized
 * @return : Lemmatized string
 */
def LemmatizeString (string : String) : String = {

        val props : Properties = new Properties()
        props.put("annotators", "tokenize, ssplit, pos, lemma")
        val pipeline : StanfordCoreNLP = new StanfordCoreNLP(props)
        val sentence : Annotation  = pipeline.process(string)
        val tokens = sentence.get(classOf[SentencesAnnotation])
                             .flatMap(_.get(classOf[TokensAnnotation]))
        val lemmas = tokens.map(_.get(classOf[LemmaAnnotation]))
                           .mkString("|")
        lemmas     
 }
 



/*
 * Given an RDD of Blocks with tuples (TokenID, TokenName, List of entityIDs from KG1, List of entityIDs from KG2),
 * generates potential entity matches  
 * @param tokenBlocks : RDD of Blocks
 * @param numPartitions : Default = 4
 * @return : a RDD of tuples (Entity ID 1, Entity ID 2) which correspond to the indices of candidate pairs of entities from either KGs
 */
def CandidateGeneration(tokenBlocks : RDD[Block], numPartitions : Int = 4 ) : RDD[(Int,Int)] = {
  
  //Create pairwise list of entity IDs that have a common token. Generates tuples of the form (Entity ID 1, Entity ID 2) where Entity ID 1 belongs to KG1 and Entity ID 2 belongs to KG2
  val candidates = tokenBlocks.map {x =>
                                           x.entitiyIDsKG1.flatMap(y => x.entitiyIDsKG2.map(z => (y,z)))
                                            
                                   }.flatMap(x=>x)
                                    .distinct()
  
  candidates
}



/*
 * Given an RDD of candidate entity IDs (Entity ID 1, Entity ID 2),
 * generates a map of key-value pairs
 * where, each key is an entity ID and values are a few transformations done on the respective Subject and Predicate (required for filtering and matching processes) 
 * @param candidates : RDD of candidate entity ID pairs
 * @param mergedTriplesDF : Merged KG as a dataframe
 * @param spark : Spark Session object (needed for dataframe handling)
 * @return : Map of key-value pairs (Entity ID 1, (Subject, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject)) 
 */
def GenerateCandidateEntityMap (candidates : RDD[(Int,Int)], mergedTriplesDF : DataFrame, spark : SparkSession) : Map[Long,(String,String,String,String,String)] = {
  
  import spark.implicits._
  
  //Get distinct entity IDs from the RDD of entity ID pairs
  val listOfEntityIDs = (candidates.map{case(x,y) => x} ++ candidates.map{case(x,y) => y}).distinct()
  
  //Convert the list of entity IDs to a dataframe
  val listOfEntityIDsDF = listOfEntityIDs.toDF("ID")
  
  val candidatesMap = mergedTriplesDF.as("d1")
                                     .join(broadcast(listOfEntityIDsDF).as("d2"), $"d1.ID" === $"d2.ID","left_semi")
                                     .select($"ID", $"Subject", $"Predicate")
                                     .collect
                                     .map(x => {
                                         val id = x.getAs[Long]("ID")
                                         val subject = x.getAs[String]("Subject")
                                         val predicate = x.getAs[String]("Predicate")
                                  
                                         //Extract the category and sub-category from the Subject (for category filtering)
                                         val partsOfSubject = subject.split('/')
                                         val typeOfSubject = partsOfSubject(4) //for resource, property etc.
                                         val subtypeOfSubject = {
                                            if(partsOfSubject(5).contains(":"))
                                               partsOfSubject(5).split(':')(0) //for resource/Category: etc.
                                            else
                                               ""
                                          }
                                  
                                         //Extract the relationship name from the Predicate URI (for predicate filtering)
                                         val filteredPredicate = predicate.substring(predicate.lastIndexOf('/') + 1)
                                  
                                         //Extract the entity name from the Subject URI (for URI Similarity Values)
                                         /*val filteredSubject = subject.split('/')(5)
                                                                        .toLowerCase()
                                                                        .replaceAll("\\s|[^a-zA-Z0-9]","")*/
                                         val filteredSubject = {
                                           if(partsOfSubject.length > 6)
                                             partsOfSubject(5).concat(partsOfSubject(6))
                                           else
                                             partsOfSubject(5)
                                         }.toLowerCase()
                                          .replaceAll("\\s|[^a-zA-Z0-9]","")
                                                               
                                         (id,(subject,filteredPredicate,filteredSubject,typeOfSubject,subtypeOfSubject))
                                     })
                                     .toMap
  candidatesMap
}



/*
 * Given an RDD of candidate entity IDs (Entity ID 1, Entity ID 2), filters out candidate pairs that do not have:
 * 1. The same predicate (for the object literal that they have in common)
 * 2. The same subject category (Ex. resource, property, class etc.)
 * @param candidates : RDD of candidate entity ID pairs
 * @param candidatesMap : Map of key-value pairs (Entity ID 1, (Subject URI, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject)) 
 * @return : RDD of tuples (Entity ID 1, Entity ID 2) which correspond to the filtered candidate entity pairs from either KG
 */
def FilteringCandidates (candidates : RDD[(Int,Int)], candidatesMap : Map[Long,(String,String,String,String,String)]) : RDD[(Int,Int)] = {
                                   

  //Filter out all candidate pairs that do not have a matching predicate for the common token literal
  val candidatesFilteredPredicate = FilterPredicates (candidates, candidatesMap)
  
  //Filter out all candidate pairs that do not have a matching category (based on the entity name URI)
  val candidatesFilteredCategory = FilterCategories (candidatesFilteredPredicate, candidatesMap)
  
   
  candidatesFilteredCategory
}



/*
 * Given an RDD of (filtered) candidate entity IDs (Entity ID 1, Entity ID 2), find entity matches based on similarity score
 * @param candidates : RDD of candidate entity ID pairs
 * @param candidatesMap : Map of key-value pairs (Entity ID 1, (Subject URI, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject)) 
 * @param similarityThreshold : Minimum similarity score required to declare a candidate pair, a match. Default = 0.45
 * @param spark : Spark Session object (needed for dataframe handling)
 * @return : RDD of tuples (EntityName 1, EntityName 2) which correspond to the predicted entity matches from either KG
 */
def MatchingCandidates (candidates : RDD[(Int,Int)], candidatesMap : Map[Long,(String,String,String,String,String)], spark : SparkSession, similarityThreshold : Double = 0.45) : RDD[(String,String)] = {
    
  
  //Calculating Jaccard Similarity for the remaining candidate pairs
  val candidatesSimilarityValues = URISimilarity_Jaccard (candidates, candidatesMap)
   
  //Sorting the candidate pairs in decreasing order of similarity values
  //Keeping only the candidate pairs that have similarity value beyond a certain threshold (maximum similarity value = 0.5)
  val sortedCandidates = candidatesSimilarityValues.sortBy(- _._2)
                                                   .filter(_._2 > similarityThreshold)
                                                   .collect()

  //Generating tuples (Entity ID 1, Entity ID 2) of predicted entity matches based on highest similarity values
  val entityMatchesIDs = spark.sparkContext.parallelize(sortedCandidates.map(_._1))                                       

  //Resolving the entity IDs into names for the predicted entity matches            
  val predictedEntityMatchesNames = GetPredictedEntityMatchesNames(entityMatchesIDs, candidatesMap).distinct() 
  
  predictedEntityMatchesNames 
}
  




/*
 * Extracts predicted entity matches of the form (Entity Name 1, Entity Name 2) 
 * @param entityMatchesIDs : RDD of  predicted entity tuples (Entity ID 1, Entity ID 2)
 * @param candidatesMap : Map of key-value pairs (Entity ID 1, (Subject URI, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject))
 * @return : RDD of tuples (EntityName 1, EntityName 2) which correspond to the predicted entity matches from either KG
 */
def GetPredictedEntityMatchesNames (entityMatchesIDs : RDD[(Int,Int)], candidatesMap : Map[Long,(String,String,String,String,String)]) : RDD[(String,String)] = {

  //Get entity names for each of the entity ID pairs
  val predictedEntityMatchesNames = entityMatchesIDs.map{case(x,y) => (
                                                                candidatesMap.get(x).map(_._1).getOrElse(""),
                                                                candidatesMap.get(y).map(_._1).getOrElse("")
                                                              )}
  predictedEntityMatchesNames
}



/*
 * Used by MatchingCandidates()
 * Filters out all candidate entity pairs that have differing relationship values (corresponding to the common object literal) 
 * @param candidates : RDD of candidate entity ID pairs
 * @param candidatesMap : Map of key-value pairs (Entity ID 1, (Subject URI, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject))
 * @return : RDD of tuples (Entity ID 1, Entity ID 2) which correspond to the filtered candidate entity pairs from either KG
 */
def FilterPredicates (candidates : RDD[(Int,Int)], candidatesMap : Map[Long,(String,String,String,String,String)]) : RDD[(Int,Int)] = {
  
  val filteredCandidates = candidates.filter{case(x,y) => (
                                                            candidatesMap.get(x).map(_._2).getOrElse("") == 
                                                            candidatesMap.get(y).map(_._2).getOrElse("")
                                                          )}
  filteredCandidates
}





/*
 * Used by MatchingCandidates()
 * Filters out all candidate entity pairs that have differing subject categories (Ex. property, class, resource etc.) or sub-categories (Ex. actors)  
 * @param candidates : RDD of candidate entity ID pairs
 * @param candidatesMap : Map of key-value pairs (Entity ID 1, (Subject URI, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject))
 * @return : RDD of tuples (Entity ID 1, Entity ID 2) which correspond to the filtered candidate entity pairs from either KG
 */
def FilterCategories (candidates : RDD[(Int,Int)], candidatesMap : Map[Long,(String,String,String,String,String)]) : RDD[(Int,Int)] =  {
  
  val filteredCandidates = candidates.filter{case(x,y) => {
                                                  val x_categories = candidatesMap.get(x).map{case(a,b,c,d,e) => (d,e)}
                                                  val y_categories = candidatesMap.get(y).map{case(a,b,c,d,e) => (d,e)}
                                                  val x_subjectType = x_categories.map(_._1).getOrElse("")
                                                  val y_subjectType = y_categories.map(_._1).getOrElse("")
                                                  val x_subjectSubType = x_categories.map(_._2).getOrElse("")
                                                  val y_subjectSubType = y_categories.map(_._2).getOrElse("")
                                                  (x_subjectType == y_subjectType) && (x_subjectSubType == y_subjectSubType)
                                            }}
  filteredCandidates
}




/*
 * Calculates the Similarity Value of candidate entity pairs based on the Subject URI using Jaccard Index
 * Maximum value = 0.5
 * @param candidates : RDD of candidate entity ID pairs
 * @param candidatesMap : Map of key-value pairs (Entity ID 1, (Subject URI, Relationship Name, Subject Name, Category of Subject, Sub-Category of Subject))
 * @return : RDD of tuples ((Entity ID 1, Entity ID 2), Similarity Value)
 */
def URISimilarity_Jaccard (candidates : RDD[(Int,Int)], candidatesMap : Map[Long,(String,String,String,String,String)]) : RDD[((Int,Int),Double)] = {
   
   //Finds Jaccard Similarity between bi-grams of characters in the Subject Name (extracted from the Subject URI) for either entities
   val candidatesSimilarityValues = candidates.map{case(x,y) => {
                                                 val x_bigrams = candidatesMap.get(x)
                                                                              .map(_._3)
                                                                              .getOrElse("")
                                                                              .sliding(2)
                                                                              .toList
                                                 val y_bigrams = candidatesMap.get(y)
                                                                              .map(_._3)
                                                                              .getOrElse("")
                                                                              .sliding(2)
                                                                              .toList
                                                 val jaccardSimBigramMatches = (((x_bigrams intersect y_bigrams).length.toDouble)/(x_bigrams.length + y_bigrams.length).toDouble)
                                                 ((x,y),jaccardSimBigramMatches)
                                           }}
  candidatesSimilarityValues
 }




/*
 * Calculates the Precision, Recall and F-measure using the predicted and true entity matches
 * @param predictedEntityMatchesNames : RDD of  predicted entity tuples (Entity Name 1, Entity Name 2) 
 * @param trueEntityMatchesNames : RDD of  true entity tuples (Entity Name 1, Entity Name 2) 
 * @return : -
 */
def Evaluation (predictedEntityMatchesNames : RDD[(String,String)], trueEntityMatchesNames : RDD[(String,String)]) {

  println("\n\n-----EVALUATION-----\n") 
  
  val noOfTrueMatches = trueEntityMatchesNames.count()
  val noOfPredictedMatches = predictedEntityMatchesNames.count()
  
  val predictedEntitySet = predictedEntityMatchesNames.collect().toSet
  val trueEntitySet = trueEntityMatchesNames.collect().toSet
  
  val falsePositives = predictedEntitySet.diff(trueEntitySet)
  val noOfFalsePositives = falsePositives.size
  
  val truePositives = trueEntitySet intersect predictedEntitySet
  val noOfTruePositives = truePositives.size
  
  val falseNegatives = trueEntitySet.diff(predictedEntitySet)
  val noOfFalseNegatives = falseNegatives.size
  
  //Precision
  val precision = (noOfTruePositives.toDouble)/(noOfTruePositives + noOfFalsePositives).toDouble
  
  //Recall
  val recall = (noOfTruePositives.toDouble)/(noOfTruePositives + noOfFalseNegatives).toDouble
  
  //F-Measure
  val f_measure = 2*((precision*recall)/(precision + recall))
  
  println("\tPrecision : " + precision)
  println("\tRecall : " + recall)
  println("\tF-measure : " + f_measure)
  println("\tNo. Of False Positives : " + noOfFalsePositives)
  println("\tNo. Of False Negatives : " + noOfFalseNegatives)
  println("\t\nFalse Positives : \n")
  falsePositives.foreach(println(_))
  println("\t\nFalse Negatives : \n")
  falseNegatives.foreach(println(_))
   
}


}//End of Object
  
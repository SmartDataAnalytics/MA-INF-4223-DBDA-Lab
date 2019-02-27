import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql._
import net.sansa_stack.rdf.spark.kge.triples._

/**
 * Main object to run the TransH model
 */
object TransHMain {

  //Initialize spark
  val spark = SparkSession.builder.master("local")
    .appName("TransH").getOrCreate()
    
  def main(args: Array[String]): Unit = {

    //Read file containing entities with respective Ids ex: ("entity as string", 1)
    val entityIds = spark.sparkContext.textFile("FB15k/entity2id.txt").map( i => {
      var splitRow = i.split("\t")
      (splitRow(0), splitRow(1).toInt)
    }).collectAsMap()
    
    //Read file containing relations with respective Ids ex: ("relation as string", 2)
    val relationIds = spark.sparkContext.textFile("FB15k/relation2id.txt").map( i => {
      var splitRow = i.split("\t")
      (splitRow(0), splitRow(1).toInt)
    }).collectAsMap()

    //Read the training data as String Triples 
    val trainData = new Triples("FB15k/train.txt", "\t", false, false, spark)
    
    import spark.implicits._
    
    //Convert the string Triples into IntegerTriples using the mappings of entities and relations to Ids
    val train = trainData.triples.map ( i =>
        IntegerTriples(entityIds(i.Subject)+1,relationIds(i.Object)+1,entityIds(i.Predicate)+1)
    ).persist()

    //Instatiate and run TransH model
    var model = new TransH(train, entityIds.size, relationIds.size, 1200, 20, 0.25f, 1000, 0.01f, spark)
    model.run()

  }
  
}
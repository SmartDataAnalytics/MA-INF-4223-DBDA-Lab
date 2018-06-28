package axiom_extraction


import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.fpm.FPGrowth


object Main extends App {

  var DEBUG: Boolean = false
  
  override def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName(s"AxiomExtraction")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val resource = "test_input.nt"
    val path = getClass.getResource(resource).getPath()
    val df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(path)
    Console.println("===========Input DataFrames===========")
    df.show()
    
    Console.println("===========Predicates===========")
    //selecting all distinct predicates
    val distinctValuesDF = df.select("p").distinct
    //selecting all distinct instances
    val distinctSubjectsDF = df.select("s").distinct
    val distinctObjectsDF = df.select("o").distinct
    distinctValuesDF.show()
    
    //all distinct instances in a list
    val instances = distinctSubjectsDF.union(distinctObjectsDF).distinct().rdd.map(r => r(0)).collect()
    
    //list of subsamples for instance pair extraction
    var dfList : ListBuffer[DataFrame] = new ListBuffer()
    
    var transactions : ListBuffer [String] = new ListBuffer()
    
    
    Console.println("===========Extendet Predicates===========")
    //printing out distinct predicate values
    distinctValuesDF.select("p").take((distinctValuesDF.count()).toInt).foreach(printWithNeg)    
    
    //sampling the pair groups
    instances.foreach {x => getInstanceFilter(x,df,dfList)}
    
    Console.println("===========Transactions Extracted===========")
    //extracting pairs and generating transactions
    dfList.foreach(x=>transactions += extractTransactions(x))
    transactions.foreach(println)
    
    import spark.implicits._
    val dataset = spark.createDataset(transactions).map(t => t.split(" ")).toDF("items")
    
    
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.8)
    val model = fpgrowth.fit(dataset)
    
    Console.println("===========Frequent Items===========")
    // Display frequent itemsets.
    model.freqItemsets.show()
    
    // Display generated association rules.
    Console.println("===========Association Rules===========")
    model.associationRules.show()
   
    
        
  }

	def debug(out: String) {
	   if (DEBUG)
	      println(out)
	}
	
	def printWithNeg(x:Any) {
	  Console.println(x)	  
	  Console.println("[not_"+x.toString().takeRight(x.toString().length()-1))
	}
	
	def extractTransactions(df :DataFrame): String = {
	  var count:Int = 0
	  var transactions : ListBuffer[String] = new ListBuffer[String]
    var outString: String = ""
    var currObject: String = ""                        
    count = df.count().toInt         
    currObject = df.sort("o").first()(2).toString()
    //outString = "(" + df.sort("o").first()(0).toString() + ":" + df.sort("o").first()(2).toString() + ") -> "
    df.sort("o").collect.foreach{ x=>
                    	    {                           	  
                    	      outString += x(1) +" "
                    	      outString += "not_"+x(1).toString() +" "            	    
                    	    }  
                    	   
                         }
     return outString.take(outString.length()-1)
	  }
	
	
	def getInstanceFilter(x:Any,df:DataFrame,lDf:ListBuffer[DataFrame]) {
	  val output=df.select("s","p","o").where("s == '"+x.toString()+"' ")
	  val instances = output.select("o").distinct().rdd.map(r => r(0)).collect()
	  instances.foreach(f =>{
	    val subOutput=output.select("s","p","o").where("o == '"+f.toString()+"' ")
  	  if(subOutput.count()>0)//if at least one paire exists
  	  {
  	    lDf.append(subOutput.sort(asc("o")))
  	  }
	  })

	}
}



















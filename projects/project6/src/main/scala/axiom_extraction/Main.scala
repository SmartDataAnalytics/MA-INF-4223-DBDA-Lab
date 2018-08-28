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
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.NodeFactory


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
    val lang: Lang = Lang.NTRIPLES
    
    val df = spark.read.rdf(lang)(path)
    Console.println("===========Input DataFrames===========")
    df.show()
    Console.println("===========Predicates===========")
    //selecting all distinct predicates
    val distinctPredicatesDF = df.getPredicates().distinct
    //selecting all distinct instances
    val distinctSubjectsDF = df.getSubjects().distinct
    val distinctObjectsDF = df.getObjects().distinct
    distinctPredicatesDF.show()
    
    //all distinct instances in a list
    val instances = distinctSubjectsDF.union(distinctObjectsDF).distinct().rdd.map(r => r(0)).collect()
    
    //list of subsamples for instance pair extraction
    var dfList : ListBuffer[DataFrame] = new ListBuffer()
    
    var transactions : ListBuffer [String] = new ListBuffer()
    
    //sampling the pair groups
    instances.foreach {x => getInstanceFilter(x,df,dfList)}
    
    Console.println("===========Transactions Extracted Symmetry===========")
    //extracting pairs and generating transactions
    //dfList.foreach(f=>f.show())
    dfList.foreach(x=>transactions += extractTransactionsSymmetryAxiom(x))
    transactions.foreach(println)
    
    import spark.implicits._
    val dataset = spark.createDataset(transactions).map(t => t.split(" ")).toDF("items")
    
    
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.8)
    val model = fpgrowth.fit(dataset)
    
    Console.println("===========Frequent Items===========")
    // Display frequent itemsets.
    model.freqItemsets.show()
    
    // Display generated association rules.
    Console.println("===========Association Rules Symmetry===========")
    model.associationRules.show()
    extractSymmetryAxiom(model.associationRules)
    
    transactions.clear()
    Console.println("===========Transactions Extracted Subsumption and Disj===========")
    //extracting pairs and generating transactions
    dfList.foreach(x=>transactions += extractTransactionsSubsumptionAndDisjointnessAxiom(x, distinctPredicatesDF))
    transactions.foreach(println)
    
    import spark.implicits._
    val datasetSub = spark.createDataset(transactions).map(t => t.split(" ")).toDF("items")
    
    
    val fpgrowthSub = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.8)
    val modelSub = fpgrowthSub.fit(datasetSub)
    
    Console.println("===========Frequent  Subsumption and Disj===========")
    // Display frequent itemsets.
    modelSub.freqItemsets.show()
    
    // Display generated association rules.
    Console.println("===========Association Rules  Subsumption and Disj===========")
    modelSub.associationRules.show()
    extractSubsumptionDisjointnessAxiom(modelSub.associationRules)
   
        
  }

	def debug(out: String) {
	   if (DEBUG)
	      println(out)
	}
	
	
	def extractTransactionsSymmetryAxiom(df :DataFrame): String = {
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
                    	      outString += x(1).toString() +"^ "            	    
                    	    }  
                    	   
                         }
     return outString.take(outString.length()-1)
	  }
	
		def extractSymmetryAxiom(df :DataFrame) {  
    df.collect.foreach{ x=>
                    	    {                           	  
                    	      val ant: Seq[String] = x.getAs[Seq[String]]("antecedent")	  
                    	      val con: Seq[String] = x.getAs[Seq[String]]("consequent")
                    	      if(ant.size==1 && con.size==1)
                    	      {
                    	        val axiom: String = ant(0)
                    	        if(con.contains(axiom+"^"))
                    	        {
                    	          Console.println("Sym("+axiom+")")
                    	        }
                    	      }
                    	    }  
                }
	  }
	
	def extractTransactionsSubsumptionAndDisjointnessAxiom(df :DataFrame , predicates: DataFrame): String = {
	  
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
                    	    }  
                    	   
                         }
	  predicates.collect.foreach(f=>{
	    if (!(outString contains f(0).toString())){
	    //  println("True: " + outString + " has no " + f(0).toString())
	      outString += "not_"+ f(0).toString() + " "
	    }
	  })
	   return outString.take(outString.length()-1)
	  }
	
			def extractSubsumptionDisjointnessAxiom(df :DataFrame) {  
    df.collect.foreach{ x=>
                    	    {                           	  
                    	      val ant: Seq[String] = x.getAs[Seq[String]]("antecedent")	  
                    	      val con: Seq[String] = x.getAs[Seq[String]]("consequent")
                    	      if(ant.size==1 && con.size==1)
                    	      {
                    	        val axiom: String = ant(0)
                    	        if(con(0).contains("not_"))
                    	        {
                    	          Console.println("Disj("+axiom+","+con(0).replace("not_","")+")")
                    	        }
                    	        else
                    	        {
                    	          Console.println("Subsumes("+axiom+","+con(0)+")")
                    	        }
                    	      }
                    	    }  
                }
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



















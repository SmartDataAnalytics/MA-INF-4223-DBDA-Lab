import cosum._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.SparseMatrix
import java.io.File

import breeze.linalg._
import breeze.numerics._
import breeze.math._

object MainProgramC{ 
  def main(args: Array[String]) { 

    //initialize Spark
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Distributed Entity Resolution")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    
    val t1 = System.currentTimeMillis()
    
    var product2attr = "src/main/resources/tt/product2attr.csv"   
    var product2product = "src/main/resources/tt/product2product.csv"
    var attr2attr = "src/main/resources/tt/attr2attr.csv"
    
    //Read Graph in COO format
    var parseG = sqlContext.read.option("header", "false").csv(product2attr).rdd.map(Utils.parsTriplesnew(_)).collect().toIterable
    var G = SparseMatrix.fromCOO(10, 71, parseG).toDense
    var n = G.numRows
    var m = G.numCols
    
    //Read sim matrix for 1st type vertex: product to product in COO format
    parseG = sqlContext.read.option("header", "false").csv(product2product).rdd.map(Utils.parsTriplesnew(_)).collect().toIterable
    var sim = SparseMatrix.fromCOO(n, n, parseG)
    
    //Read sim matrix for 1st type vertex: product to product in COO format
    parseG = sqlContext.read.option("header", "false").csv(attr2attr).rdd.map(Utils.parsTriplesnew(_)).collect().toIterable
    var sim2 = SparseMatrix.fromCOO(m, m, parseG)

    //Create DenseMatrices
    var simDM = new DenseMatrix(sim.numRows, sim.numCols, sim.toArray)
    var sim2DM = new DenseMatrix(sim2.numRows, sim2.numCols, sim2.toArray)
    var GDM = new DenseMatrix(G.numRows, G.numCols, G.toArray)
    
    var A = Array.ofDim[DenseMatrix[Double]](2, 2)
    A(0)(0) = Utils.normalize_DM(simDM) 
    A(0)(1) = GDM / max(GDM)
    A(1)(0) = DenseMatrix.zeros[Double](71, 10)
    A(1)(1) = Utils.normalize_DM(sim2DM) 

    var sumGraph = new CSummaryGraph(sc)

    //Call summarization function
    var C = sumGraph.graphSumPlus(A)
    
    //Output to file the C matrices containing probabilities of links between original vertices to super nodes of summarization graph
    csvwrite(new File("simbreeze.csv"), C._2(0))  //product to product
    csvwrite(new File("sim2breeze.csv"), C._2(1)) //word to word
    val t2 = System.currentTimeMillis()
    println("Run time : " + (t2 - t1) / 1000 + " seconds.")
   
  }

}
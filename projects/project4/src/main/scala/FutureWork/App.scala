package FutureWork

import cosum._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry }
import org.apache.spark.mllib.linalg._

object App {
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Distributed Entity Resolution")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)

    val t1 = System.currentTimeMillis()
     
    var product2attr = "src/main/resources/product2attr.csv"
    val Gt = sqlContext.read.option("header", "false").csv(product2attr).rdd
    val thirdRdd = Gt.map { x => (x(2).toString().toDouble) }
    val maxOfG = thirdRdd.max()
    println("max sum "+maxOfG)
    
    val parseG = sqlContext.read.option("header", "false").csv(product2attr).rdd.map(Utils.parsTriples(_))
    val G = new CoordinateMatrix(parseG)
    //val transformedRDD1 = G.entries.map { case MatrixEntry(row: Long, col: Long, sim: Double) => Array(row, col, sim).mkString(",") }
    //transformedRDD1.take(100).foreach(println(_))
    val n = G.numRows()
    val m = G.numCols()
    
    //val transformedRDD = twiceG.entries.map { case MatrixEntry(row: Long, col: Long, sim: Double) => Array(row, col, sim).mkString(",") }
    //transformedRDD.take(100).foreach(println(_))
    
    println("Values of initial G are "+n+" "+m)
    
    val simNorm = Utils.generateNormalizedCM("src/main/resources/product2product.csv",n, n, sc, sqlContext)
    val sim2Norm = Utils.generateNormalizedCM("src/main/resources/attr2attr.csv", m, m, sc, sqlContext)

    
    var A = Array.ofDim[CoordinateMatrix](2,2)
    A(0)(0) = simNorm 
    A(0)(1) = G
    A(1)(0) = new CoordinateMatrix(sc.emptyRDD[MatrixEntry])
    A(1)(1) = sim2Norm 
    
    val sumGraph = new SummaryGraph(sc)
    val tupleOutput = sumGraph.GraphSumPlus(A)
    val finalC = tupleOutput._2
    
    val t2 = System.currentTimeMillis()
    println("Run time : " + (t2 - t1) / 1000 + " seconds.")
  }

}
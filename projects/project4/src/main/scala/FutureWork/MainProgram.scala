package FutureWork

import cosum._
import org.apache.spark.sql._
import breeze.linalg._
import breeze.numerics._

object MainProgram{ 
  def main(args: Array[String]) { 
//
//    val spark = SparkSession.builder
//      .master("local[*]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .appName("Distributed Entity Resolution")
//      .getOrCreate()
//    import spark.implicits._
//    val sc = spark.sparkContext
//    val sqlContext = new SQLContext(sc)
//    
//    val t1 = System.currentTimeMillis()
//      
//    var product2attr = "src/main/resources/product2attr.csv"   
//    var maxOfG = 1 // todo
//    
//    var parseG = sqlContext.read.option("header", "false").csv(product2attr).rdd.map(Utils.parsTriples(_, maxOfG))
//    var G = new CoordinateMatrix(parseG)
//
//    var matrixG = G.toBlockMatrix().toLocalMatrix()
//    var n = G.numRows()
//    var m = G.numCols()
//    
//    println(n+" "+m)
//    
//    G = Utils.generateNormalizedCM("src/main/resources/product2product.csv",n, n, sc, sqlContext)
//    var mSimNorm = G.toBlockMatrix().toLocalMatrix()
// 
//    G = Utils.generateNormalizedCM("src/main/resources/attr2attr.csv", m, m, sc, sqlContext)
//      
//    var newread = csvread(new File("src/main/resources/bproduct2attr.csv"))
//    println(newread(0,1))
//      for(i <- 0 until matrixG.numRows){
//        for(j <- 0 until matrixG.numCols){
//          print(matrixG(i,j)+",")
//        }
//        println()
//      }
//    
//    println(matrixG)
//    var dSim2Norm = G.toBlockMatrix()
//    var mSim2Norm = dSim2Norm.toLocalMatrix()
//
//    println("ok1")
//    var dSimNormDM = new DenseMatrix(mSimNorm.numRows, mSimNorm.numCols, mSimNorm.toArray)
//    println(max(dSimNormDM))
//    println("test")
//    println("ok52")
//    var matrixGDM = new DenseMatrix(matrixG.numRows, matrixG.numCols, matrixG.toArray)
//    println("ok3")
//
//    var sim2toArr = mSim2Norm.toArray
//    println("ok4")
//    var mSim2NormDM = new DenseMatrix(mSim2Norm.numRows, mSim2Norm.numCols, sim2toArr)
//
//    var A = Array.ofDim[DenseMatrix[Double]](2, 2)
//    A(0)(0) = dSimNormDM
//    A(0)(1) = matrixGDM
//    A(1)(0) = DenseMatrix.zeros[Double](0, 0)
//    A(1)(1) = mSim2NormDM 
//    
//    println("start everything")
//    var sumGraph = new LSummaryGraph(sc)
//    var C = sumGraph.GraphSumPlus(A)
//    val t2 = System.currentTimeMillis()
//    println("Run time : " + (t2 - t1) / 1000 + " seconds.")

   
  }

}
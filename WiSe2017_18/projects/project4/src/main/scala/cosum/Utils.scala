package cosum

import org.apache.spark.sql._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry }

import breeze.linalg._ 
import breeze.numerics._
import breeze.math._
object Utils {

  def parsTriples(parsData: Row): MatrixEntry = {
    MatrixEntry(parsData(0).toString().toLong, parsData(1).toString().toLong, parsData(2).toString().toDouble)
  }
  
  /**
   * Parse COO format to rdd
   */
  def parsTriplesnew(parsData: Row): (Int,Int,Double) = {
    (parsData(0).toString().toInt, parsData(1).toString().toInt, parsData(2).toString().toDouble)
  }


  def normalize_X(X: CoordinateMatrix, n: Long, m: Long, sc: SparkContext): CoordinateMatrix = {
    val d = X.numCols()

    val norms = X.entries
      .map { case MatrixEntry(row, _, value) => (row, value * value) }
      .reduceByKey(_ + _)

    val normsSqrt = norms.map { case (x, y) => (x, Math.sqrt(y)) }

    val broadcastednorms = sc.broadcast(normsSqrt.collect().toMap)

    val eps = 1e-9;
    println("EPSILON IS:" + eps)
    val normX = X.entries
      .map { case MatrixEntry(row, col, value) => MatrixEntry(row, col, value / (broadcastednorms.value(row) + eps)) }

    val findMin = normX.map { case MatrixEntry(row, col, value) => value }
    val min = findMin.min()
    if (min < 0) {
      sys.ShutdownHookThread {
        println("The entries cannot be negative")
      }
      sc.stop()
    }
    new CoordinateMatrix(normX,n,m)
  }
  
  def generateNormalizedCM(path: String, n: Long, m: Long, sc: SparkContext, sqlContext: SQLContext) : CoordinateMatrix = {
    val product2productRDD = sqlContext.read.option("header", "false").csv(path).rdd
    val matrixEntry = sqlContext.read.option("header", "false").csv(path).rdd.map(parsTriples(_))
    val sim = new CoordinateMatrix(matrixEntry, n, m)
    val simNorm = normalize_X(sim, n, m, sc)
    simNorm
  }
  
  /**
   * Normalization function of a DenseMatrix
   */
  def normalize_DM(X: DenseMatrix[Double]) : DenseMatrix[Double] = {
    var d = X.cols
    var norms = sum(X *:* X, Axis._0)
    norms = sqrt(norms)
    var eps = 1e-9

    var XX = X /:/ (tile(norms, 1, d) + eps) 
    XX
  }
  
  def showAllDM(X: DenseMatrix[Double]){
    for(i<-0 until X.rows){
      for(j<-0 until X.cols){
        print(X(i,j)+"  ")
      }
      println()
    }
  }
  
  
  
  
  
  
  
  
  
}
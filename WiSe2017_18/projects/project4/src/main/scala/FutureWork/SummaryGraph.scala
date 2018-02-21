package cosum

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Round
import org.apache.spark.mllib.feature.ElementwiseProduct
import util.control.Breaks._
import scala.collection.mutable.ListBuffer

class SummaryGraph(sc: SparkContext) {

  def GraphSumPlus(A: Array[Array[CoordinateMatrix]], varargin: Vector[Any] = Vector()): (Array[Array[CoordinateMatrix]], Array[CoordinateMatrix], Double) = {

    val k = A.length
    var n = Array.ofDim[Int](k)
    
    for (i <- 0 until k) {
      for (j <- 0 until k) {
        var nn = 0
        
        try {
          nn = A(i)(j).numRows().toInt
        }catch{
          case _: Throwable => nn = 0
        }
                
        if (nn > 0) {
          if (n(i) == 0){
            n(i) = nn
          }
          else if (n(i) != nn && (i!=1 && j!=0)) {
            throwError("need same sizes in k-partite graph description in matrix" + i + " " + j)
          }
        }

        try {
          nn = A(j)(i).numCols().toInt
        } catch {
          case _: Throwable => nn = 0
        }
        
        if (n(i) == 0) {
          n(i) = nn
        } else if (n(i) != nn && (i!=0 && j!=1)) {
          throwError("Need same sizes in k-partite graph description in matrix" + i + " " + j)
        }
        
      }
    }

    if (n.reduceLeft(_ min _) == 0L) {
      throwError("need at least one connectivity matrix per column")
    }

    var clusters = n
    var numit = 200
    var abort = 1e-10
    var verbose = true
    var verbosecompact = true

    if (varargin.length > 0) {
      var clusters = varargin(0)
      var numit = varargin(1)
      var abort = varargin(2)
      var verbose = varargin(3)
      var verbosecompact = varargin(4)
    }
    var res = (clusters, numit, abort, verbose, verbosecompact)

    if (verbose) println("Starting graph Co-clustering of k-partite graph with partition sizes:" + k)

    /*
     * symmetrize or fill up lower (or upper) triangular part of A
     */
    for (i <- 0 until k) {
      for (j <- 0 until k) {
        if (A(i)(j).entries.isEmpty()) {
          A(i)(j) = new CoordinateMatrix(A(j)(i).transpose().entries, A(j)(i).numRows(), A(j)(i).numCols())
        }
        else if(!A(j)(i).entries.isEmpty()){
          A(i)(j) = sumTwoCoordinateMatrix(A(i)(j), A(j)(i).transpose())
          A(i)(j) = elementWiseOpOfCMByScalar(A(i)(j), 2, "add")
        }
      }
    }

    // matrix containing empty connections implying NO fit here, includes diagonal!
    var hasCon = Array.ofDim[Int](k, k)
    for (i <- 0 to k - 1) {
      for (j <- 0 to k - 1) {
        hasCon(i)(j) = if (A(i)(j).numRows() > 0) 1 else 0;
      }
    }
    var flag = true
    var r = Array.ofDim[Int](k)
    for (i <- 0 until k) {
      r(i) = n(i)
    }

    // Initialization
    val t1 = System.currentTimeMillis()
    var tupleInit = initilize(A, hasCon, n, r)
    var B = tupleInit._1
    var C = tupleInit._2
    var powerA = tupleInit._3
    val t2 = System.currentTimeMillis()
    println("Initialization time : " + (t2 - t1) / 1000 + " seconds.")

    var Cluster = Array.ofDim[CoordinateMatrix](k)
    var Summary = Array.ofDim[CoordinateMatrix](k, k)
    while (flag) {
      var tupleRefinement = Refinement(B, C, A, r, powerA, res, hasCon)
      B = tupleRefinement._1
      C = tupleRefinement._2
      var fold = objective(A, B, C, k, hasCon)
      println("Old fold function is : " + fold)

      var tupleSearch = (C, B, r, false)
      if (r.foldLeft(0L)(_ + _) > n.foldLeft(0L)(_ + _)) {
        tupleSearch = search(B, C, k, r, true, n)
      } else {
        tupleSearch = search(B, C, k, r, false, n)
      }
      Cluster = tupleSearch._1
      Summary = tupleSearch._2
      r = tupleSearch._3
      var isreduced = tupleSearch._4

      if (isreduced) {
        var tupletupleRefinement2 = Refinement(Summary, Cluster, A, r, powerA, res, hasCon)
        Summary = tupletupleRefinement2._1
        Cluster = tupletupleRefinement2._2
      }
      var fnew = objective(A, Summary, Cluster, k, hasCon)
      println("New objective value is : " + fnew)
      if (Math.abs(fnew - fold) / powerA < abort) {
        flag = false;
      }
      B = Summary
      C = Cluster

    }

    val f = objective(A, Summary, Cluster, k, hasCon);

    (Summary, Cluster, f)

    //val duration = (System.nanoTime - t1) / 1e9d

  }

  def initilize(A: Array[Array[CoordinateMatrix]], hasCon: Array[Array[Int]], n: Array[Int], r: Array[Int]): (Array[Array[CoordinateMatrix]], Array[CoordinateMatrix], Double) = {
    val k = A.length;
    var B = Array.ofDim[CoordinateMatrix](k, k)
    var C = Array.ofDim[CoordinateMatrix](k)
    var powerA = 0.0
    val maxcluster = r.reduceLeft(_ max _)
    println(maxcluster)
    if (maxcluster < 50) {
      for (i <- 0 to k - 1) {
        C(i) = randomCoordinateMatrix(n(i), r(i))
        val sums = C(i).entries
          .map { case MatrixEntry(row, _, value) => (row, value) }
          .reduceByKey(_ + _)
        val broadcastedSums = sc.broadcast(sums.collect().toMap)
        val newC = C(i).entries
          .map { case MatrixEntry(row, col, value) => MatrixEntry(row, col, value / (broadcastedSums.value(row))) }
        C(i) = new CoordinateMatrix(newC, n(i), r(i))

      }
    } else {
      for (i <- 0 until k) {
        println("line 173: n(i), r(i) "+n(i)+" "+r(i))
        C(i) = zerosCoordinateMatrix(n(i), r(i))
        if (hasCon(i)(i) == 1) {
          val G = A(i)(i)
          val finalG = sumTwoCoordinateMatrix(G, G.transpose())
          //val SAndCC = conncomp(finalG)
          //val S = SAndCC._1
          //var CC = SAndCC._2
          //CC = CC.transpose()
          //if (S > 5) {
          //  C(i) = ccInit(C(i), CC)
          //} else {
          C(i) = mYRandomInit(C(i))
          //}
        } else {
          C(i) = mYRandomInit(C(i))
        }
      }
    }
    
   println("line : 193 C(i)  n and M are : ====> "+C(0).numRows()+" "+ C(0).numCols())
   
   val transformedRDD1 = C(1).entries.map { case MatrixEntry(row: Long, col: Long, sim: Double) => Array(row, col, sim).mkString(",") }
   transformedRDD1.take(10).foreach(println(_))
    
println("exit initialize 203 ok")
    for (i <- 0 until k) {
      if (hasCon(i)(i) == 1) {
        C(i) = LP(A(i)(i), C(i))
      }
    }
println("exit initialize 203 ok")
    for (i <- 0 until k) {
      for (j <- i until k) {
        if (hasCon(i)(j) == 1) {
          if (j != i) {
            B(i)(j) = randomCoordinateMatrix(r(i), r(j))
          } else {
            B(i)(j) = speyeCoordinateMatrix(r(i), r(j))
          }
          powerA += getSumOfAllElementsSquared(A(i)(j))
        }
      }
    }
println("exit initialize 216")
    (B, C, powerA)
  }

  def throwError(msg: String) {
    sys.ShutdownHookThread {
      println(msg)
    }
    sc.stop()
  }

  def randomCoordinateMatrix(dim1: Int, dim2: Int) = {

    val r = new java.security.SecureRandom
    var entries = sc.emptyRDD[MatrixEntry]
    for (i <- 0 to dim1) {
      for (j <- 0 to dim2) {
        val str: String = i + " " + j + " " + r.nextDouble() + 0.00001;
        val tmpRDD = sc.parallelize(List(str))

        val tmpRDDMatrixEntry = tmpRDD.map(parseMatrixEntry(_))

        entries.union(tmpRDDMatrixEntry)
      }
    }
    new CoordinateMatrix(entries, dim1, dim2)
  }

  def zerosCoordinateMatrix(dim1: Int, dim2: Int) = {
    var entries = sc.emptyRDD[MatrixEntry]
    new CoordinateMatrix(entries, dim1, dim2)
  }

  def speyeCoordinateMatrix(dim1: Int, dim2: Int) = {
    var entries = sc.emptyRDD[MatrixEntry]
    for (i <- 0 until dim1) {
      for (j <- 0 until dim2) {
        var tmpV = 0
        if (i == j) {
          tmpV = 1
        }
        val str: String = i + " " + j + " " + tmpV
        val tmpRDD = sc.parallelize(List(str))

        val tmpRDDMatrixEntry = tmpRDD.map(parseMatrixEntry(_))

        entries.union(tmpRDDMatrixEntry)
      }
    }
    new CoordinateMatrix(entries, dim1, dim2)
  }

  def reduceSumMatrixEntries(EM1: MatrixEntry, EM2: MatrixEntry): MatrixEntry = {
    MatrixEntry(EM1.i, EM1.j, EM1.value + EM2.value)
  }

  def matrixEntryColOnly(s: String, EM: MatrixEntry): MatrixEntry = {
    EM
  }

  def parseMatrixEntry(parsData: String): MatrixEntry = {
    val splitted = parsData.split(" ")
    MatrixEntry(splitted(0).toLong, splitted(1).toLong, splitted(2).toDouble)
  }

//  def conncomp(G: CoordinateMatrix): (Int, CoordinateMatrix) = {
//    (1, G)
//  }

//  def ccInit(Y: CoordinateMatrix, CC: CoordinateMatrix): CoordinateMatrix = {
//    val n = Y.numRows().toInt
//    val r = Y.numCols()
//
//    var entriesOfY = Y.entries
//
//    for (j <- 0 until n) {
//
//      var convertToRDD = CC.entries.map { case MatrixEntry(i, k, v) => ((i, k), v) }
//      var lookUpV = convertToRDD.lookup((0, j))(0)
//
//      var idx = Math.round(lookUpV)
//      if (idx > r) {
//        idx = idx % r
//      }
//      if (idx == 0) {
//        idx = 1
//      }
//
//      entriesOfY = entriesOfY.map {
//        case MatrixEntry(i, k, v) =>
//          if (i == j && k == idx) {
//            MatrixEntry(i, k, 1)
//          } else {
//            MatrixEntry(i, k, v)
//          }
//      }
//
//    }
//    new CoordinateMatrix(entriesOfY)
//  }

  def mYRandomInit(Y: CoordinateMatrix): CoordinateMatrix = {
    val random = new java.security.SecureRandom
    val n = Y.numRows()
    val r = Y.numCols()
    println("line 321: Values n and r inside myrandominit are "+n +" "+r)
    
    var listTmp = new ListBuffer[MatrixEntry]()
    for (j <- 0 until n.toInt) {
      var index = Math.round(random.nextDouble() * r);
      if (index < 1) {
        index = 1
      }
      if (index > r.toInt) {
        index = index % r
      }

      listTmp += MatrixEntry(j, index, 1)
      println("line 331: Iteration j is "+j+" and array length is"+listTmp.length)
    }
    var newY = sc.parallelize(listTmp)
    newY.take(10).foreach(println(_))
    new CoordinateMatrix(newY,n, r)
  }

  def LP(S: CoordinateMatrix, x: CoordinateMatrix): CoordinateMatrix = {

    var y = new CoordinateMatrix(x.entries, x.numRows(), x.numCols())
    for (iter <- 1 until 2) {
      println("Iteration: "+iter)
      var sXyXzeropointfive = multiplyTwoCoordinateMatrixWithConstant(S, y, 0.5)
      //println("sXyXzeropointfive n and M are : ====> "+sXyXzeropointfive.numRows(), sXyXzeropointfive.numCols())
      var xXzeropointfive = multiplyCoordinateMatrixWithConstant(x, 0.5)
      //println("xXzeropointfive n and M are : ====> "+xXzeropointfive.numRows(), xXzeropointfive.numCols())
      y = sumTwoCoordinateMatrix(sXyXzeropointfive, xXzeropointfive)
      y = divideCoordinateMatrixWithRowSum(y, 0)
    }
    y = multiplyCoordinateMatrixWithConstant(y, 2) //or divide by 0.5
    y
    
  }

  def multiplyCoordinateMatrixWithConstant(x: CoordinateMatrix, nr: Double): CoordinateMatrix = {
    val EM = x.entries.map {
      case MatrixEntry(r, c, v) => MatrixEntry(r, c, v * nr)
    }
    new CoordinateMatrix(EM, x.numRows(), x.numCols())
  }

  def multiplyTwoCoordinateMatrixWithConstant(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix, nr: Double): CoordinateMatrix = {
    val M_ = leftMatrix.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
    val N_ = rightMatrix.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

    val productEntries = M_
      .join(N_)
      .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w * nr)) })
      .reduceByKey(_ + _)
      .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

    new CoordinateMatrix(productEntries, leftMatrix.numRows(), rightMatrix.numCols())
    
    //leftMatrix.toBlockMatrix().multiply(rightMatrix.toBlockMatrix()).toCoordinateMatrix()
  }

  def sumTwoCoordinateMatrix(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {
//        println("leftMatrix n and M are : ====> "+leftMatrix.numRows(), leftMatrix.numCols())
//     val transformedRDD1 = leftMatrix.entries.map { case MatrixEntry(row: Long, col: Long, sim: Double) => Array(row, col, sim).mkString(",") }
//   transformedRDD1.take(10).foreach(println(_))
//    println("X n and M are : ====> "+rightMatrix.numRows(), rightMatrix.numCols())
//    
//   
//     val transformedRDD1x = rightMatrix.entries.map { case MatrixEntry(row: Long, col: Long, sim: Double) => Array(row, col, sim).mkString(",") }
//   transformedRDD1x.take(10).foreach(println(_))
    
    val entriesRDD = leftMatrix.toBlockMatrix().add(rightMatrix.toBlockMatrix()).toCoordinateMatrix().entries
    new CoordinateMatrix(entriesRDD, leftMatrix.numRows(), leftMatrix.numCols())
  }

  def divideCoordinateMatrixWithRowSum(CM: CoordinateMatrix, eps: Double): CoordinateMatrix = {
    val sums = CM.entries
      .map { case MatrixEntry(row, _, value) => (row, value) }
      .reduceByKey(_ + _)

    val broadcastedSums = sc.broadcast(sums.collect().toMap)
    val normX = CM.entries
      .map { case MatrixEntry(row, col, value) => MatrixEntry(row, col, value / (broadcastedSums.value(row) + eps)) }
    new CoordinateMatrix(normX, CM.numRows(), CM.numCols())
  }

  def divideCoordinateMatrixWithColSum(CM: CoordinateMatrix, eps: Double): CoordinateMatrix = {
    val sums = CM.entries
      .map { case MatrixEntry(_, col, value) => (col, value) }
      .reduceByKey(_ + _)

    val broadcastedSums = sc.broadcast(sums.collect().toMap)
    val normX = CM.entries
      .map { case MatrixEntry(row, col, value) => MatrixEntry(row, col, value / (broadcastedSums.value(col) + eps)) }
    new CoordinateMatrix(normX, CM.numRows(), CM.numCols())
  }

  def getSumOfAllElementsSquared(X: CoordinateMatrix): Double = {
    val norms = X.entries
      .map { case MatrixEntry(row, _, value) => (value * value) }
    norms.reduce(_ + _)
  }

  def sumOfCMCol(X: CoordinateMatrix): RDD[(Long, Double)] = {
    val sum = X.entries
      .map { case MatrixEntry(_, col, value) => (col, value) }
      .reduceByKey(_ + _)
    sum
  }

  def diag(X: RDD[(Long, Double)]): CoordinateMatrix = {
    val diag = X.map(line => MatrixEntry(line._1, line._1, line._2))
    new CoordinateMatrix(diag)
  }
  /*
   * End of Initilize functions
   */
  /*
   * Start of Refinement
   */
  def Refinement(B: Array[Array[CoordinateMatrix]], C: Array[CoordinateMatrix], A: Array[Array[CoordinateMatrix]], r: Array[Int], powerA: Double, res: Tuple5[Array[Int], Int, Double, Boolean, Boolean], hasCon: Array[Array[Int]]): (Array[Array[CoordinateMatrix]], Array[CoordinateMatrix], Array[Double]) = {
    var numchar = 0;
    var fs = Array.ofDim[Double](res._2)
    val myzero = 1e-18
    val k = A.length
    for (it <- 0 until res._2) {
      for (i <- 0 until k) {
        var N = zerosCoordinateMatrix(C(i).numRows().toInt, C(i).numCols().toInt)
        var D = zerosCoordinateMatrix(C(i).numRows().toInt, C(i).numCols().toInt)
        for (j <- i until k) {
          if (hasCon(i)(j) == 1) {
            if (j != i) {
              val AxC = multiplyTwoCoordinateMatrixWithConstant(A(i)(j), C(j), 1)
              val AxCxBT = multiplyTwoCoordinateMatrixWithConstant(AxC, B(i)(j).transpose(), 1)
              N = sumTwoCoordinateMatrix(N, AxCxBT)

              val CxB = multiplyTwoCoordinateMatrixWithConstant(C(i), B(i)(j), 1)
              val CxBxCT = multiplyTwoCoordinateMatrixWithConstant(CxB, C(j).transpose(), 1)
              val CxBxCTxC = multiplyTwoCoordinateMatrixWithConstant(CxBxCT, C(j), 1)
              val CxBxCTxCxBT = multiplyTwoCoordinateMatrixWithConstant(CxBxCTxC, B(i)(j).transpose(), 1)
              D = sumTwoCoordinateMatrix(D, CxBxCTxCxBT)
            } else {

              val AxC = multiplyTwoCoordinateMatrixWithConstant(A(i)(j), C(i), 1)
              N = sumTwoCoordinateMatrix(N, AxC)
              val sumA = sumOfCMCol(A(i)(j))
              val diagSumA = diag(sumA)
              val diagSumXC = multiplyTwoCoordinateMatrixWithConstant(diagSumA, C(i), 1)
              D = sumTwoCoordinateMatrix(D, diagSumXC)
            }
          }
        }

        val cixN = elementWiseOpOfCM(C(i), N, "mul")
        val maxDmyzero = maxCM(D, myzero)
        C(i) = elementWiseOpOfCM(cixN, maxDmyzero, "div")
        C(i) = divideCoordinateMatrixWithColSum(C(i), myzero)
      }

      for (i <- 0 until k) {

        for (j <- i + 1 until k) {

          if (hasCon(i)(j) == 1) {

            val CtxA = multiplyTwoCoordinateMatrixWithConstant(C(i).transpose(), A(i)(j), 1)
            val CtxAxC = multiplyTwoCoordinateMatrixWithConstant(CtxA, C(j), 1)
            val BxCtxAxC = elementWiseOpOfCM(B(i)(j), CtxAxC, "mul")

            val CtxC = multiplyTwoCoordinateMatrixWithConstant(C(i).transpose(), C(i), 1)
            val CtxCxB = multiplyTwoCoordinateMatrixWithConstant(CtxC, B(i)(j), 1)
            val CtxCxBxCt = multiplyTwoCoordinateMatrixWithConstant(CtxCxB, C(j).transpose(), 1)
            val CtxCxBxCtxC = multiplyTwoCoordinateMatrixWithConstant(CtxCxBxCt, C(j), 1)
            val max = maxCM(CtxCxBxCtxC, myzero)

            B(i)(j) = elementWiseOpOfCM(BxCtxAxC, max, "div")
            B(j)(i) = B(i)(j).transpose()

          }
        }
      }

      var f = 0.0
      for (i <- 0 until k) {

        for (j <- i until k) {

          if (hasCon(i)(j) == 1) {
            if (j != i) {
              val CxB = multiplyTwoCoordinateMatrixWithConstant(C(i), B(i)(j), 1)
              val CxBxCt = multiplyTwoCoordinateMatrixWithConstant(CxB, C(j).transpose(), 1)
              val AminusCxBxCt = elementWiseOpOfCM(A(i)(j), CxBxCt, "sub")
              f = f + getSumOfAllElementsSquared(AminusCxBxCt)

            } else {
              val CxCt = multiplyTwoCoordinateMatrixWithConstant(C(i), C(i).transpose(), 1)
              val AminusCxCt = elementWiseOpOfCM(A(i)(i), CxCt, "sub")
              f = f + getSumOfAllElementsSquared(AminusCxCt)

            }
          }

        }
      }

      fs(it) = f
    }

    (B, C, fs)
  }

  def maxCM(A: CoordinateMatrix, B: Double): CoordinateMatrix = {

    val max = A.entries.map {
      case MatrixEntry(i, j, v) =>
        if (v < B)
          MatrixEntry(i, j, B)
        else
          MatrixEntry(i, j, v)
    }

    new CoordinateMatrix(max, A.numRows(), A.numCols())
  }

  def elementWiseOpOfCM(A: CoordinateMatrix, B: CoordinateMatrix, op: String): CoordinateMatrix = {
    val M_ = A.entries.map({ case MatrixEntry(i, j, v) => ((i, j), v) })
    val N_ = B.entries.map({ case MatrixEntry(i, j, w) => ((i, j), w) })

    var funct = (x: Double, y: Double) => x + y
    if (op.equals("sub")) {
      val funct = (x: Double, y: Double) => x - y
    } else if (op.equals("mul")) {
      val funct = (x: Double, y: Double) => x * y
    } else if (op.equals("div")) {
      val funct = (x: Double, y: Double) => x / y
    }
    
    println("dimensions of A are: "+ A.numRows(), A.numCols())
    println("dimensions of B are: "+ B.numRows(), B.numCols())

    val productEntries = M_
      .join(N_)
      .map({ case ((i, j), (v, w)) => ((i, j), (funct(v, w))) })
      .map({ case ((i, k), value) => MatrixEntry(i, k, value) })

    new CoordinateMatrix(productEntries, A.numRows(), A.numCols())
  }
  
  def elementWiseOpOfCMByScalar(A: CoordinateMatrix, nr: Int, op: String) : CoordinateMatrix = {

    var funct = (x: Double, y: Double) => x + y
    if (op.equals("sub")) {
      val funct = (x: Double, y: Double) => x - y
    } else if (op.equals("mul")) {
      val funct = (x: Double, y: Double) => x * y
    } else if (op.equals("div")) {
      val funct = (x: Double, y: Double) => x / y
    }
     
    val entriesRDD = A.entries.map({ case MatrixEntry(i, j, v) => MatrixEntry(i, j, funct(v, nr)) }) 
     
    new CoordinateMatrix(entriesRDD, A.numRows(), A.numCols())
  }

  def objective(A: Array[Array[CoordinateMatrix]], B: Array[Array[CoordinateMatrix]], C: Array[CoordinateMatrix], k: Int, hasCon: Array[Array[Int]]): Double = {

    var f = 0.0
    for (i <- 0 until k) {

      for (j <- i + 1 until k) {

        if (hasCon(i)(j) == 1) {

          val CxB = multiplyTwoCoordinateMatrixWithConstant(C(i), B(i)(j), 1)
          val CxBxCt = multiplyTwoCoordinateMatrixWithConstant(CxB, C(j).transpose(), 1)
          val dz = elementWiseOpOfCM(A(i)(j), CxBxCt, "sub")
          f = f + getSumOfAllElementsSquared(dz)

        }
      }

    }
    f
  }

  def search(B: Array[Array[CoordinateMatrix]], C: Array[CoordinateMatrix], k: Int, r: Array[Int], ishard: Boolean, n: Array[Int]): (Array[CoordinateMatrix], Array[Array[CoordinateMatrix]], Array[Int], Boolean) = {
    var isreduced = false

    for (i <- 0 until k) {
      println("old number of clusters for" + i + "-type is " + r(i))
      breakable {
        if (r(i) < Math.max(3, n(i) / k)) {
          println("skip " + i + "-type")
          break;
        }
        var ni = C(i).numRows().toInt
        var a = Array.ofDim[Double](r(i))
        var maxIndexOfRows = C(i).toIndexedRowMatrix().rows.map { case IndexedRow(i, v) => (i, v.argmax) }
        var sortedMaxIndexOfRows = maxIndexOfRows.sortByKey()
        for (j <- 0 until ni) {
          var index = sortedMaxIndexOfRows.lookup(j)(0)
          a(index) += 1
        }

        val v = 0
        if (ishard) {
          val v = a.min
        }

        val tupleClusterMinusmany = ClusterMinusmany(C, i, a, v)
        var newC = tupleClusterMinusmany._1
        var isflag = tupleClusterMinusmany._2
        if (isflag) {
          isreduced = true
        }

        var newB = SummaryMinusmany(B, i, a, v)
        r(i) = newC(i).numCols().toInt
        println("new number of clusters for " + i + 1 + "-type is: " + r(i))
      }
    }

    (C, B, r, isreduced)
  }

  def ClusterMinusmany(C: Array[CoordinateMatrix], i: Int, a: Array[Double], v: Int): (Array[CoordinateMatrix], Boolean) = {
    var M = C(i)
    var Cnew = C
    val n1 = C(i).numRows()
    val n2 = C(i).numCols()
    var nc = 0
    var isreduced = false
    for (m1 <- 0 until a.length) {
      if (a(m1) <= v) {
        nc += 1
        isreduced = true
      }
    }
    var entries = sc.emptyRDD[MatrixEntry]

    nc = 0
    for (m1 <- 0 until a.length) {
      if (a(m1) > v) {
        val filteredRDD = M.entries.filter { case MatrixEntry(i, j, v) => j == m1 }
        val reindexRDD = filteredRDD.map { case MatrixEntry(i, j, v) => MatrixEntry(i, nc, v) }
        entries.union(reindexRDD)
        nc = nc + 1
      }
    }
    var Mnew = new CoordinateMatrix(entries, n1, n2 - nc)

    Cnew(i) = Mnew
    (Cnew, isreduced)
  }

  def SummaryMinusmany(B: Array[Array[CoordinateMatrix]], i: Int, a: Array[Double], v: Int): Array[Array[CoordinateMatrix]] = {
    var Bnew = B
    var n1 = B.length
    var n2 = B(0).length
    var nc = 0

    for (m1 <- 0 until a.length) {
      if (a(m1) <= v) {
        nc = nc + 1;
      }
    }
    for (j <- 0 until n1) {
      breakable {
        var M = B(j)(i)
        if (M.numCols() < 1) {
          break;
        }
        var b1 = M.numRows()
        var b2 = M.numCols()
        var idx = 0
        var entries = sc.emptyRDD[MatrixEntry]
        for (m1 <- 0 until a.length) {
          if (a(m1) > v) {
            val filteredRDD = M.entries.filter { case MatrixEntry(i, j, v) => j == m1 }
            val reindexRDD = filteredRDD.map { case MatrixEntry(i, j, v) => MatrixEntry(i, idx, v) }
            entries.union(reindexRDD)
            idx = idx + 1
          }
        }
        var Mnew = new CoordinateMatrix(entries, b1, b2 - nc)
        Bnew(j)(i) = Mnew
      }
    }

    var entries2 = sc.emptyRDD[MatrixEntry]
    for (j <- 0 until n2) {
      breakable {
        var M = B(i)(j)
        if (M.numRows() < 1) {
          break
        }
        var b1 = M.numRows()
        var b2 = M.numCols()
        var idx = 0
        for (m1 <- 0 until a.length) {
          if (a(m1) > v) {
            val filteredRDD = M.entries.filter { case MatrixEntry(i, j, v) => i == m1 }
            val reindexRDD = filteredRDD.map { case MatrixEntry(i, j, v) => MatrixEntry(idx, j, v) }
            entries2.union(reindexRDD)
            idx = idx + 1
          }
        }
        var Mnew = new CoordinateMatrix(entries2, b1 - nc, b2)
        Bnew(i)(j) = Mnew
      }
    }
    Bnew
  }

}


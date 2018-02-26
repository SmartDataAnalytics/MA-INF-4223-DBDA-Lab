package cosum

import org.apache.spark.SparkContext
import util.control.Breaks._
import breeze.linalg._
import breeze.numerics._
import breeze.math._

class CSummaryGraph(sc: SparkContext) {

  /**
   * The main function which starts the summarization graph algorithm.
   * Taking a k-partite graph A it first checks this graph given as multidimensional array of DenseMatrices for valid input.
   * Initializes a random summary graph which maximum number of super nodes n. 
   * Loops until the Lagrangian objective function converges.
   * Inside that loop it continuously searches for a better summary graph.
   * After termination condition is met returns the Summary graph, the C matrices containing the 
   * probabilistic mapping between original vertices and super nodes in summary graph for the k-type vertices.
   * 
   * @param A - k-partite graph A, given as cell array with matrix A(i,j)=A(j,i)
   * 						describing weights between vertices of type i and those of type j
   * @param varargin - Vector containing additional input options.
   * 
   * @return Summary - k-partite Summary Graph
   * @return Cluster - The k probabilistic mappings between original vertices and super nodes in summary graph.
   * @return f - The last objective function value
   */
  def graphSumPlus(A: Array[Array[DenseMatrix[Double]]], varargin: Vector[Any] = Vector()): (Array[Array[DenseMatrix[Double]]], Array[DenseMatrix[Double]], Double) = {

    val k = A.length
    var n = DenseMatrix.zeros[Int](k,1)
    
    for (i <- 0 until k) {
      for (j <- 0 until k) {
        var nn = 0
        try {
          nn = A(i)(j).rows
        }catch{
          case _: Throwable => nn = 0
        }
                
        if (nn > 0) {
          if (n(i,0)== 0){
            n(i,0) = nn
          }
          else if (n(i,0) != nn && i==j) {
            throwError("need same sizes in k-partite graph description in matrix" + i + " " + j)
          }
        }

        try {
          nn = A(j)(i).cols
        } catch {
          case _: Throwable => nn = 0
        }
        
        if (n(i,0) == 0) {
          n(i,0) = nn
        } else if (n(i,0) != nn && i==j) {
          throwError("Need same sizes in k-partite graph description in matrix" + i + " " + j)
        }
        
      }
    }

    if (min(n) == 0L) {
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
        if (max(A(i)(j))== 0) {
          A(i)(j) = A(j)(i).t
        }
        else if(max(A(j)(i))!= 0){
          A(i)(j) = (A(i)(j) + A(j)(i).t) * 0.5
        }
      }
    }

    // matrix containing empty connections implying NO fit here, includes diagonal!
    var hasCon = DenseMatrix.zeros[Int](k, k)
    for (i <- 0 until k) {
      for (j <- 0 until k) {
        hasCon(i, j) = if (A(i)(j).rows > 0) 1 else 0;
      }
    }
    
    var flag = true
    var r = DenseMatrix.zeros[Int](k, 1)
    for (i <- 0 until k) {
      r(i,0) = n(i,0)
    }

    // Initialization
    val t1 = System.currentTimeMillis()
    var tupleInit = initilize(A, hasCon, n, r)
    var B = tupleInit._1
    var C = tupleInit._2
    var powerA = tupleInit._3
        
    val t2 = System.currentTimeMillis()
    println("Initialization time : " + (t2 - t1) / 1000 + " seconds.")

    var Cluster = Array.ofDim[DenseMatrix[Double]](k)
    var Summary = Array.ofDim[DenseMatrix[Double]](k, k)
    while (flag) {

      var tupleRefinement = refinement(B, C, A, r, res, hasCon)
      B = tupleRefinement._1
      C = tupleRefinement._2

      var fold = objective(A, B, C, k, hasCon)
      println("Old objective value is : " + fold)
      var tupleSearch = (C, B, r, false)
      if (sum(r) > sum(n)) {
        tupleSearch = search(B, C, k, r, true, n)
      } else {
        tupleSearch = search(B, C, k, r, false, n)
      }
      Cluster = tupleSearch._1
      Summary = tupleSearch._2
      r = tupleSearch._3
      var isreduced = tupleSearch._4

      if (isreduced) {
        var tupletupleRefinement2 = refinement(Summary, Cluster, A, r, res, hasCon)
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
  }

  /**
   * This function initializes a random k-partite symmary Graph and initializes the random k C-matrices which 
   * contain probabilistic mapping between original vertices and super nodes in summary graph.
   * 
   * @param A - k-partite graph A, given as cell array with matrix A(i,j)=A(j,i)
   * @param hasCon - k by k Matrix. Its elements contain 1 if it has connection on the cells of the k-partite graph A
   * @param n - Array of size k containing number of vertices of the original input Graph
   * @param r - Array of size k containing the number of clusters for each k-type vertex
   * 
   * @return B - k-partite Summary Graph
   * @return C - The probabilistic mapping between original vertices and super nodes in summary graph
   * @return powerA - The sum of squared elements of k-partite graph A needed to calculate convergence of objective function
   */
  def initilize(A: Array[Array[DenseMatrix[Double]]], hasCon: DenseMatrix[Int], n: DenseMatrix[Int], r: DenseMatrix[Int]) : (Array[Array[DenseMatrix[Double]]], Array[DenseMatrix[Double]], Double) = {
    val k = A.length;
    var B = Array.ofDim[DenseMatrix[Double]](k, k)
    var C = Array.ofDim[DenseMatrix[Double]](k)
    var powerA = 0.0
    val maxcluster = max(r)
    
    if (maxcluster < 50) {
      for (i <- 0 until k) {
        C(i) = DenseMatrix.rand(n(i,0), r(i,0))
        C(i) = C(i) /:/ tile(sum(C(i), Axis._1), 1, r(i,0))
      }
    } else {
      for (i <- 0 until k) {
        C(i) = DenseMatrix.zeros[Double](n(i,0), r(i,0))
        C(i) = myRandomInit(C(i))
      }
    }
   
    for (i <- 0 until k) {
      if (hasCon(i, i) == 1) {
        C(i) = LP(A(i)(i), C(i))
      }
    }
    
    for (i <- 0 until k) {
      for (j <- i until k) {
        if (hasCon(i, j) == 1) {
          if (j != i) {
            B(i)(j) = (DenseMatrix.rand(r(i,0), r(j,0)) :+= 0.0000001)
          } else {
            B(i)(j) = eye(r(i,0), r(j,0))
          }
          powerA += sum(A(i)(j) *:* A(i)(j))
        }
      }
    }

    (B, C, powerA)
  }

  /**
   * Returns a random C-Matrix with random connections between vertices and super nodes.
   * @param Y - The DenseMatrix C containing probabilistic mapping between original vertices and super nodes in summary graph
   * @return Y - Same matrix with random connections of original vertices to super nodes
   */
  def myRandomInit(Y: DenseMatrix[Double]): DenseMatrix[Double] = {
    val random = new java.security.SecureRandom
    val n = Y.rows
    val r = Y.cols
    for (j <- 0 until n) {
      var index = Math.round(random.nextDouble() * r);
      if (index < 1) {
        index = 1
      }
      if (index > r.toInt) {
        index = index % r
      }
      Y(j,index.toInt-1) = 1
    }
    Y
  }
  
  /**
   * Normalization function for the C-matrices used when initializing the summary graph
   * @param S - Similiarity matrix between a type k vertices.
   * @param x - The current C-matrix for the k type vertices.
   * 
   * @return y - Normalized C-matrix containing the probabilistic mapping between original 
   * 							vertices and super nodes in summary graph
   */
  
  def LP(S: DenseMatrix[Double], x: DenseMatrix[Double]): DenseMatrix[Double] = {
    var y = x.copy
    for (iter <- 1 until 80) {
      y = ((S * y) :*= 0.5) + (x :*= 0.5);
      y = y  /:/ tile(sum(y, Axis._1), 1, y.cols)
    }
    y = y :*= 2.0
    y
  }
  
  /**
   * This function updates the C-matrices containing the probabilistic mapping between original 
   * vertices and super nodes in summary graph and the k-partite Summary Graph.
   * The intuition of the multiplicative rule is that whenever the solution is smaller
   * than the local optimum, it multiplies with a larger value; otherwise, it multiplies with a
   * smaller value.
   * 
   * @param B - k-partite Summary Graph
   * @param C - The k probabilistic mappings between original vertices and super nodes in summary graph.
   */

  def refinement(B: Array[Array[DenseMatrix[Double]]], C: Array[DenseMatrix[Double]], A: Array[Array[DenseMatrix[Double]]], r: DenseMatrix[Int], res: Tuple5[DenseMatrix[Int], Int, Double, Boolean, Boolean], hasCon: DenseMatrix[Int]): (Array[Array[DenseMatrix[Double]]], Array[DenseMatrix[Double]], Array[Double]) = {
    var numchar = 0;
    var fs = Array.ofDim[Double](res._2)
    val myzero = 1e-18
    val k = A.length
    for (it <- 0 until res._2) {
      for (i <- 0 until k) {
        var N = DenseMatrix.zeros[Double](C(i).rows,C(i).cols)
        var D = DenseMatrix.zeros[Double](C(i).rows,C(i).cols)
        for (j <- i until k) {
          if (hasCon(i, j) == 1) {
            if (j != i) {
              N = N + A(i)(j)*C(j)*B(i)(j).t
              D = D + C(i)*B(i)(j)*C(j).t*C(j)*B(i)(j).t
            } else {
              N = N + A(i)(j)*C(i)
              D = D + diag(sum(A(i)(j), Axis._0)) * C(i) 
            }
          }
        }
        C(i) = C(i) *:* N /:/ maxDM(D, myzero)
        C(i) = C(i) /:/ tile(sum(C(i), Axis._1)+myzero, 1, r(i,0) )
      }

      for (i <- 0 until k) {
        for (j <- i + 1 until k) {
          if (hasCon(i, j) == 1) {
           B(i)(j) = B(i)(j) *:* (C(i).t*A(i)(j)*C(j)) /:/ maxDM((C(i).t*C(i)*B(i)(j)*C(j).t*C(j)), myzero )  
           B(j)(i) = B(i)(j).t
          }
        }
      }

      var f = 0.0
      for (i <- 0 until k) {
        for (j <- i until k) {
          if (hasCon(i, j) == 1) {
            if (j != i) {
              var tmp = A(i)(j) - C(i)*B(i)(j)*C(j).t
              f = f + sum( tmp :*= tmp )

            } else {
              var tmp = A(i)(i) - C(i)*C(i).t
              f = f + sum( tmp :*= tmp )
            }
          }

        }
      }
      fs(it) = f
    }

    (B, C, fs)
  }

  /**
   * Function to calculate the objective value needed for the termination condition.
   * @param A - MultiDimensional array of DenseMatrix containing the Graph and sim matrices between k-type vertices
   * @param B - k-partite Summary Graph
   */
  def objective(A: Array[Array[DenseMatrix[Double]]], B: Array[Array[DenseMatrix[Double]]], C: Array[DenseMatrix[Double]], k: Int, hasCon: DenseMatrix[Int]): Double = {
    var f = 0.0
    for (i <- 0 until k) {
      for (j <- i + 1 until k) {
        if (hasCon(i, j) == 1) {
          var dz = A(i)(j) - C(i)*B(i)(j)*C(j).t 
          f = f + sum( dz :*= dz ).toDouble
        }
      }
    }
    f
  }

  /**
   * This function searches for a better summary graph which means it deletes super nodes.
   * By deleting super nodes calls two other functions which perform the deletion of the super nodes for the k C-Matrices and for the
   * k-partite Summary Graph
   * 
   * @param B - k-partite Summary Graph
   * @param C - The k probabilistic mappings between original vertices and super nodes in summary graph.
   * @param k - number of the type of vertices
   * @param r - The array of size k containing the number of current clusters or super nodes for each k-type vertices
   * @param ishard - Boolean which decides the value for which supernodes to be removed.
   * @param n - Array of size k containing the number of vertices for each k type
   * 
   * @return C - The new C-matrices with reduced super nodes if condition was met
   * @return B - The new Summary Graph with reduced super nodes if condition was met
   * @return r - The new array containing the new number of super nodes for each k-type vertices
   * @return isreduced - Boolean value which tells if there was a reduction of super nodes
   */
  def search(B: Array[Array[DenseMatrix[Double]]], C: Array[DenseMatrix[Double]], k: Int, r: DenseMatrix[Int], ishard: Boolean, n: DenseMatrix[Int]): (Array[DenseMatrix[Double]], Array[Array[DenseMatrix[Double]]], DenseMatrix[Int], Boolean) = {
    var isreduced = false

    for (i <- 0 until k) {
      println("old number of clusters for" + i + "-type is " + r(i,0))
      breakable {
        if (r(i,0) < Math.max(3, n(i,0) / k)) {
          println("skip " + i + "-type")
          break;
        }
        var ni = C(i).rows
        var a = DenseMatrix.zeros[Int](1,r(i,0))

        for (j <- 0 until ni) {
          var index = argmax(C(i)(j, ::))
          a(0, index) += 1
        }

        var v = 0
        if (ishard) {
          var v = min(a)
        }

        val tupleClusterMinusmany = clusterMinusMany(C, i, a, v)
        var newC = tupleClusterMinusmany._1
        var isflag = tupleClusterMinusmany._2
        if (isflag) {
          isreduced = true
        }

        var newB = summaryMinusMany(B, i, a, v)
        r(i,0) = newC(i).cols
        println("new number of clusters for " + i + 1 + "-type is: " + r(i,0))
      }
    }

    (C, B, r, isreduced)
  }

  /**
   * Removes super nodes from the C-Matrices and create a new one with less columns.
   * @param C - The k probabilistic mappings between original vertices and super nodes in summary graph
   * @param i - The index indicating the k-type vertex for which the corresponded C-matrix to be modified
   * @param a - Bucket array of biggest indices for each column of the C-matrix
   * @param v - The value which dicided if the super node should be removed based on the values of a 
   * 
   * @return Cnew - The new C-Matrix
   */
  def clusterMinusMany(C: Array[DenseMatrix[Double]], i: Int, a: DenseMatrix[Int], v: Int): (Array[DenseMatrix[Double]], Boolean) = {
    var M = C(i)
    var Cnew = C
    val n1 = C(i).rows
    val n2 = C(i).cols
    var nc = 0
    var isreduced = false

    for (m1 <- 0 until a.cols) {
      if (a(0, m1) <= v) {
        nc += 1
        isreduced = true
      }
    }
    var Mnew = DenseMatrix.zeros[Double](n1,n2-nc)
    nc = 0
    for (m1 <- 0 until a.cols) {
      if (a(0, m1) > v) {
          for(x <- 0 until Mnew.rows){
            Mnew(x, nc) = M(x, m1);
          }
        nc = nc + 1
      }
    }

    Cnew(i) = Mnew
    (Cnew, isreduced)
  }

  /**
   * Removes super nodes from the k-partite Summary Graph and create a new one with less columns.
   * @param B - The k-partite summary graph
   * @param i - The index indicating the k-type vertex for which the corresponded k-partite Summary graph to be modified
   * @param a - Bucket array of biggest indices for each column of the k-partite Summary graph
   * @param v - The value which dicided if the super nodes should be removed based on the values of a 
   * 
   * @return Bnew - The new k-partite Summary graph
   */
  def summaryMinusMany(B: Array[Array[DenseMatrix[Double]]], i: Int, a: DenseMatrix[Int], v: Int): Array[Array[DenseMatrix[Double]]] = {
    var Bnew = B
    var n1 = B.length
    var n2 = B(0).length
    var nc = 0

    for (m1 <- 0 until a.cols) {
      if (a(0, m1) <= v) {
        nc = nc + 1;
      }
    }
    for (j <- 0 until n1) {
      breakable {
        var M = B(j)(i)
        if (M.cols < 1) {
          break;
        }
        var b1 = M.rows
        var b2 = M.cols
        var idx = 0

        var Mnew = DenseMatrix.zeros[Double](b1,b2-nc)
        
        for (m1 <- 0 until a.cols){
           if (a(0, m1) > v){
             for(x <- 0 until Mnew.rows){
               Mnew(x, idx) = M(x, m1);
             }
             idx=idx+1;
           }
        }
        Bnew(j)(i) = Mnew
      }
    }

    for (j <- 0 until n2) {
      breakable {
        var M = B(i)(j)
        if (M.rows < 1) {
          break
        }
        var b1 = M.rows
        var b2 = M.cols
        var idx = 0

        var Mnew = DenseMatrix.zeros[Double](b1-nc,b2)
        for (m1 <- 0 until a.rows){
           if (a(0, m1) > v){
             for(x <- 0 until Mnew.cols){
               Mnew(idx, x) = M(m1, x);
             }
             idx=idx+1;
           }
        }
        
        Bnew(i)(j) = Mnew
      }
    }
    Bnew
  }

  /**
   * Create an Indentity Matrix with different number of rows and columns
   * @param n - Rows of the wanted Identity matrix
   * @param m - Columns of the wanted Identity matrix
   * @return r - Identity Matrix
   */
  def eye(n: Int, m: Int): DenseMatrix[Double] = {
    val r = DenseMatrix.zeros[Double](n, m)
    for(i<-0 until r.rows){
      for(j<-0 until r.cols){
        if(i==j)
          r(i, j) = 1
      }
    }
    r
  }
  
  /**
   * Replace all elements of a DenseMatrix which are smaller than the 2nd parameter myzero with that value
   * @param X - DenseMatrix[Double] 
   * @param myzero - The minimum value possible in the DenseMatrix
   * @return Z - DenseMatrix with replaced values which where smaller than myzero
   */
  def maxDM(X: DenseMatrix[Double], myzero: Double) : DenseMatrix[Double] ={
    var Z = X.copy
    for(i <- 0 until Z.rows ){
      for(j<- 0 until Z.cols){
        if(Z(i, j) < myzero) 
          Z(i, j) = myzero
      }
    }
    Z
  }
    
  /**
   * Stop Spark Context if error occurs
   * @param msg - Message to show as info of the error
   */
  def throwError(msg: String) {
    sys.ShutdownHookThread {
      println(msg)
    }
    sc.stop()
  }
  
}


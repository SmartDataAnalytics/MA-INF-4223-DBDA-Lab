[![Codacy Badge](https://api.codacy.com/project/badge/Grade/da88f5d82fdb4c08a9640d023af36442)](https://www.codacy.com/app/DennisKubitza/Lab-DBDA-RDF-Kernels-for-Spark?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=DennisKubitza/Lab-DBDA-RDF-Kernels-for-Spark&amp;utm_campaign=Badge_Grade)
[![AUR](https://img.shields.io/aur/license/yaourt.svg)]()
# Lab-DBDA-RDF-Kernels-for-Spark

## Description
Implementation of methods for computing kernel functions for intersection graphs and intersection trees. Designed to process RDF data packed in Spark Resilient Distributed Datasets. Based on ideas described in [this beautiful publication](https://link.springer.com/content/pdf/10.1007%2F978-3-642-30284-8_16.pdf). Built on and requires [SANSA-Stack](https://github.com/SANSA-Stack/) for operating.

## Available methods and their usage

### intersectionAdjacencyMatrix()
Calculates adjacency matrix for intersection of two RDF graphs. This matrix is essential for implementation of matrix-based kernel calculation.
```scala
@param firstRDD Resilient Distributed Dataset containing entries of RDF triples.
@param secondRDD Resilient Distributed Dataset containing entries of RDF triples.
@return CoordinateMatrix containing '1' at the positions where edges exist and '0' otherwise.

def intersectionAdjacencyMatrix(firstRDD: RDD[Triple], secondRDD: RDD[Triple]) : CoordinateMatrix
```
### matrixPathKernel()
Calculates path kernel of a graph represented by adjacency matrix. Path kernel is calculated as a weighted sum of number of possible paths up to given length.
```scala
@param adjacencyMatrix CoordinateMatrix representing the graph structure.
@param depth maximal number of edges which a path can contain, i.e. maximal path length.
@param lambda discount factor, which allows to weight long and short paths differently; values bigger than 1 contribute to long paths and otherwise.
@return Double value of path kernel. 

def matrixPathKernel(adjacencyMatrix: CoordinateMatrix, depth: Int, lambda: Double) : Double
```

### matrixWalkKernel()
Calculates walk kernel of a graph represented by adjacency matrix. Walk kernel is calculated as a weighted sum of number of possible walks up to given length. 
```scala
@param adjacencyMatrix CoordinateMatrix representing the graph structure.
@param depth maximal number of edges which a walk can contain, i.e. maximal walk length.
@param lambda discount factor, which allows to weight long and short walks differently; values bigger than 1 contribute to long walks and otherwise.
@return Double value of walk kernel. 

def matrixWalkKernel(adjacencyMatrix: CoordinateMatrix, depth: Int, lambda: Double) : Double
```

### coordinateMatrixMultiply()
Helper function for matrix multiplication.
```scala
@param leftMatrix left product matrix of type CoordinateMatrix.
@param rightMatrix right product matrix of type CoordinateMatrix.
@return CoordinateMatrix which is a result of multiplication. 
 
def coordinateMatrixMultiply(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix
```

### msgPathKernel()
Calculates path kernel of an intersection of two RDF datasets represented by RDDs. Kernel is calculated as a weighted sum of number of possible paths up to given length. Paths are constructed iteratively by propagating messages over intersection graph.
```scala
@param firstRDD Resilient Distributed Dataset containing entries of RDF triples.
@param secondRDD Resilient Distributed Dataset containing entries of RDF triples.
@param depth maximal number of edges which a path can contain, i.e. maximal path length.
@param lambda discount factor, which allows to weight long and short paths differently; values bigger than 1 contribute to long paths and otherwise.
@return Double value of path kernel. 

def msgPathKernel(firstRDD: RDD[Triple], secondRDD: RDD[Triple], depth: Int, lambda: Double) : Double
```

### msgWalkKernel()
Calculates walk kernel of an intersection of two RDF datasets represented by RDDs. Kernel is calculated as a weighted sum of number of possible walks up to given length. Walks are constructed iteratively by propagating messages over intersection graph.

```scala
@param firstRDD Resilient Distributed Dataset containing entries of RDF triples.
@param secondRDD Resilient Distributed Dataset containing entries of RDF triples.
@param depth maximal number of edges which a path can contain, i.e. maximal path length.
@param lambda discount factor, which allows to weight long and short paths differently; values bigger than 1 contribute to long paths and otherwise.
@return Double value of walk kernel. 
 
def msgWalkKernel(firstRDD: RDD[Triple], secondRDD: RDD[Triple], depth: Int, lambda: Double) : Double
```


### constructIntersectionTree()
Calculates and builds the intersection Tree between two nodes of a given graph or null if no intersection tree exists within given depth.

```scala
@param graphRDD Resilient Distributed Dataset containing entries of RDF graph triples.
@param e1 ID of the first node.
@param e2 ID of the second node.
@param depth maximal number of hops from each node.
@param spark instance of SparkSession.
@return Graph which represents the intersection tree or null if such tree cannot be found. 

def constructIntersectionTree(graph: Graph[Node,Node], e1: Long, e2: Long, depth:Int, spark: SparkSession) : Graph[Null, String]
```
### fullSubtreeKernel()
Calculates the full subtree kernel for the graph interscection tree

```scala
@param g A Graph in the format outputed by constructIntersectionTree 
@param depth: The depth to calculate. Use the same depth as for the intersection Tree.
@param lambda: Scaling factor
@return The Full Subtree Kernel for the parameters.

def FullSubtreeKernel(g: Graph[Null,String], depth: Int, lambda: Double) : Double
```
### partialSubtreeKernel()
Calculates the partial subtree kernel for the graph intersection tree

```scala
@param g A Graph in the format outputed by constructIntersectionTree 
@param depth: The depth to calculate. Use the same depth as for the intersection Tree.
@param lambda: Scaling factor
@return The Full Subtree Kernel for the parameters.

def PartialSubtreeKernel(g: Graph[Null,String], depth: Int, lambda: Double) : Double
```


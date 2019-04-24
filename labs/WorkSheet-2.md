COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

**Lab Distributed Big Data Analytics**

Worksheet-2: **Getting started with Spark + Spark GraphX and Spark SQL operations**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen/), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu/),  [Denis Lukovnikov](http://sda.cs.uni-bonn.de/people/denis-lukovnikov/), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann/)

April 11, 2019

In this lab we are going to perform basic RDD and DataFrame operations such as actions and transformations (described on “Spark Fundamentals I”). We are going to cover caching as well in order to speed up any iterative job that we will have during our lab.
Furthermore, we will cover basic Spark SQL and Spark GraphX operations. Spark SQL provides the ability to write sql-like queries which can run on Spark. Their main abstraction is SchemaRDD which allows creating an RDD in which you can run SQL, HiveQL, and Scala. 
GraphX is the new Spark API for graphs and graph-parallel computation. At a high-level, GraphX extends the Spark RDD abstraction by introducing the Resilient Distributed Property Graph: a directed multigraph with properties attached to each vertex and edge.
In this lab, you will use SQL and GraphX to find out the subject distribution over nt file. The purpose is to demonstrate how to use the Spark SQL and GraphX libraries on Spark.

* * *

IN CLASS

* * *

1.  Spark basic RDDs & DataFrames operations
    - After a file ([page\_links\_simple.nt.bz2](http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2)) have been downloaded, unzipped, and uploaded on HDFS under `/yourname` folder you may need to create an RDD out of this file. This is created using the `spark context` and `textFile` method. Since spark transformations are considered being lazy evaluation, means nothing really happens there. We have to perform this just to tell spark that we want to create an RDD, containing data from the file.
    - Let's perform some RDD actions on this file by counting the number of triples in the RDD.
    - Use the `filter` transformation to return a new RDD with a subset of the triples on the file by checking if the first row contains “#”, which on .nt file represent a comment.
    - Since the data is going to be type of .nt file which inside contains rows of triples in format `<subject> <predicate> <object>` we may need to transform this data into a different format of representation. By using `map` function we will transform the data into `(Subject, 1)` which we are going to use for counting the subject distributions.
    - After we have transformed our RDD into `(Subject, 1)` - which assign every subject a counter (in this case 1) we may need to count how many time the specific subject have been used on our dataset. To do so, we will use `reduceByKey`
    - Collect and print subjects and their frequencies.
<hr>
------------------------------------------------------Solution------------------------------------------------------

```scala
package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession

object TestSpark extends App {

  val spark = SparkSession.builder
    .appName(s"WordCount example")
    .master("local[*]")
    .getOrCreate()

  val input = "src/main/resources/rdf.nt"

  val triplesLines = spark.sparkContext.textFile(input)

  triplesLines.take(5).foreach(println)

  triplesLines.cache()

  println(triplesLines.count())
  val removedComments = triplesLines.filter(!_.startsWith("#"))

  val triples = removedComments.map(data => TripleUtils.parsTriples(data))

  val mapSubject = triples.map(s => (s.subject, 1))

  mapSubject.take(5).foreach(println)

  val subject_freq = mapSubject.reduceByKey((a, b) => a + b) //(_+_)

  subject_freq.take(5).foreach(println)

  spark.stop()
}

object TripleUtils {

  def parsTriples(parsData: String): Triples = {
    val subRAngle = parsData.indexOf('>')
    val predLAngle = parsData.indexOf('<', subRAngle + 1)
    val predRAngle = parsData.indexOf('>', predLAngle + 1)
    var objLAngle = parsData.indexOf('<', predRAngle + 1)
    var objRAngle = parsData.indexOf('>', objLAngle + 1)

    if (objRAngle == -1) {
      objLAngle = parsData.indexOf('\"', objRAngle + 1)
      objRAngle = parsData.indexOf('\"', objLAngle + 1)
    }

    val subject = parsData.substring(1, subRAngle)
    val predicate = parsData.substring(predLAngle + 1, predRAngle)
    val `object` = parsData.substring(objLAngle + 1, objRAngle)

    Triples(subject, predicate, `object`)
  }

}

case class Triples(subject: String, predicate: String, `object`: String) {

  def isLangTag(resource: String) = resource.startsWith("@")
}
```

2. Spark SQL operations

    - After a file ([page\_links\_simple.nt.bz2](http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2)) have been downloaded, unzipped, and uploaded on HDFS under `/yourname` folder you may need to create an RDD out of this file.
    - First create a Scala class Triple containing information about a triple read from a file, which will be used as schema. Since the data is going to be type of .nt file which inside contains rows of triples in format <subject> <predicate> <object> we may need to transform this data into a different format of representation. Hint: Use map function.
    - Create an RDD of a Triple object
    - Use the filter transformation to return a new RDD with a subset of the triples on the file by checking if the first row contains “#”, which on .nt file represent a comment.
    - Run SQL statements using sql method provided by the SQLContext:
        - Taking all triples which are related to ‘Category:Events’
        - Taking all triples for predicate ‘author’.
        - Taking all triples authored by ‘Andre_Engels’
        - Count how many time the specific subject have been used on our dataset.

    - Since result is considered to be SchemaRDD, every RDD operations work out-of-the-box. By using map function collect and print out Subjects and their frequencies.

------------------------------------------------------Solution----------------------------------------------------------
```scala

object  SparkSQLlab {

def  main(args: Array[String]) = {

val  input  =  "src/main/resources/rdf.nt"  // args(0)  
  
val  spark  =SparkSession.builder  
            .master("local[*]")  
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
            .appName("SparkSQL example")  
            .getOrCreate()  
  
import  spark.implicits._  
  
val  tripleDF  = spark.sparkContext.textFile(input)  
                .map(TripleUtils.parsTriples)  
                .toDF()  
  
tripleDF.show()  
  
//tripleDF.collect().foreach(println(_))  
  
tripleDF.createOrReplaceTempView("triple")  
  
val  sqlText  =  "SELECT * from triple where subject = 'http://commons.dbpedia.org/resource/Category:Events'" 

val  triplerelatedtoEvents  = spark.sql(sqlText)  
  
triplerelatedtoEvents.collect().foreach(println(_))val  subjectdistribution  = spark.sql("select subject, count(*) from triple group by subject") 

println("subjectdistribution:")  
subjectdistribution.collect().foreach(println(_))  
    }    
}
```

3.  Spark GraphX operations
    - After a file ([page\_links\_simple.nt.bz2](http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2)) have been downloaded, unzipped, and uploaded on HDFS under /yourname folder you may need to create an RDD out of this file.
    - First create a Scala class Triple containing information about a triple read from a file. Since the data is going to be type of .nt file which inside contains rows of triples in format <subject> <predicate> <object> we may need to transform this data into a different format of representation. Hint: Use map function.
    - Use the filter transformation to return a new RDD with a subset of the triples on the file by checking if the first row contains “#”, which on .nt file represent a comment.
    - Perform these operations in order to transform your data into GraphX
        - Generate vertices by combining (Subject, Object) as VertexId and their value.x
        - Create Edges by using subject as a key to join within vertices and generate Edge into format (s\_index, obj\_index, predicate)
    - Compute connected components for triples containing “author” as a predicate.
    - Compute triangle count.
    - List top 5 connected component by applying pagerank over them.

------------------------------------------------------Solution----------------------------------------------------------
```scala

object  SparkGraphXlab {
def  main(args: Array[String]) = {
val  input  =  "src/main/resources/rdf.nt"  // args(0)  
  
val  spark  =  SparkSession.builder  
            .master("local[*]")  
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
            .appName("GraphX example")  
            .getOrCreate()  
  
val  tripleRDD  = spark.sparkContext.textFile(input)  
.map(TripleUtils.parsTriples)  
  
val  tutleSubjectObject  = tripleRDD.map { x => (x.subject, x.`object`) }

type  VertexId  =  Long  
  
val  indexVertexID  = (tripleRDD.map(_.subject) union tripleRDD.map(_.`object`)).distinct().zipWithIndex()
val  vertices:  RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))

val  tuples  = tripleRDD.keyBy(_.subject).join(indexVertexID).map( 
{
    case (k, (Triple(s, p, o), si)) => (o, (si, p))  
})

val  edges:  RDD[Edge[String]] = tuples.join(indexVertexID).map({
    case (k, ((si, p), oi)) =>  Edge(si, oi, p)  
})  
  
val  graph  =  Graph(vertices, edges)  
  
graph.vertices.collect().foreach(println(_))  
  
println("edges")  
graph.edges.collect().foreach(println(_))  
  
val  subrealsourse  = graph.subgraph(t => t.attr ==  "http://commons.dbpedia.org/property/source")  
println("subrealsourse")  
subrealsourse.vertices.collect().foreach(println(_))  
  
val  conncompo  = subrealsourse.connectedComponents()
val  pageranl  = graph.pageRank(0.0001)  
  
val  printoutrankedtriples  = pageranl.vertices.join(graph.vertices)  
.map({ case (k, (r, v)) => (k, r, v) })  
.sortBy(5  - _._2)  
  
println("printoutrankedtriples")  
printoutrankedtriples.take(5).foreach(println(_))  
 }  
}
```
* * *

AT HOME

* * *

1.  Read and explore
    - Spark Programming Guide
    - RDD API Examples
    - DataFrame API Examples
    - Spark Streaming Programming Guide
    - Spark SQL, DataFrames and Datasets Guide
    - GraphX Programming Guide
    - Spark Cluster Overview
    - Spark Configuration, Monitoring and tuning

2.  RDF Class Distribution - count the usage of respective classes of a RDF dataset._Hint_: Class fulfils the `rule(?predicate = rdf:type && ?object.isIRI()))`.
    - Read the nt file into an RDD of triples.
    - Apply map function for separating triples into `(Subject, Predicate, Object)`
    - Apply filter transformation for defining the respective classes.
    - Count the frequencies of Object and map them into `(Object, count)`.
    - Return the top 100 classes used in the dataset.
    - Try to do steps (a - e) within DataFrames.

3. RDF Class Distribution - using Spark SQL - count the usage of respective classes of a RDF dataset.
    Hint: Class fulfils the rule(?predicate = rdf:type && ?object.isIRI())).
        - Read the nt file into an RDD of triples.
        - Apply map function for separating triples into (Subject, Predicate, Object)
        - Apply filter transformation for defining the respective classes.
        - Count the frequencies of Object by using sql statement
        - Return the top 100 classes used in the dataset.

4.  Using GraphX To Analyze a Real Graph
    - Count the number of vertices and edges in the graph
    - How many resources are on your graph?
    - What is the max in-degree of this graph?
    - Which triple are related to ‘Category:Events’
    - Run Pagerank for 50 iterations.
    - Compute similarity between two nodes - using Spark GraphX
        - Apply different similarity measures
            - Jaccard similarity
            - Edit distance

5.  Further readings
    - [Spark: Cluster Computing with Working Sets](http://static.usenix.org/legacy/events/hotcloud10/tech/full_papers/Zaharia.pdf)
    - [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
    - [Clash of the Titans: MapReduce vs. Spark for Large Scale Data Analytics](http://www.vldb.org/pvldb/vol8/p2110-shi.pdf)
    - [Spark SQL: Relational Data Processing in Spark](https://amplab.cs.berkeley.edu/wp-content/uploads/2015/03/SparkSQLSigmod2015.pdf)
    - [Shark: SQL and Rich Analytics at Scale](http://people.csail.mit.edu/matei/papers/2013/sigmod_shark.pdf)
    [GraphX: Graph Processing in a Distributed Dataflow Framework](https://amplab.cs.berkeley.edu/wp-content/uploads/2014/09/graphx.pdf)


COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

**Lab Distributed Big Data Analytics**

Worksheet-3: **Spark GraphX and Spark SQL operations**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen/), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu/), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann/)

October 25, 2018

In this lab we are going to perform basic Spark SQL and Spark GraphX operations (described on “Spark Fundamentals II”). Spark SQL provides the ability to write sql-like queries which can run on Spark. Their main abstraction is SchemaRDD which allows creating an RDD in which you can run SQL, HiveQL, and Scala.  
GraphX is the new Spark API for graphs and graph-parallel computation. At a high-level, GraphX extends the Spark RDD abstraction by introducing the Resilient Distributed Property Graph: a directed multigraph with properties attached to each vertex and edge.

In this lab, you will use SQL and GraphX to find out the subject distribution over nt file. The purpose is to demonstrate how to use the Spark SQL and GraphX libraries on Spark.

* * *

IN CLASS

* * *

1.  Spark SQL operations

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

def  main(args: Array\[String\]) = {

val  input  =  "src/main/resources/rdf.nt"  // args(0)  
  
val  spark  =SparkSession.builder  
            .master("local\[*\]")  
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

2.  Spark GraphX operations
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
def  main(args: Array\[String\]) = {
val  input  =  "src/main/resources/rdf.nt"  // args(0)  
  
val  spark  =  SparkSession.builder  
            .master("local\[*\]")  
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
    - Spark SQL, DataFrames and Datasets Guide
    - GraphX Programming Guide
    - RDF Class Distribution - using Spark SQL - count the usage of respective classes of a RDF dataset.
    Hint: Class fulfils the rule(?predicate = rdf:type && ?object.isIRI())).
        - Read the nt file into an RDD of triples.
        - Apply map function for separating triples into (Subject, Predicate, Object)
        - Apply filter transformation for defining the respective classes.
        - Count the frequencies of Object by using sql statement
        - Return the top 100 classes used in the dataset.

2.  Using GraphX To Analyze a Real Graph
    - Count the number of vertices and edges in the graph
    - How many resources are on your graph?
    - What is the max in-degree of this graph?
    - Which triple are related to ‘Category:Events’
    - Run Pagerank for 50 iterations.
    - Compute similarity between two nodes - using Spark GraphX
        - Apply different similarity measures
            - Jaccard similarity
            - Edit distance

3.  Further readings
    - [Spark SQL: Relational Data Processing in Spark](https://amplab.cs.berkeley.edu/wp-content/uploads/2015/03/SparkSQLSigmod2015.pdf)
    - [Shark: SQL and Rich Analytics at Scale](http://people.csail.mit.edu/matei/papers/2013/sigmod_shark.pdf)
    - [GraphX: Graph Processing in a Distributed Dataflow Framework](https://amplab.cs.berkeley.edu/wp-content/uploads/2014/09/graphx.pdf)
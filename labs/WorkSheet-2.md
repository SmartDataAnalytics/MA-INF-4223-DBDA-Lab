COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

**Lab Distributed Big Data Analytics**

Worksheet-2: **Getting started with Spark**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen/), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu/), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann/)

October 18, 2018

In this lab we are going to perform basic RDD and DataFrame operations such as actions and transformations (described on “Spark Fundamentals I”). We are going to cover caching as well in order to speed up any iterative job that we will have during our lab.

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

* * *

AT HOME

* * *

1.  Read and explore
    - Spark Programming Guide
    - RDD API Examples
    - DataFrame API Examples
    - Spark Streaming Programming Guide
    - Spark Cluster Overview
    - Spark Configuration, Monitoring and tuning

2.  RDF Class Distribution - count the usage of respective classes of a RDF dataset._Hint_: Class fulfils the `rule(?predicate = rdf:type && ?object.isIRI()))`.
    - Read the nt file into an RDD of triples.
    - Apply map function for separating triples into `(Subject, Predicate, Object)`
    - Apply filter transformation for defining the respective classes.
    - Count the frequencies of Object and map them into `(Object, count)`.
    - Return the top 100 classes used in the dataset.
    - Try to do steps (a - e) within DataFrames.

3.  Further readings
    - [Spark: Cluster Computing with Working Sets](http://static.usenix.org/legacy/events/hotcloud10/tech/full_papers/Zaharia.pdf)
    - [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
    - [Clash of the Titans: MapReduce vs. Spark for Large Scale Data Analytics](http://www.vldb.org/pvldb/vol8/p2110-shi.pdf)
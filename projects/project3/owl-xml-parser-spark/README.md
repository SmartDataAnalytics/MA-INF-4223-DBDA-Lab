# Parser for Owl files of OWLXML syntax
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

## Description
Library to read OWL files in OWL/XML format and convert extracted axioms into [Spark](https://spark.apache.org) [RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
It allows files to reside in HDFS as well as in a local file system and distributes them across Spark RDDs.
It follows the structure of [sansa-stack](http://sansa-stack.net/).

## Package Structure

The module `owl-xml-parser-spark` consists of two packages:

- `owl.spark.parsing` - contains code to parse OWL files of OWL/XML syntax.
- `owl.spark.rdd` - contains Spark-specific code to build RDDs of OWL Axioms - `OWLXMLSyntaxOWLAxiomsRDDGenerator` 
contains main function to read owl file and build RDD of OWLAxioms out of it.




## Usage

The following Scala code shows how to read an OWL file in OWL/XML Syntax (be it a local file or a file residing in HDFS) into a Spark RDD:
```scala
import owl.spark._
import org.apache.spark.sql.SparkSession

val syntax = Syntax.OWLXML
val sparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("OWL/XML Parser")
    .getOrCreate()

val input = "hdfs://..."
val rdd = sparkSession.owl(syntax)(input)

rdd.foreach(println(_))

```


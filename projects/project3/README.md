# SANSA-RDF : Reading more types of RDF data

## Description
SANSA RDF is a library to read RDF files into [Spark](https://spark.apache.org). SANSA RDF Reader is an extension of io package of SANSA RDF Reader for reading N-Quads, Turtle and RDF/XML serialization formats of RDF.

This package reads N-Quads, Turtle and RDF/XML files and loads them into **RDD**, **DataFrame** and [GraphX](https://spark.apache.org/graphx/)'s **Graph** of [Spark](https://spark.apache.org).

### SANSA RDF Spark
The main application class is `sansa_rdf.App`.
The application requires as application argument:

- path to the input folder containing the data as *.nq*, *.rdf* or *.ttl* (e.g. `data/stw.rdf`)

## Running the application on a Spark

To run the application on a standalone Spark cluster

1. Setup a Spark cluster
2. Build the application with Maven

  ```
  cd /path/to/application
  mvn clean package
  ```

3. Submit the application to the Spark cluster

  ```
  spark-submit \
		--class sansa_rdf.App \
		--master spark://spark-master:7077 \
 		target/RDF_Reader-1.0-SNAPSHOT.jar \
		/data/input
  ```

  and for running each object individually replace the value of *--class* with one of *sansa_rdf.io.NQuadReader*, *sansa_rdf.io.TurtleReader* or *sansa_rdf.io.XmlReader*.

package owl.spark.parsing

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import owl.spark.rdd.OWLExpressionsRDD
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.OWLXMLDocumentFormat
import org.semanticweb.owlapi.io.StringDocumentSource
import org.semanticweb.owlapi.model.OWLAxiom
import org.semanticweb.owlapi.model._
import scala.compat.java8.StreamConverters._

/**
Singleton instance of type OWLXMLSyntaxParsing
  */
object OWLXMLSyntaxParsing {

  // "static"/"class" - level functionality to define OWLXML syntax pattern
  val OWLXMLSyntaxPattern: Map[String,Map[String,String]] = Map(
    "versionPattern" -> Map(
      "beginTag" -> "<?xml",
      "endTag" -> "?>"
    ).withDefaultValue("Pattern for begin and end tags not found"),
    "prefixPattern" -> Map(
      "beginTag" -> "<rdf:RDF",
      "endTag" -> ">"
    ).withDefaultValue("Pattern for begin and end tags not found"),
    "owlClassPattern" -> Map(
      "beginTag" -> "<owl:Class",
      "endTag" -> "</owl:Class>"
    ).withDefaultValue("Pattern for begin and end tags not found"),
    "owlDataTypePropertyPattern" -> Map(
      "beginTag" -> "<owl:DatatypeProperty",
      "endTag" -> "</owl:DatatypeProperty>"
    ).withDefaultValue("Pattern for begin and end tags not found"),
    "owlObjectPropertyPattern" -> Map(
      "beginTag" -> "<owl:ObjectProperty",
      "endTag" -> "</owl:ObjectProperty>"
    ).withDefaultValue("Pattern for begin and end tags not found")
  )
}


/**
  * Trait to support the parsing of input OWL files in OWLXML syntax. This
  * trait mainly defines how to make axioms from input OWLXML syntax
  * expressions.
  */
trait OWLXMLSyntaxParsing {

  private val logger = Logger(classOf[OWLXMLSyntaxParsing])

  /**
    * definition to build Owl Axioms out of expressions
    * @param xmlString : xmlVersion string
    * @param prefixString: owlxml prefix string
    * @param expression: owl expressions of owlxml syntax
    * @return A set consisting OWL Axioms built out of owl expressions
    * */
  def makeAxiom(xmlString:String, prefixString:String, expression: String): Set[OWLAxiom] = {

    // compose a string consisting of xml version, owlxml prefixes, owlxml expressions
    val modelText = xmlString + "\n" + prefixString + "\n" + expression + "\n" + "</rdf:RDF>"

    // object to manage the ontology
    val manager = OWLManager.createOWLOntologyManager()

    // load an ontology from file
    val ontology = try {
      // load an ontology from an input stream
      manager.loadOntologyFromOntologyDocument(new StringDocumentSource(modelText))
    } catch {
      case e: OWLOntologyCreationException => null
    }

    val extractedAxioms = if (ontology!=null){
      // get the ontology format
      val format = manager.getOntologyFormat(ontology)
      val owlxmlFormat = new OWLXMLDocumentFormat

      // copy all the prefixes from OWL document format to OWL XML format
      if (format != null && format.isPrefixOWLDocumentFormat) {
        owlxmlFormat.copyPrefixesFrom(format.asPrefixOWLDocumentFormat)
      }

      // get axioms from the loaded ontology
      val axioms = ontology.axioms().toScala[Set]
      axioms
    } else {
      logger.warn("No ontology was created for expression \n" + expression)
      null
    }
    extractedAxioms
  }
}


class OWLXMLSyntaxExpressionBuilder(spark: SparkSession, val filePath: String) extends OWLXMLSyntaxParsing {

  // define an empty RDD of generic type
  private var owlRecordRDD : org.apache.spark.rdd.RDD[String] = spark.sparkContext.emptyRDD

  // get xml version string as an RDD
  private val xmlVersionRDD = getRecord(OWLXMLSyntaxParsing.OWLXMLSyntaxPattern("versionPattern"))

  // get owlxml prefix string as an RDD
  private val owlPrefixRDD = getRecord(OWLXMLSyntaxParsing.OWLXMLSyntaxPattern("prefixPattern"))

  // get pattern for begin and end tags for owl expressions to be specified for hadoop stream
  private val owlRecordPatterns = OWLXMLSyntaxParsing.OWLXMLSyntaxPattern.filterKeys(_ != "versionPattern").filterKeys(_!= "prefixPattern")

  /**
    * definition to get owl expressions from hadoop stream as RDD[String]
    * @param pattern: a Map object consisting of key-value pairs to define begin and end tags
    * @return OWlExpressionRDD
    * */
  def getRecord(pattern: Map[String,String]): RDD[String] ={
    val config = new JobConf()
    val beginTag:String = pattern("beginTag")
    val endTag:String = pattern("endTag")
    config.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    config.set("stream.recordreader.begin", beginTag) // start Tag
    config.set("stream.recordreader.end", endTag) // End Tag
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(config, filePath)
    val rawRDD: RDD[(Text, Text)] = spark.sparkContext.hadoopRDD(config,
      classOf[org.apache.hadoop.streaming.StreamInputFormat], //input format
      classOf[org.apache.hadoop.io.Text], //class for the key
      classOf[org.apache.hadoop.io.Text]) //class for the value

    val recordRDD: RDD[String] = rawRDD.map { (x) => (x._1.toString) }
    recordRDD
  }

  /**
    * definition to get owl expressions for each patterns defined in OWLXMLSyntaxPattern
    * @return : a tuple consisting of (RDD for xml version, RDD for owlxml prefixes, RDD for owl expressions)
    * */
  def getOwlExpressions():(OWLExpressionsRDD,OWLExpressionsRDD,OWLExpressionsRDD) = {
    val unionOwlExpressionsRDD = for {
      (pattern, tags) <- owlRecordPatterns
    } yield owlRecordRDD union getRecord(tags)

    val owlExpressionsRDD = unionOwlExpressionsRDD.reduce(_ union _)
    (xmlVersionRDD,owlPrefixRDD,owlExpressionsRDD)
  }
}






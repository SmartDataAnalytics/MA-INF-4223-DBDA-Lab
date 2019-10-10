package owl.spark.rdd

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.io.OWLParserException
import owl.spark.parsing.OWLXMLSyntaxParsing


object OWLXMLSyntaxOWLAxiomsRDDBuilder extends OWLXMLSyntaxParsing {

  private val logger = Logger(this.getClass)

  /**
    * definition to build OWLAxioms out of OWL file
    * @param spark: spark session
    * @param filePath: absolute path of the OWL file
    * @return OwlAxioms: RDD[Set[OwlAxioms]s]
    * */
  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {
    build(spark, OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, filePath))
  }

  /**
    * definition to build OwlAxioms out of expressions
    * @param spark: spark session
    * @param owlRecordsRDD: a tuple consisting of RDD records for xmlVersion string, owlxml prefix string and owl expressions
    * */
  def build(spark: SparkSession, owlRecordsRDD: (OWLExpressionsRDD,OWLExpressionsRDD,OWLExpressionsRDD)): OWLAxiomsRDD = {

    // get RDD consisting of xmlVersion
    val xmlVersionRDD = owlRecordsRDD._1.first()

    // get RDD consisting of owlxml prefix
    val owlPrefixRDD = owlRecordsRDD._2.first()

    // get RDD consisting of owl expressions in owlxml syntax
    val owlExpressionsRDD = owlRecordsRDD._3

    // for each owl expressions try to extract axioms in it,
    // if not print the corresponding expression for which axioms could not extracted using owl api
    owlExpressionsRDD.map(expressionRDD => {
      try makeAxiom(xmlVersionRDD,owlPrefixRDD,expressionRDD)
      catch {
        case exception: OWLParserException =>
          logger.warn("Parser error for line " + expressionRDD + ": " + exception.getMessage)
          null
      }
    }).filter(_ != null)
  }

}

package owl.spark.rdd

import org.apache.spark.sql.SparkSession
import owl.spark.parsing.{OWLXMLSyntaxExpressionBuilder, OWLXMLSyntaxParsing}

object OWLXMLSyntaxOWLExpressionsRDDBuilder extends Serializable with OWLXMLSyntaxParsing  {

  /**
    * definition to build owl expressions of syntax OWLXML out of OWL file
    * @param spark: spark session
    * @param filePath: absolute path of the OWL file
    * @return RDDs consisting following expressions
    *         xmlVersion string : <?xml version...?>
    *         owlPrefix string : <rdf:RDF xmlns:.. >
    *         owlExpression string : <owl:Class,ObjectProperty,DatatypeProperty.../owl>
  */
  def build(spark: SparkSession, filePath: String):(OWLExpressionsRDD,OWLExpressionsRDD,OWLExpressionsRDD) = {
    val builder = new OWLXMLSyntaxExpressionBuilder(spark,filePath)
    val (xmlVersionRDD,owlPrefixRDD,owlExpressionsRDD) = builder.getOwlExpressions()
    (xmlVersionRDD,owlPrefixRDD,owlExpressionsRDD)
  }

}

package owl.spark

import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model.OWLAxiom

/**
  * Package object to define constants to be used in entire rest of the package
  */
package object rdd {
  /**
    * type declaration to define an alias to hold RDD[String]s character snippets extracted in OWL XML syntax
    * Ex: RDD[<owl:Class
    *       ...
    *     </owl:Class>]
    */
  type OWLExpressionsRDD = RDD[String]

  /**
    * type declaration to define an alias to hold RDD[Set[OWLAxiom]s]
    * where each OWLAxiom is of type OWLClass, OWLObjectProperty, OWLDataProperty etc.,
    */
  type OWLAxiomsRDD = RDD[Set[OWLAxiom]]
}
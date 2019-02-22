package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.DataFrame

object Experiments {

  /** Calculates Constraint Satisfaction.
    *
    *  @param ns n when the condition became true.
    *  @param n Final n.
    */
  def constraintSatisfaction(ns: Long, n: Long): Float = {
    return ns/n
  }

  /** Calculates Reduction Ratio.
    *
    *  @param trainingSet training set dataframe.
    *  @param optimalScheme optimal scheme returned by the main algorithm.
    */
  def reductionRatio(trainingSet: DataFrame, optimalScheme: String): Float = {

    val columnNames = trainingSet.columns

    val satisfyingRows = trainingSet.filter { row =>
      var scheme = optimalScheme

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme)
    }

    val notSatisfyingRows = trainingSet.exceptAll(satisfyingRows)

    return 1.toFloat - (satisfyingRows.count().toFloat/notSatisfyingRows.count().toFloat)
  }

  /** Calculates Pair Completeness.
    *
    *  @param trainingSet training set dataframe.
    *  @param optimalScheme optimal scheme returned by the main algorithm.
    */
  def pairCompleteness(trainingSet: DataFrame, optimalScheme: String): Float = {

    val columnNames = trainingSet.columns

    val satisfyingRows = trainingSet.filter { row =>
      var scheme = optimalScheme
      val humanOracle = row.getString(row.length - 1)

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme) && humanOracle == "M"
    }

    val matchedRows = trainingSet.filter { row =>
      val humanOracle = row.getString(row.length - 1)
      humanOracle == "M"
    }

    return satisfyingRows.count().toFloat/matchedRows.count().toFloat
  }

  /** Calculates Pair Quality.
    *
    *  @param trainingSet training set dataframe.
    *  @param optimalScheme optimal scheme returned by the main algorithm.
    */
  def pairQuality(trainingSet: DataFrame, optimalScheme: String): Float = {

    val columnNames = trainingSet.columns

    val satisfyingRows = trainingSet.filter { row =>
      var scheme = optimalScheme
      val humanOracle = row.getString(row.length - 1)

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme) && humanOracle == "M"
    }

    val schemeSatisfyRows = trainingSet.filter { row =>
      var scheme = optimalScheme

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme)
    }

    return satisfyingRows.count().toFloat/schemeSatisfyRows.count().toFloat
  }

  /** Calculates F Measure.
    *
    *  @param pairCompleteness pair completeness calculated using above method.
    *  @param pairQuality pair quality calculated using about method.
    */
  def fMeasure(pairCompleteness: Float, pairQuality: Float): Float = {
    return (2 * pairCompleteness * pairQuality)/(pairQuality + pairCompleteness)
  }
}

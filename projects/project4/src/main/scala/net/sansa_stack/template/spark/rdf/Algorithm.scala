package net.sansa_stack.template.spark.rdf

import scala.Predef._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.rockymadden.stringmetric.phonetic.RefinedSoundexMetric
import org.apache.spark.sql.types._


object Algorithm {

  def main(args: Array[String]): Unit = {

    // Pass the dataset file as input argument
    // -i "src/main/resources/dataset.csv"

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName(s"Active Blocking Scheme Learning For Entity Resolution")
      .getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load(input)

    val featureVectors = generateFeatureVectors(spark, df)

    algorithm(spark, featureVectors)

    spark.stop
  }

  // Returns optimal scheme
  /** Runs the main algorithm and prints the experimental results.
    *
    *  @param spark the spark session object.
    *  @param featureVectors set of feature vectors.
    */
  def algorithm(spark: SparkSession, featureVectors: DataFrame): String = {

    var n: Long = 0
    var oldN: Long = 0
    var stop = false

    val humanOracleThresholdPercentage = 75
    val budget = 170
    var S = featureVectors.columns
    var optimalScheme = ""
    val errorRateThreshold = 0.3

    val sampleSize = 45
    var X = randomSample(featureVectors, sampleSize)

    var trainingSet = spark.emptyDataFrame

    var constraintSatisfactionNs: Long = 0

    while (n < budget && !stop) {

      S.foreach { scheme =>
        val featureVectorsExceptSample = featureVectors.exceptAll(X)
        val gamma = calculateGamma(X, scheme)
        val deficiency = (gamma * X.count()).toInt

        X = addSample(X, featureVectorsExceptSample,
          scheme, deficiency, gamma <=0)

        n = X.count()
      }

      if (n == oldN) {
        stop = true
      }
      oldN = n

      trainingSet = humanOracle(spark, X, humanOracleThresholdPercentage)

      val trainingSetColumnNames = trainingSet.columns

      val schemeErrorRates = S.map { s => spark.sparkContext.doubleAccumulator }

      trainingSet.foreach { row =>
        val humanOracle = row.getString(row.length - 1)
        var schemes = S.clone()

        for (i <- 0 to row.length - 2) {
          val columnName = trainingSetColumnNames(i)
          val value = row.getInt(i)

          schemes = schemes.map { scheme =>
            scheme.replaceAll(columnName, value.toString)
          }
        }

        val schemeResults = schemes.map { scheme =>
          BlockingSchemeCalculator.calculate(scheme)
        }

        for (i <- 0 to schemeResults.length - 1) {
          val result = schemeResults(i)

          if ((result && humanOracle == "N") || (!result && humanOracle == "M")) {
            schemeErrorRates(i).add(1)
          }
        }
      }

      val finalErrorRates = schemeErrorRates.map { error =>
        error.value / trainingSet.count()
      }

      var minIndex = 0
      var min: Double = finalErrorRates(0)

      for (i <- 0 to finalErrorRates.length - 1) {
        val value = finalErrorRates(i)
        if (value < min) {
          minIndex = i
          min = value
        }
      }

      optimalScheme = S(minIndex)

      val sPrevious = S.clone()
      S = Array[String]()

      if (min <= errorRateThreshold) {

        constraintSatisfactionNs = n
        val optimumFP = falsePositives(spark, optimalScheme, trainingSet)

        for (i <- 0 to sPrevious.length - 1) {
          val scheme = sPrevious(i)
          val newSchemes = generateBlockingSchemes(Array(scheme), optimalScheme, "and")

          if (!newSchemes.isEmpty) {
            val andScheme = newSchemes(0)

            val fp = falsePositives(spark, scheme, trainingSet)

            val andSchemeFP = falsePositives(spark, andScheme, trainingSet)

            if ((optimumFP >= andSchemeFP) && fp >= andSchemeFP) {
              S = S :+ andScheme
            } else {
              S = S :+ scheme
            }
          }
        }

      } else {

        val optimalFN = falseNegatives(spark, optimalScheme, trainingSet)
        val optimalTP = truePositives(spark, optimalScheme, trainingSet)

        for (i <- 0 to sPrevious.length - 1) {
          val scheme = sPrevious(i)
          val fn = falseNegatives(spark, scheme, trainingSet)
          val tp = truePositives(spark, scheme, trainingSet)

          val newSchemes = generateBlockingSchemes(Array(scheme), optimalScheme, "or")

          if (!newSchemes.isEmpty) {
            val orScheme = newSchemes(0)
            val orSchemeFN = falseNegatives(spark, orScheme, trainingSet)
            val orSchemeTP = truePositives(spark, orScheme, trainingSet)

            if (((optimalFN/optimalTP) >= (orSchemeFN/orSchemeTP)) && ((fn/tp) >= (orSchemeFN/orSchemeTP))) {
              S = S :+ orScheme
            } else {
              S = S :+ scheme
            }
          }
        }
      }
    }

    println(s"Optimal Scheme ${optimalScheme}")
    val reductionRatio = Experiments.reductionRatio(trainingSet, optimalScheme)
    val completness = Experiments.pairCompleteness(trainingSet, optimalScheme)
    val quality = Experiments.pairQuality(trainingSet, optimalScheme)
    val fmeas = Experiments.fMeasure(completness, quality)
    val cSatisfaction = Experiments.constraintSatisfaction(constraintSatisfactionNs, n)

    println("*** Results ***")

    println(s"Budget ${budget}")
    println(s"Human Oracle Percentage ${humanOracleThresholdPercentage}")
    println(s"Initial Sample Size ${sampleSize}")
    println(s"Sample Size ${trainingSet.count()}")
    println(s"Error Rate Threshold ${errorRateThreshold}")
    println(s"Reduction Ratio ${reductionRatio}")
    println(s"Pair Completeness ${completness}")
    println(s"Pair Quality ${quality}")
    println(s"F Measure ${fmeas}")
    println(s"Constraint Satisfaction ${cSatisfaction}")

    return optimalScheme
  }

  /** Calculates false positives given a training set dataframe and a blocking scheme.
    *
    *  @param spark the spark session object.
    *  @param scheme blocking scheme.
    *  @param trainingSet training set dataframe.
    */
  def falsePositives(spark: SparkSession, scheme: String, trainingSet: DataFrame): Long = {

    val count = spark.sparkContext.longAccumulator
    val columnNames = trainingSet.columns

    trainingSet.foreach { row =>
      val humanOracle = row.getString(row.length - 1)
      var currentScheme = scheme

      for (i <- 0 to row.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        currentScheme = currentScheme.replaceAll(columnName, value.toString)
      }

      val result = BlockingSchemeCalculator.calculate(currentScheme)

      if (result && humanOracle == "N") {
        count.add(1)
      }
    }

    return count.value
  }

  /** Calculates false negatives given a training set dataframe and a blocking scheme.
    *
    *  @param spark the spark session object.
    *  @param scheme blocking scheme.
    *  @param trainingSet training set dataframe.
    */
  def falseNegatives(spark: SparkSession, scheme: String, trainingSet: DataFrame): Long = {

    val count = spark.sparkContext.longAccumulator
    val columnNames = trainingSet.columns

    trainingSet.foreach { row =>
      val humanOracle = row.getString(row.length - 1)
      var currentScheme = scheme

      for (i <- 0 to row.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        currentScheme = currentScheme.replaceAll(columnName, value.toString)
      }

      val result = BlockingSchemeCalculator.calculate(currentScheme)

      if (!result && humanOracle == "M") {
        count.add(1)
      }
    }

    return count.value
  }

  /** Calculates true positives given a training set dataframe and a blocking scheme.
    *
    *  @param spark the spark session object.
    *  @param scheme blocking scheme.
    *  @param trainingSet training set dataframe.
    */
  def truePositives(spark: SparkSession, scheme: String, trainingSet: DataFrame): Long = {

    val count = spark.sparkContext.longAccumulator
    val columnNames = trainingSet.columns

    trainingSet.foreach { row =>
      val humanOracle = row.getString(row.length - 1)
      var currentScheme = scheme

      for (i <- 0 to row.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        currentScheme = currentScheme.replaceAll(columnName, value.toString)
      }

      val result = BlockingSchemeCalculator.calculate(currentScheme)

      if (result && humanOracle == "M") {
        count.add(1)
      }
    }

    return count.value
  }

  /** Human oracle that labels the feature vectors based on a threshold percentage.
    *
    *  @param spark the spark session object.
    *  @param sampleFeatureVectors sample feature vectors to label.
    *  @param thresholdPercentage threshold percentage used by human oracle to decide if its a match or not.
    */
  def humanOracle(spark: SparkSession, sampleFeatureVectors: DataFrame, thresholdPercentage: Float): DataFrame = {

    val newSample = sampleFeatureVectors.withColumn("Label", lit("M"))

    val rdd = newSample.rdd.map { row =>
      val valuesArray = row.toSeq.toArray

      var numberOf1 = 0
      val total = row.length - 2

      for (i <- 0 to total) {
        val value = row.getInt(i)

        if (value == 1) {
          numberOf1 = numberOf1 + 1
        }
      }

      val totalFeatures = row.length - 1
      val percentage: Float = (numberOf1.toFloat/totalFeatures.toFloat) * 100
      var result = "N"
      if (percentage >= thresholdPercentage) {
        result = "M"
      }

      valuesArray(row.length - 1) = result

      Row.fromSeq(valuesArray.toSeq)
    }

    spark.createDataFrame(rdd, newSample.schema)
  }

  /** Generates feature vectors given a dataset.
    *
    *  @param spark the spark session object.
    *  @param dataframe dataset for which feature vectors are generated.
    */
  def generateFeatureVectors(spark: SparkSession, dataframe: DataFrame): DataFrame = {

    // Ignore first column because it is considered to be a primary key column.
    // Use only string datatype columns because soundex supports only string comparisons.

    val stringColumns = dataframe.dtypes
      .slice(1, dataframe.dtypes.length)
      .filter { col =>
      col._2 == "StringType"
    }.map { col => dataframe.col(col._1) }

    val df = dataframe.select(stringColumns: _*)
    var featureVectors: Seq[Row] = Seq()
    val rddWithIndex = df.rdd.zipWithIndex()

    rddWithIndex.collect().foreach { value =>
      val xRecord = value._1
      val xIndex = value._2

      var xFeatures: List[String] = List()

      for (i <- 0 to xRecord.length - 1) {
        val feature = xRecord.getString(i)
        xFeatures = xFeatures :+ feature
      }

      rddWithIndex.filter { v => v._2 > xIndex }
        .collect()
        .foreach { value =>

          val yRecord = value._1
          val  yIndex = value._2
          var soundexResult: Seq[Int] = Seq()

          var yFeatures: List[String] = List()

          for (i <- 0 to yRecord.length - 1) {
            val feature = yRecord.getString(i)
            yFeatures = yFeatures :+ feature
          }

          for (i <- 0 to xFeatures.length - 1) {
            val xFeature = xFeatures(i)
            val yFeature = yFeatures(i)

            val soundex = RefinedSoundexMetric.compare(xFeature, yFeature).getOrElse(false)

            if (soundex) {
              soundexResult = soundexResult :+ 1
            } else {
              soundexResult = soundexResult :+ 0
            }
          }

          featureVectors = featureVectors :+ Row.fromSeq(soundexResult)
      }
    }

    val featureVectorsRdd = spark.sparkContext.parallelize(featureVectors)

    var schema = new StructType()
    df.schema.foreach { s =>
      schema = schema.add(s.name, IntegerType)
    }

    val featureVectorsDf = spark.createDataFrame(featureVectorsRdd, schema)

    return featureVectorsDf
  }

  /** Generates a random sample without replacement of a dataframe (Feature vectors in our case).
    *
    *  @param dataframe the feature vectors dataframe object.
    *  @param size size of the sample generated.
    */
  def randomSample(dataframe: DataFrame, size: Int): DataFrame = {
    val sample = dataframe.sample(false, 1D*size/dataframe.count)
    return sample
  }

  /** Calculates gamma using a formula given in the algorithm.
    *
    *  @param sample the feature vectors sample for which gamma is to be calculated.
    *  @param blockingScheme Blocking scheme used for feature vectors against which gamma is calculated.
    */
  def calculateGamma(sample: DataFrame, blockingScheme: String): Float = {

    val columnNames = sample.columns
    val satisfyingRows = sample.filter { row =>
      var scheme = blockingScheme

      for (i <- 0 to columnNames.length - 1) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme)
    }

    val notSatisfyingRows = sample.exceptAll(satisfyingRows)
    val gamma: Float = ((satisfyingRows.count() - notSatisfyingRows.count()).toFloat / sample.count())

    return gamma
  }

  /** Adds new similar or dissimilar rows based on gamma to create a balanced training feature vectors set.
    *
    *  @param sampleFeatureVectors sample of feature vectors.
    *  @param featureVectorsExceptSample complete set of feature vectors except the current sample.
    *  @param blockingScheme blocking scheme used.
    *  @param deficiency number of rows needed to create the balanced training set.
    *  @param similar indicates whether similar or dissimilar samples are needed to create a balanced training set.
    */
  def addSample(sampleFeatureVectors: DataFrame,
                featureVectorsExceptSample: DataFrame,
                blockingScheme: String,
                deficiency: Int,
                similar: Boolean): DataFrame = {


    val columnNames = featureVectorsExceptSample.columns

    val filteredRows = featureVectorsExceptSample.filter { row =>
      var scheme = blockingScheme

      for (i <- 0 to columnNames.length - 1) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      if (similar) {
        BlockingSchemeCalculator.calculate(scheme)
      } else {
        !BlockingSchemeCalculator.calculate(scheme)
      }
    }

    val newRows = filteredRows.limit(math.abs(deficiency))

    return sampleFeatureVectors.union(newRows)
  }

  /** Generates new blocking schemes given a optimum scheme, set of schemes and a condition(and or or) .
    *
    *  @param schemes set of blocking schemes.
    *  @param optimumScheme optimum blocking scheme.
    *  @param condition "and" or "or" condition to be used to create new schemes.
    */
  def generateBlockingSchemes(schemes: Array[String],
                              optimumScheme: String,
                              condition: String): Array[String] = {

    val schemesExceptOptimum = schemes.filter { scheme =>
      scheme != optimumScheme
    }

    val newSchemes = schemesExceptOptimum.map { scheme =>
      s"(${scheme} ${condition} ${optimumScheme})"
    }

    return newSchemes
  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Active Blocking Scheme Learning For Entity Resolution") {

    head("Active Blocking Scheme Learning For Entity Resolution")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in CSV format).")
  }
}

package net.sansa_stack.template.spark.rdf

import java.net.URL
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithLBFGS }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.regression.{ IsotonicRegression, IsotonicRegressionModel }

object App {
  
  def decisionTree(input: String, name: String) = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Regression (" + input + ")")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, input)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val (trainingData, testData) = (splits(0), splits(1))
    
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    
    // Evaluate model on test instances and compute test error
    val predictionAndLabels = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    
    val accuracy = predictionAndLabels.filter(r => r._1 == r._2).count().toDouble / testData.count()
    
        // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    
    println(metrics.confusionMatrix)
    println("Accuracy = " + accuracy)
    
    // Save and load model
    //model.save(sc, "target/decisionTree/" + name)
    //val sameModel = DecisionTreeModel.load(sc, "target/decisionTree/" + name)
  }
  
  def logicalRegression(input: String, name: String) = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Regression (" + input + ")")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, input)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(4)
      .run(training)      
      
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
        
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    
    println(metrics.confusionMatrix)
    println(s"Accuracy = $accuracy")

    // Save and load model
    //model.save(sc, "target/logicalRegression/" + name)
    //val sameModel = LogisticRegressionModel.load(sc, "target/logicalRegression/" + name)
  }
  
  def naiveBayes(input: String, name: String) = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Regression (" + input + ")")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, input)

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println(metrics.confusionMatrix)
    println("Accuracy = " + accuracy)

    // Save and load model
    //model.save(sc, "target/naiveBayes/" + name)
    //val sameModel = NaiveBayesModel.load(sc, "target/naiveBayes/" + name)
  }

  def IsotonicRegression(input: String, name: String) = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Regression (" + input + ")")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val data = MLUtils.loadLibSVMFile(sc, input).cache()

    // Create label, feature, weight tuples from input data with weight set to default value 1.0.
    val parsedData = data.map { labeledPoint =>
      (labeledPoint.label, labeledPoint.features(0), 1.0)
    }

    // Split data into training (60%) and test (40%) sets.
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    // Create isotonic regression model from training data.
    // Isotonic parameter defaults to true so it is only shown for demonstration
    val model = new IsotonicRegression().setIsotonic(true).run(training)

    // Create tuples of predicted and real labels.
    val predictionAndLabels = test.map { point =>
      val predictedLabel = model.predict(point._2)
      (predictedLabel, point._1)
    }
    
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy

    println(metrics.confusionMatrix)    
    println(s"Accuracy = $accuracy")

    // Save and load model
    //model.save(sc, "target/isotonicRegression/" + name)
    //val sameModel = IsotonicRegressionModel.load(sc, "target/isotonicRegression/" + name)
  }
  
  
  def main(args: Array[String]) = {

    var fileName = "language"
    //var fileName = "country"
    //var fileName = "city"

    //var input = "src/main/resources/triple/" + fileName;
    //val output = "src/main/resources/feature/" + fileName;
    //makeFeatureVector(input, output)

    val inputFile = "src/main/resources/feature/" + fileName + "/part-00000";

    //logicalRegression(inputFile, fileName)
    //decisionTree(inputFile, fileName)
    //naiveBayes(inputFile, fileName)
    //IsotonicRegression(inputFile, fileName)
  }
    
  def makeFeatureVector(input: String, output: String) = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("WordCount example (" + input + ")")
      .getOrCreate()

    val triples = sparkSession.sparkContext.textFile(input)

    val removeCommentRows = triples.filter(!_.startsWith("#"))

    val parsedTriples = removeCommentRows.map(parsTriples)

    val predicate = parsedTriples.map(predicateTriples)

    val predicate1 = predicate.map(f => (f.predicate, (f.objectIsLink, 1)))

    val predicate2 = predicate1.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).sortBy(_._2._2, false)
    
    val predicate3 = predicate2.zipWithIndex()

    val nrOfPredicates = predicate3.count()

    val predicate4 = predicate3.map(f => (f, nrOfPredicates))
        
    val finalPredicate = predicate4.map(featuresExtract)
    
    finalPredicate.coalesce(1, true).saveAsTextFile(output)
    
    print("FINISHED")

    sparkSession.stop
  }

  def featuresExtract(parsData: (((String, (Int, Int)), Long), Long) ) = {
    val predicate = parsData._1._1._1
    val arrNames = predicate.contains("#") match { case true => predicate.split("#"); case false => predicate.split('/') }

    val predicateName = arrNames.apply(arrNames.length - 1)
    val isEnglish = predicateName.matches("\\w+") match { case true => 1; case false => 0 }

    var numberOfWords = Character.isUpperCase(predicateName.charAt(0)) match { case true => 0; case false => 1 }

    if (isEnglish != 0) {
      for (x <- predicateName)
        if (Character.isUpperCase(x))
          numberOfWords += 1
    } else
      numberOfWords = 1

    val frequency = parsData._1._1._2._2
    val containsNumber = predicateName.matches(".*\\d+.*") match { case true => 1; case false => 0 }
    val ontOrRaw = predicateName.toLowerCase().contains("dbpedia-owl") match { case true => 1; case false => 0 }
    val answIsLink = parsData._1._1._2._1.toFloat / parsData._1._1._2._2

    val _1 = frequency != 0 match { case true => "1:" + frequency + " "; case false => "" }
    val _2 = numberOfWords != 0 match { case true => "2:" + numberOfWords + " "; case false => "" }
    val _3 = containsNumber != 0 match { case true => "3:" + containsNumber + " "; case false => "" }
    val _4 = isEnglish != 0 match { case true => "4:" + isEnglish + " "; case false => "" }
    val _5 = ontOrRaw != 0 match { case true => "5:" + ontOrRaw + " "; case false => "" }
    val _6 = answIsLink != 0 match { case true => "6:" + answIsLink; case false => "" }

    val totalNumber = parsData._2
    val index = parsData._1._2
    
    var y = 0

    if( (0.25 * totalNumber <= index) && (index <= 0.5 * totalNumber) )
      y = 1
    else if( (0.5 * totalNumber <= index) && (index <= 0.75 * totalNumber) )
      y = 2
    else if( index >= 0.75 * totalNumber )
      y = 3
    
    y + " " +_1 + _2 + _3 + _4 + _5 + _6
  }

  def parsTriples(parsData: String): Triples = {
    val subRAngle = parsData.indexOf('>')
    val predLAngle = parsData.indexOf('<', subRAngle + 1)
    val predRAngle = parsData.indexOf('>', predLAngle + 1)
    var objLAngle = parsData.indexOf('<', predRAngle + 1)
    var objRAngle = parsData.length() - 1

    if (objRAngle == -1) {
      objLAngle = parsData.indexOf('\"', objRAngle + 1)
      objRAngle = parsData.indexOf('\"', objLAngle + 1)
    }

    val subject = parsData.substring(1, subRAngle)
    val predicate = parsData.substring(predLAngle + 1, predRAngle)
    val `object` = parsData.substring(objLAngle + 1, objRAngle)

    Triples(subject, predicate, `object`)
  }

  def predicateTriples(triple: Triples): PredicateTriple = {
    val isLink = triple.`object`.contains("link") match { case true => 1; case false => 0 }
    PredicateTriple(triple.predicate, isLink)
  }

  case class Triples(subject: String, predicate: String, `object`: String)
  case class PredicateTriple(predicate: String, objectIsLink: Int)
}
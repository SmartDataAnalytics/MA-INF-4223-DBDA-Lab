package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import java.net.{ URI => JavaURI }
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import net.sansa_stack.inference.utils.TripleUtils
import org.apache.spark.graphx.lib.ShortestPaths
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.util.ArrayList
import scala.util.control.Breaks._
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._

object RulesMining {

  var featureMap: HashMap[String, String] = new HashMap[String, String] //map of nodes with feature
  var compTriples = List.newBuilder[Triple[String, List[String], String]] //list of composition triples
  var instanceTransactions: ListBuffer[HashSet[String]] = new ListBuffer[HashSet[String]] //list of instance transactions
  var targetConceptNodesList: ListBuffer[String] = new ListBuffer[String] //nodes in graph with feature as target concept
  var targetConceptNeighboursList: ListBuffer[Long] = new ListBuffer[Long] //immediate nodes of target concept in the graph
  var targetConceptVertexId: Long = 0 //vertexId of targetConceptNodes in graph
  var targetConceptNeighboursId: Long = 0 //vertexId of neighbours of targetConcept
  val query: String = "Patient,Report,{Disease,Drug,damageIndex}" //query pattern "targetConcept,context,{features+}"
  type VertexId = Long // vertex id for every node vertex in graph
  var testList: List[String] = List.empty[String] //list of modified composition triples for item generation
  var finalListReport: List[String] = List.empty[String] //list of item transactions

  System.setProperty("hadoop.home.dir", "G:/Bonn/Sem2/BigDataLab/"); //File location should be specified
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"RulesMining $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|            Reading Input           |")
    println("======================================")

    //Reading the nTriples file
    val lang = Lang.NTRIPLES
    val triplesRDD = spark.rdf(lang)(input)
    val tripleRDD = triplesRDD.map(triple => (triple.getSubject.toString, triple.getPredicate.toString, triple.getObject.toString))
    updateFeatures()
    val turtleSubjectObject = tripleRDD.map { x => (x._1, x._3) }

    val indexVertexID = (tripleRDD.map(_._1) union tripleRDD.map(_._3)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))

    val tuples = tripleRDD.keyBy(_._1).join(indexVertexID).map(
      {
        case (k, (Triple(s, p, o), si)) => (o, (si, p))
      })

    val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    val graph = Graph(vertices, edges)

    println()
    println("|              Vertices              |")
    graph.vertices.collect().foreach(println(_))

    println()
    println("|               Edges                |")
    graph.edges.collect().foreach(println(_))

    createCompositionTriples(query, graph)

    println()
    println("======================================")
    println("|        Instance Transactions       |")
    println("======================================")

    instanceTransactions.result().foreach(println(_))

    println()
    println("======================================")
    println("|        Composition Triples         |")
    println("======================================")

    compTriples.result().foreach(println(_))

    itemTransactions()
    println()
    println("======================================")
    println("|           Item Transactions        |")
    println("======================================")

    finalListReport.foreach(println(_))

    val transactions = spark.sparkContext.parallelize(Seq(
      Array("Treatment.Drug.Methotrexate", "Treatment.Drug.Corticosteroids"),
      Array("Diagnosis.Disease.SystemicArthritis"),
      Array("Rheumatology.Disease.Bad Rotation", "Rheumatology.Disease.Malformation", "Rheumatology.Rheumatology.damageIndex->15"),
      Array("Treatment.Drug.Methotrexate"),
      Array("Diagnosis.Disease.Arthritis"),
      Array("Rheumatology.Disease.Malformation", "Rheumatology.Rheumatology.damageIndex->10")))

    println()
    println("======================================")
    println("|           Association Rules        |")
    println("======================================")
    association_rules(transactions)
    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple reader example") {

    head(" Triple reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triple format)")

    help("help").text("prints this usage text")
  }

  //get vertex id of node from graph based on name
  def getVertexId(graph: Graph[String, String], vertextName: String): Long = {
    val vert = graph.vertices.filter {
      case (_, name) => {
        (name.substring(name.lastIndexOf("/") + 1).equals(vertextName) ||
          name.substring(name.lastIndexOf("/") + 1).contains("\"" + vertextName + "\""))
      }
    }
    if (vert.map(_._1).isEmpty()) {
      return -1
    }
    return vert.map(_._1).first()
  }

  //get all set of nodes in path between two vertices in graph {(2,5),(5,4),(4,6)}
  def getNodesInPath(graph: Graph[String, String], src: Long, dest: Long, pathList: ListBuffer[(Long, Long)]): ListBuffer[(Long, Long)] = {
    if (src != -1 && dest != -1) {
      val res = graph.triplets.collect {
        case t if t.srcId == src => t.dstId
      }
      res.collect().map(obj => {
        if (obj != null) {
          pathList.append((src, obj))
          if (obj != dest)
            getNodesInPath(graph, obj, dest, pathList)
        }
      })
    }
    return pathList
  }

  //get final list of nodes in the path between two vertices in graph {2,5,4,6}
  def getRefinedPathList(pathList: ListBuffer[(Long, Long)], src: Long, dest: Long, nodeList: ListBuffer[Long]): ListBuffer[Long] = {
    nodeList.append(dest)
    val temp = pathList.filter { case (_, destId) => destId == dest }
    if (temp.isEmpty)
      return null
    val sourceNode = pathList.filter { case (_, destId) => destId == dest }(0)._1
    if (sourceNode != src) {
      nodeList.append(sourceNode)
      getRefinedPathList(pathList, src, sourceNode, nodeList)
    } else if (sourceNode == src) {
      nodeList.append(src)
    }
    return nodeList
  }

  //updating nodes in graph with features based on ontology
  def updateFeatures() {

    featureMap("PTN_XY21") = "Patient"
    featureMap("Male") = "Gender"
    featureMap("VISIT1") = "Visit"
    featureMap("VISIT2") = "Visit"
    featureMap("RHEX1") = "Rheumatology"
    featureMap("RHEX2") = "Rheumatology"
    featureMap("DIAG1") = "Diagnosis"
    featureMap("DIAG2") = "Diagnosis"
    featureMap("TREAT1") = "Treatment"
    featureMap("TREAT2") = "Treatment"
    featureMap("ULTRA1") = "Ultrasonography"
    featureMap("ULTRA2") = "Ultrasonography"
    featureMap("ULTRA3") = "Ultrasonography"
    featureMap("Malformation") = "Disease"
    featureMap("Bad Rotation") = "Disease"
    featureMap("Arthritis") = "Disease"
    featureMap("SystemicArthritis") = "Disease"
    featureMap("DT1") = "DrugTherapy"
    featureMap("DT2") = "DrugTherapy"
    featureMap("DT3") = "DrugTherapy"
    featureMap("Knee") = "Joint"
    featureMap("Wrist") = "Joint"
    featureMap("Rheumatology") = "Report"
    featureMap("Diagnosis") = "Report"
    featureMap("Treatment") = "Report"
    featureMap("Methotrexate") = "Drug"
    featureMap("Corticosteroids") = "Drug"
    featureMap("10") = "damageIndex"
    featureMap("15") = "damageIndex"
  }

  //get vertex name of list of nodes from graph based on ID
  def getVertexName(graph: Graph[String, String], nodeList: ListBuffer[Long]): ListBuffer[String] = {
    var vertexNames: ListBuffer[String] = new ListBuffer[String]
    nodeList.foreach(nodeId => {
      val vert = graph.vertices.filter { case (id, _) => id == nodeId }
      if (!vert.map(_._2).isEmpty()) {
        val name = vert.map(_._2).first()
        if (name.contains("/"))
          vertexNames += name.substring(name.lastIndexOf("/") + 1)
        else
          vertexNames += name
      }
    })
    return vertexNames
  }

  //creation of composition triples based on targetConcept and Features
  def createCompositionTriples(queryPattern: String, graph: Graph[String, String]) {
    var pathList: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]
    var nodeList: ListBuffer[Long] = new ListBuffer[Long]
    var instanceTransactionList: HashSet[String] = new HashSet[String]
    var features = ListBuffer[String]()
    val targetConcept: String = queryPattern.substring(0, queryPattern.indexOf(","))
    val contextList = featureMap.filter { case (_, value) => value.trim.equalsIgnoreCase(targetConcept.trim) }
    val contextNodes = contextList.keySet

    contextNodes.foreach(nodes => {
      targetConceptVertexId = getVertexId(graph, nodes)
      graph.edges.foreach(edge => {
        if (edge.srcId == targetConceptVertexId) {
          targetConceptNeighboursList += edge.dstId
        }
      })
      targetConceptNeighboursList.foreach(destId => {
        graph.vertices.foreach(dest => {
          if (dest._1 == destId) {
            val temp = dest._2.substring(dest._2.lastIndexOf("/") + 1)
            targetConceptNodesList += temp
          }
        })
      })
    })

    if (queryPattern.substring(queryPattern.indexOf("{") + 1, queryPattern.indexOf("}")).contains(",")) {
      val featList = queryPattern.substring(queryPattern.indexOf("{") + 1, queryPattern.indexOf("}")).split(",").toList
      featList.foreach(feat => features += feat)
    } else
      features += queryPattern.substring(queryPattern.indexOf("{") + 1, queryPattern.indexOf("}"))

    targetConceptNodesList.foreach(contextName => {
      features.foreach(feature => {
        val featureList = featureMap.filter { case (_, value) => value.equals(feature) }
        val featureNodes = featureList.keySet
        featureNodes.foreach(node => {
          pathList = getNodesInPath(graph, getVertexId(graph, contextName), getVertexId(graph, node), pathList)
          if (!pathList.isEmpty) {
            nodeList = getRefinedPathList(pathList, getVertexId(graph, contextName), getVertexId(graph, node), nodeList)
            if (nodeList != null && !nodeList.isEmpty) {
              val path = getVertexName(graph, nodeList.distinct.reverse)
              if (path != null && path.length > 0) {
                var comp = path.slice(0, path.length - 1).toList
                val triple = (targetConcept, comp, path(path.length - 1))
                testList :::= List(triple._1, triple._2.toString(), triple._3)
                compTriples += triple
                instanceTransactionList += (path(path.length - 1))
              }
            }
          }
          nodeList = new ListBuffer[Long]
          pathList = new ListBuffer[(Long, Long)]
        })
      })
      instanceTransactions += instanceTransactionList
      instanceTransactionList = new HashSet[String]
    })
  }

  //Generation of Item Transactions
  def itemTransactions() = {
    //get subject and predicate and for every subject if predicate is damage index do if or else
    val context: String = query.substring(query.indexOf(",") + 1, query.indexOf("{") - 1)
    var MSC_i = ""
    var MSC_j = ""
    var MSC_k = ""
    var count = 0
    var tempStr1 = ""
    var resStr = ""
    var tempStr = ""
    var finalList: List[String] = List.empty[String]

    testList.foreach(k => {
      if (count == 0 && resStr != "") {
        finalList :::= List(resStr)
        resStr = ""
      }
      count += 1
      if (count == 1) {
        MSC_i = k
      } else if (count == 2) {
        MSC_j = k.substring(5, k.length - 1)
        if (MSC_j.split(", ").length > 1)
          tempStr = MSC_j.split(", ")(1)
        else
          tempStr = MSC_j

        tempStr1 = tempStr
        resStr += MSC_j.split(", ")(0) + "!" + MSC_j.split(", ")(1) + "!{" + featureMap(tempStr)
      } else {
        MSC_k = k
        if (MSC_k contains "xsd") {
          MSC_k = MSC_k.split('"')(1)
          resStr += "." + featureMap(tempStr1) + "." + featureMap(MSC_k) + "->" + MSC_k + "}\n"
        } else if (MSC_k(0) == '"') {
          MSC_k = MSC_k.substring(1, MSC_k.length - 1)
          resStr += "." + featureMap(MSC_k) + "." + MSC_k + "}\n"
        } else {
          resStr += "." + featureMap(MSC_k) + "." + MSC_k + "}\n"
        }
        count = 0
      }
    })
    finalList :::= List(resStr)
    var uniqueStr = ""
    var t1 = ""
    var z = ""
    var j = -1
    var flag: Boolean = false
    finalList.foreach(l => {
      var count1 = 0
      var k = 0
      j += 1
        var x = l.split("!")(2)
      x = x.substring(1, x.length - 2)
      if (context == featureMap(l.split("!")(0))) {
        if (uniqueStr contains l.split("!")(0)) {
          t1 += ", " + x
          flag = true
        } else {
          if (flag) {
            t1 += "}"
            finalListReport :::= List(t1)
            t1 = ""
            flag = false
          }
          t1 = "{" + x
          uniqueStr += l.split("!")(0)
        }
      } else if (context == featureMap(featureMap(l.split("!")(1)))) {
        if (count1 == 0) {
          var currentUniqueStr = ""
          finalList.zipWithIndex.foreach {
            case (m, n) => {
              if (n >= j) {
                var y = m.split("!")(2)
                y = y.substring(1, y.length - 2)
                z = l.split("!")(0) + m.split("!")(1)
                if ((uniqueStr contains z) && k != 0) {
                  if (z == currentUniqueStr)
                    t1 += ", " + y
                } else {
                  if (uniqueStr contains z) {
                    k = 0
                    flag = true
                  } else if (!flag) {
                    t1 = "{" + y
                    currentUniqueStr = z
                    uniqueStr += currentUniqueStr
                    flag = true
                    k = 1
                  }
                }
              }
            }
          }
          count1 += 1
        }
        if (t1 != "") {
          t1 += "}"
          finalListReport :::= List(t1)
          t1 = ""
        }
        flag = false
      }
    })
    if (t1 != "") {
      t1 += "}"
      finalListReport :::= List(t1)
      t1 = ""
    }
  }

  // Generating the Association rules
  def association_rules(transactions: RDD[Array[String]]) {
    
    val freqItems = transactions
      .flatMap(itemstr =>
        (itemstr.combinations(1) ++ itemstr.combinations(2)).map(x => (x.toList, 1L)))
      .reduceByKey(_ + _)
      .map { case (itemstr, cnt) => new FreqItemset(itemstr.toArray, cnt) }

    val AR = new AssociationRules()
      .setMinConfidence(0.4)

    val result = AR.run(freqItems)

    result.collect().foreach { rule =>
      println(s"[${rule.antecedent.mkString(",")}=>${rule.consequent.mkString(",")} ]" +
        s" ${rule.confidence}")
    }
    
  }
}
package foil

import java.util.ArrayList
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KnowledgeBase {

	private var baseHolder: DataSource = null
	private var posHolder: DataSource = null
	private var negHolder: DataSource = null

	def declarePositiveExamples() {

	}

	def load {
		posHolder = DataSource.apply("/foil/pos.data")
		negHolder = DataSource.apply("/foil/neg.data")
		baseHolder = DataSource.apply("/foil/bg.data")
	}

	def print {
		println("Base Knowledge:")
		baseHolder.tupleMap.foreach(println(_))
		baseHolder.predicateMap.foreach(predicate => println(predicate._2.name + " " + predicate._2.arity))

		println("\nPositive:")
		posHolder.tupleMap.foreach(println(_))

		println("\nNegative:")
		negHolder.tupleMap.foreach(println(_))

		println("\nTarget variables:")
		println(generateTargetVariables)

		println("\nCandidates generation:")
		println(generateCandidates)
	}

	def isTargetPredicate(predicate: String): Boolean = {
		if (posHolder != null) {
			posHolder.literals.contains(predicate)
		} else {
			true
		}
	}

	def base = baseHolder
	def pos = posHolder
	def neg = negHolder

	/*
	 * number of bits to signal that tuple is positive/negative
	 */
	def entropy(n_pos: Double, n_neg: Double) = {
		-math.log(n_pos / (n_pos + n_neg)) / math.log(2)
	}

	/*
	 * weighted inomation gain
	 * ic_i, ic_(i+1), n_i_++
	 */
	def wig(ic_prev: Double, ic_next: Double, n_i: Double) = {
		n_i * (ic_prev - ic_next)
	}

	def foilAlgorithm(targetTuples: List[List[String]], target: (String, ArrayList[Term]), bodyPredicates: Map[List[Term], String]) = {
		var N = 0 // n++_(i) TODO: check it !!!
		var n = 0 // n+_(i+1)

		var tuplesIntersection = targetTuples.toSet //Set[List[String]]()
		bodyPredicates.foreach(predicate => {

			val predicateTuples = baseHolder.tupleMap(predicate._2)
			val positionList = Term.positionList(target, predicate._1)

			Main.debug("\n" + predicate.toString())
			Main.debug("Target tupples: " + targetTuples)
			Main.debug("Predicate tupples: " + predicateTuples.toString())

			var newBaseKnowledge = List[List[String]]()
			var newTargetTuples = List[List[String]]() // to check existence of negative patterns

			targetTuples.foreach(targetTuple => {

				var N_added = false
				predicateTuples.foreach(predicateTuple => {
					var n_added = true
					for (index <- 0 until positionList.size()) {
						val current = positionList.get(index)
						val term = current._2._2 // obtain Var or Atom object

						n_added = n_added && (targetTuple(current._1) == predicateTuple(current._2._1)) && term.isInstanceOf[Var]

						//Main.debug(targetTuple(current._1) + " " + tuple(current._2._1) + " " + n_added)
					}

					if (n_added) {
						// we save consistent tuple from base knowledge
						newBaseKnowledge = newBaseKnowledge ::: List(predicateTuple)
						newTargetTuples = newTargetTuples ::: List(targetTuple)
						//Main.debug(positionList + " " + targetTuple + " " + tuple)
						n += 1
						if (!N_added) {
							N += 1
							N_added = true
						}
						//Main.debug("n++ = " + N + "; n+ = " + n)
					}
				})
			})

			/*if (tuplesIntersection.isEmpty) {
  	    tuplesIntersection = newTargetTuples.toSet
  	  } else {*/
			tuplesIntersection = tuplesIntersection.intersect(newTargetTuples.toSet)
			// }

			Main.debug(newBaseKnowledge.toString())
			Main.debug(newTargetTuples.toString())
		})

		Main.debug("Set of tuples: " + tuplesIntersection.toString())
		//Main.debug("n++ = " + N + "; n+ = " + n)
		(tuplesIntersection, (N, n))
	}

	/**
	 * Comparator for map members based on gain. Returns unit with
	 * highest gain.
	 */
	def getMaxGain(a: ((String, List[Term]), (Double, (Set[List[String]], Set[List[String]]))),
		b: ((String, List[Term]), (Double, (Set[List[String]], Set[List[String]])))) = {
		val gainA = a._2._1
		val gainB = b._2._1
		if (gainA > gainB) a else b
	}

	/**
	 * Generate list of all variable combinations for
	 * given candidate.
	 */
	def expandCandidate(c: (String, ArrayList[List[Term]])) = {
		var res = ArrayBuffer[(String, List[Term])]()
		val vars = c._2.toArray()

		for (v <- vars) res += ((c._1, v.asInstanceOf[List[Term]]))
		res.toArray
	}

	/**
	 * Calculate gain for predicate-variables combination.
	 */
	def calcGain(c: (String, List[Term]),
		target: (String, ArrayList[Term]),
		bodyPredicates: Map[List[Term], String],
		positiveExamples: List[List[String]],
		negativeExamples: List[List[String]]) = {
		val predicateName = c._1 // obtain right-side predicate name
		val varsCombinations = c._2 // and all its possible variables combinations
		val rightSideVars = varsCombinations // iterator.next()
		val predicates = updateRuleBody(bodyPredicates, predicateName, rightSideVars)

		Main.debug("\n\nBody: " + predicates)
		val tuples = findMaximalClause(target, predicates, positiveExamples, negativeExamples)
		tuples
	}

	def foil() {
		var targetPredicates = generateTargetVariables
		var candidates = generateCandidates
		var bodyPredicates = Map.empty[List[Term], String] // predicates to be added to the body

		val conf = new SparkConf()
			.setAppName("WordCount")
			.setMaster("local[4]")
		val sc = new SparkContext(conf)

		val candidatesRDD = sc.parallelize(candidates.toSeq, 4)

		targetPredicates.foreach(target => {

			val targetName = target._1
			var positiveExamples = posHolder.tupleMap(targetName)

			while (!positiveExamples.isEmpty) {
				var negativeExamples = negHolder.tupleMap(targetName)

				var maxPositive = Set.empty[List[String]]
				var maxNegative = Set.empty[List[String]]
				var max: (List[Term], String) = null

				var maxPositiveSpark = Set.empty[List[String]]
				var maxNegativeSpark = Set.empty[List[String]]
				var maxSpark: (List[Term], String) = null

				while (!negativeExamples.isEmpty) {
					var wig = 0d

					// Mapping candidates RDD to variable combinations first, 
					// then to gain values associated with them.
					val crdd = candidatesRDD.flatMap(c => expandCandidate(c))
						.map(cv => (cv,
							calcGain(cv,
								target,
								bodyPredicates,
								positiveExamples,
								negativeExamples)))

					crdd.foreach { x => println(x) }
					// Executing reduction action based on selection of
					// unit with highest gain value.
					val res = crdd.reduce((a, b) => getMaxGain(a, b))

					maxPositiveSpark = res._2._2._1
					maxNegativeSpark = res._2._2._2
					maxSpark = (res._1._2, res._1._1)

					println("Results:")

					println(maxPositive + " : " + maxPositiveSpark)
					println(maxNegative + " : " + maxNegativeSpark)
					println(max + " : " + maxSpark)

					Main.debug("Maximum gain: " + wig)
					Main.debug("Covered negative examples: " + maxNegative)
					Main.debug("Predicate with maximum gain " + max)

					bodyPredicates(maxSpark._1) = maxSpark._2
					negativeExamples = maxNegativeSpark.toList
				}

				//if (max != null) {

				positiveExamples = positiveExamples.toSet.diff(maxPositiveSpark).toList

				Main.debug("\nCovered positive examples: " + maxPositive)
				Main.debug("Positive examples to cover: " + positiveExamples)
				//}

			}
		})

		println("\nGenerated rule: " + targetPredicates + " <- " + bodyPredicates)
	}

	/* 
	 * Term object can be variable or atom
	 * in case of Var object we have to check that all Var objects for both target and right-side predicate must match
	 */
	def findMaximalClause(target: (String, ArrayList[Term]), bodyPredicates: Map[List[Term], String], positive: List[List[String]], negative: List[List[String]]) = {

		val n_pos_i = positive.size
		val n_neg_i = negative.size
		val ic_prev = entropy(n_pos_i, n_neg_i)

		val pos = foilAlgorithm(positive, target, bodyPredicates)
		val neg = foilAlgorithm(negative, target, bodyPredicates)

		val positiveExamples = pos._1
		val negativeExamples = neg._1
		var gain = 0d
		var ic_next = 0d
		if (pos._2._2 != 0 && neg._2._2 != 0) {
			ic_next = entropy(pos._2._2, neg._2._2)
			gain = wig(ic_prev, ic_next, pos._2._1)
		}
		Main.debug( /*n_pos_i + " " + n_neg_i + " " + ic_prev + " " + ic_next + */ " Gain: " + gain)
		(gain, (positiveExamples, negativeExamples))
	}

	/*
	 * Variable generation for the left part of the rule
	 * */
	def generateTargetVariables = {
		var target = scala.collection.mutable.Map.empty[String, ArrayList[Term]]
		posHolder.predicateMap.foreach(predicate => {
			val name = predicate._2.name
			val arity = predicate._2.arity
			var list = new ArrayList[Term]
			for (v <- 1 to arity)
				list.add(new Var("X" + v))
			target(name) = list
		})
		target
	}

	/*
	 * All possible variable combinations of base knowledge predicates
	 * TODO: update it with target predicate when new rule was generated
	 * */
	def generateCandidates = {
		var result = scala.collection.mutable.Map.empty[String, ArrayList[List[Term]]]
		baseHolder.predicateMap.foreach(predicate => {
			var candidates = new ArrayList[List[Term]]
			val name = predicate._2.name
			val arity = predicate._2.arity
			// TODO: generate all candidates correctly
			if (arity == 2) {
				candidates.add(List(new Var("X2"), new Var("X1")))
				candidates.add(List(new Var("X1"), new Var("X2")))
				candidates.add(List(new Var("X1"), new Atom("Y1")))
				candidates.add(List(new Atom("Y1"), new Var("X1")))
				candidates.add(List(new Var("X2"), new Atom("Y1")))
				candidates.add(List(new Atom("Y1"), new Var("X2")))
				candidates.add(List(new Var("X1"), new Var("X1")))
				candidates.add(List(new Var("X2"), new Var("X2")))
			} else if (arity == 1) {
				candidates.add(List(new Var("X1")))
				candidates.add(List(new Var("X2")))
			}

			result(name) = candidates
		})
		result
	}

	def updateRuleBody(bodyPredicates: Map[List[Term], String], predicateName: String, predicateVars: List[Term]) = {
		// clone map with body predicates
		val updatedBody = Map[List[Term], String]() ++= bodyPredicates
		updatedBody(predicateVars) = predicateName
		updatedBody
	}

	//	def getItem(key: String): Predicate = {
	//		//if this._map.has_key(key):
	//		// raise "Predicate with name '" + predicate.name + "' already exists." Exception
	//		this.base.getOrElse(key, null)
	//	}
	//
	//	def remove(predicate: Predicate) {
	//		this.base -= predicate.name
	//	}
	//
	//	def add(predicate: Predicate) {
	//		this.base(predicate.name) = predicate
	//	}
	//
	//	def addAll(list: List[Predicate]) {
	//		list.foreach(predicate => this.add(predicate))
	//	}
}
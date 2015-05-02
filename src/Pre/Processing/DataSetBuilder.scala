package Pre.Processing

import Miner.MinerConfig
import Ratio.Sorted.Tree.RatioSortedTree
import scala.util.Random

class DataSetBuilder(minerConfig: MinerConfig) extends DataListBuilder {

  //initialize the dat List variables
  val region = minerConfig.region
  val market = minerConfig.market
  val symbolID = minerConfig.symbolID
  val timeGranularity = minerConfig.timeGranularity
  val ratio = minerConfig.ratio
  val beginDate = minerConfig.beginDate
  val endDate = minerConfig.endDate
  val percentage = minerConfig.percentage
  val k = minerConfig.k
  val d = minerConfig.d
  val minerID = minerConfig.minerID

  val ratioSortion = new RatioSortedTree(region, market, symbolID, timeGranularity, beginDate, endDate, ratio)

  val num = ratioSortion.getNSize // the number of data
  val sortedTree = ratioSortion.getRatioTree
  var targetOneQuantity = (ratioSortion.getOneQuantity * percentage / 100).toInt
  var folderName = Random.nextInt(10000).toString;

  def getOneSet(): (List[String], List[Int]) = {
    val one = targetOneQuantity
    val zero = num - targetOneQuantity
    val groupQuantity = zero / one //number of groupQuantity gonna to groupQuantitying
    val restQuantity = zero % groupQuantity //number of groupQuantity need to have one more element

    var zeroSubSet: List[(String, Double)] = List() // the trainingPoint of zero Target ,and will be sort by random value
    var oneSubSet: List[String] = List() // the trainingPoint of one Target
    var zeroLabel: List[Int] = List()
    var oneLabel: List[Int] = List()
    var HDFSBuilderList: List[String] = List()
    var tempKey = ""

    for (i <- 1 to num) {
      if (i == 1) {
        tempKey = sortedTree.FirstKey()
      } else {
        tempKey = sortedTree.NextKey(tempKey)
      }
      if ((num - i + 1) <= targetOneQuantity) {
        oneSubSet = oneSubSet.:::(List(sortedTree.get(tempKey)))
        oneLabel = oneLabel.:::(List(1))
      } else {
        zeroSubSet = List((sortedTree.get(tempKey), util.Random.nextDouble())).:::(zeroSubSet)
        zeroLabel = zeroLabel.:::(List(0))
      }
    }

    zeroSubSet.sortBy(_._2)

    (oneSubSet.:::(zeroSubSet.map(f => f._1)), oneLabel.:::(zeroLabel))
  }
  def getRandomSubset(): (List[List[String]], List[List[Int]]) = {
    val one = targetOneQuantity
    val zero = num - targetOneQuantity
    val groupQuantity = zero / one //number of groupQuantity gonna to groupQuantitying
    val restQuantity = zero % groupQuantity //number of groupQuantity need to have one more element

    var zeroSubSet: List[(String, Double)] = List() // the trainingPoint of zero Target ,and will be sort by random value
    var oneSubSet: List[String] = List() // the trainingPoint of one Target
    var lableList: List[List[Int]] = List()
    var HDFSBuilderList: List[String] = List()
    var tempKey = ""

    for (i <- 1 to num) {
      if (i == 1) {
        tempKey = sortedTree.FirstKey()
      } else {
        tempKey = sortedTree.NextKey(tempKey)
      }
      if ((num - i + 1) <= targetOneQuantity) {
        oneSubSet = oneSubSet.:::(List(sortedTree.get(tempKey)))
      } else {
        zeroSubSet = List((sortedTree.get(tempKey), util.Random.nextDouble())).:::(zeroSubSet)
      }
    }

    zeroSubSet.sortBy(_._2)

    var trainingSubSet: List[List[String]] = List()
    for (i <- 1 to groupQuantity) {
      val num = if (i <= restQuantity) { zero / groupQuantity + 1 } else { zero / groupQuantity }
      var testingSet: List[String] = List()
      var testingLabel: List[Int] = List()

      for (j <- 1 to num) {
        testingSet = List(zeroSubSet.head._1).:::(testingSet)
        testingLabel = List(0).:::(testingLabel)
        zeroSubSet = zeroSubSet.drop(1)
      }
      testingSet = oneSubSet.:::(testingSet)
      for (k <- 1 to oneSubSet.length) {
        testingLabel = List(1).:::(testingLabel)
      }
      trainingSubSet = List(testingSet).:::(trainingSubSet)
      lableList = List(testingLabel).:::(lableList)
    }

    (trainingSubSet, lableList)
  }
  def getRatioSubset(): (List[List[String]], List[List[Int]]) = {
    val one = targetOneQuantity
    val zero = num - targetOneQuantity
    val groupQuantity = zero / one //number of groupQuantity gonna to groupQuantitying
    val restQuantity = zero % groupQuantity //number of groupQuantity need to have one more element

    var zeroSubSet: List[(String, Double)] = List() // the trainingPoint of zero Target ,and will be sort by random value
    var oneSubSet: List[String] = List() // the trainingPoint of one Target
    var lableList: List[List[Int]] = List()
    var HDFSBuilderList: List[String] = List()

    var tempKey = ""

    for (i <- 1 to num) {
      if (i == 1) {
        tempKey = sortedTree.FirstKey()
      } else {
        tempKey = sortedTree.NextKey(tempKey)
      }
      if ((num - i + 1) <= targetOneQuantity) {
        oneSubSet = oneSubSet.:::(List(sortedTree.get(tempKey)))
      } else {
        var DValue = (tempKey.substring(0, 5) + "." + tempKey.substring(5)).toDouble;
        zeroSubSet = List((sortedTree.get(tempKey), DValue)).:::(zeroSubSet)
      }
    }
    zeroSubSet.sortBy(_._2)

    var trainingSubSet: List[List[String]] = List()
    for (i <- 1 to groupQuantity) {
      val num = if (i <= restQuantity) { zero / groupQuantity + 1 } else { zero / groupQuantity }
      var testingSet: List[String] = List()
      var testingLabel: List[Int] = List()

      for (j <- 1 to num) {
        testingSet = List(zeroSubSet.head._1).:::(testingSet)
        testingLabel = List(0).:::(testingLabel)
        zeroSubSet = zeroSubSet.drop(1)
      }
      testingSet = oneSubSet.:::(testingSet)
      for (k <- 1 to oneSubSet.length) {
        testingLabel = List(1).:::(testingLabel)
      }
      trainingSubSet = List(testingSet).:::(trainingSubSet)
      lableList = List(testingLabel).:::(lableList)
    }

    (trainingSubSet, lableList)
  }
  def getRatioRanSubset(): (List[List[String]], List[List[Int]]) = {
    val one = targetOneQuantity
    val zero = num - targetOneQuantity
    val groupQuantity = zero / one //number of groupQuantity gonna to groupQuantitying
    val restQuantity = zero % groupQuantity //number of groupQuantity need to have one more element

    var zeroSubSet: List[(String, Double)] = List() // the trainingPoint of zero Target ,and will be sort by random value
    var oneSubSet: List[String] = List() // the trainingPoint of one Target
    var lableList: List[List[Int]] = List()
    var HDFSBuilderList: List[String] = List()

    var tempKey = ""

    for (i <- 1 to num) {
      if (i == 1) {
        tempKey = sortedTree.FirstKey()
      } else {
        tempKey = sortedTree.NextKey(tempKey)
      }
      if ((num - i + 1) <= targetOneQuantity) {
        oneSubSet = oneSubSet.:::(List(sortedTree.get(tempKey)))
      } else {
        zeroSubSet = List((sortedTree.get(tempKey), tempKey.toDouble)).:::(zeroSubSet)
      }
    }
    zeroSubSet.sortBy(_._2)

    var stratifiedTrainingSubSet: List[List[String]] = List() // stratified the SubSet
    for (i <- 1 to zero / groupQuantity) {
      val num = if (i <= zero % groupQuantity) { groupQuantity + 1 } else { groupQuantity }
      var testingSet: List[String] = List()

      for (j <- 1 to num) {
        testingSet = List(zeroSubSet.head._1).:::(testingSet)
        zeroSubSet = zeroSubSet.drop(1)
      }
      testingSet = oneSubSet.:::(testingSet)
      stratifiedTrainingSubSet = List(testingSet).:::(stratifiedTrainingSubSet)
    }

    var trainingSubSet: List[List[String]] = List() // random get in the stratified subSet

    for (i <- 1 to groupQuantity) { // get the Subset of groupQuantity num
      var testingSet: List[String] = List()
      var testingLabel: List[Int] = List()

      for (j <- 1 to stratifiedTrainingSubSet.length) { //get a random element form every stratify

        val random = util.Random.nextInt(stratifiedTrainingSubSet(j - 1).length)

        testingSet = List(stratifiedTrainingSubSet(j - 1)(random)).:::(testingSet) // get random element form this stratify
        testingLabel = List(0).:::(testingLabel)
        stratifiedTrainingSubSet(j - 1).drop(random + 1) // drop this element form this stratify

        if (i <= restQuantity && i == j) { // if the groupQuantity num and the subset num are the same , get twice

          val random = util.Random.nextInt(stratifiedTrainingSubSet(j - 1).length)

          testingSet = List(stratifiedTrainingSubSet(j - 1)(random)).:::(testingSet)
          testingLabel = List(0).:::(testingLabel)
          stratifiedTrainingSubSet(j - 1).drop(random + 1)
        }
      }
      testingSet = oneSubSet.:::(testingSet) // add one traget list into the Set
      for (k <- 1 to oneSubSet.length) {
        testingLabel = List(1).:::(testingLabel)
      }
      trainingSubSet = List(testingSet).:::(trainingSubSet) //add Set into whole SubSet
      lableList = List(testingLabel).:::(lableList)

    }

    (trainingSubSet, lableList)
  }

  //  // print the targetList
  //  val targetWriterFile = new File("/home/kmlab/PreprocessingData/TrainingTargetList.txt")
  //  targetWriterFile.getParentFile().mkdirs();
  //  val targetWriter = new PrintWriter(targetWriterFile)
  //  for (i <- 0 to dataList.length - 1) {
  //    targetWriter.write(dataList(i).toString + ",")
  //  }
  //  targetWriter.close()  

}


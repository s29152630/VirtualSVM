package svm.control

import svm.demo._
import svm.libsvm._
import java.io.FileWriter

class SVMEntity(trainArgs: Array[String], targetArray: Array[Int], trainingArray: Array[Array[Double]]) {
  val totalLabelNum = targetArray.length
  val filter = extract_forVote
  val numOfAll = targetArray.length
  val numOfZero = filter._1.length
  val numOfOne = filter._3.length
  val (candidate, citizen) = {
    if (numOfOne < numOfZero) {
      ((filter._3, filter._4), (filter._1, filter._2))
    } else {
      ((filter._1, filter._2), (filter._3, filter._4))
    }
  }
  val voteNum = ((citizen._1.length / candidate._1.length) + 1) * 1
  val percentageOfElectVote = 80 / 100.toDouble
  PredictHelper.voteNum = voteNum
  PredictHelper.percentageOfElectVote = percentageOfElectVote
  var modelOfAllData: svm_model = null
  var modelArray_forVote = Array.ofDim[svm_model](PredictHelper.voteNum)

  def run {
      svm_forVote
  }

  def svm_forVote {
    println("[SVNEntity] preparing data")
    val trainProbArray = transformData_forVote
    val trainProb = transformData(targetArray, trainingArray)

    println("[SVNEntity] run begin")
    svmTrain_forVote(trainProbArray)

    svmPredict_forVote(trainProb.x, modelArray_forVote)
    println("[SVNEntity] run end")

  }

  /*def svm_Normal {
    //val (labelList: List[Int], trainingList: List[List[Double]]) = extractForOneClass(targetArray, dataArray)
    println("[SVNEntity] preparing data")
    val trainProb = transformData(targetArray, trainingArray)

    println("[SVNEntity] run begin")
    svmTrain(trainProb)

    svmPredict(trainProb.x)
    println("[SVNEntity] run end")
  }*/

  def svmTrain_forVote(trainProbArray: Array[svm_problem]) {
    (0 until PredictHelper.voteNum).foreach(i => modelArray_forVote(i) = svm_train.main(trainArgs, trainProbArray(i)))
  }

  /*def svmTrain(trainProb: svm_problem) = {
    modelOfAllData = svm_train.main(trainArgs, trainProb)
  }*/

  def svmPredict_forVote(dataArray: Array[Array[svm_node]], modelArray: Array[svm_model]) {
    var hit: Int = 0
    var oneHit: Int = 0
    var oneFalse: Int = 0
    var ratio: Double = 0.0
    var oneHitRatio: Double = 0.0
    var oneFalseRatio: Double = 0.0
    var oneLoseRatio: Double = 0.0
    val predictResult = dataArray.map(data => PredictHelper.predictVote(data, modelArray)).toArray
    (predictResult zip targetArray).foreach {
      case (p, t) => {
        if (t == 1) {
          if (p == 1) {
            oneHit += 1
          }
        }else{
          if (p == 1) {
            oneFalse += 1
          }
        }
        if (p == t) {
          hit += 1
        }
      }
    }
    val oneSaidBySVM = predictResult.filter(_ == 1).length
    val zeroSaidBySVM = totalLabelNum - oneSaidBySVM
    ratio = (hit.toDouble / totalLabelNum.toDouble) * 100
    oneHitRatio = (oneHit.toDouble / numOfOne.toDouble) * 100
    oneFalseRatio = (oneFalse.toDouble / numOfZero.toDouble) * 100
    oneLoseRatio = 100 - oneHitRatio
    println("\nRealOneNum / RealZeroNum : " + numOfOne + " / " + numOfZero + "     " + "OneSaidBySVM : " + oneSaidBySVM + " ...")
    println("Training Ratio : " + ratio + " % ...")
    println("One Hit Ratio : " + oneHitRatio + " % ...")
    println("One False Ratio : " + oneFalseRatio + " % ...")
    println("One Lose Ratio : " + oneLoseRatio + " % ...")
    /*val writer: FileWriter = new FileWriter("SVMTestOutput.txt", true)
    writer.append("\nRealOneNum / RealZeroNum : " + numOfOne + " / " + numOfZero + "     " + "OneSaidBySVM : " + oneSaidBySVM + " ...\n")
    writer.append("Training Ratio : " + ratio + " % ...\n")
    writer.append("One Hit Ratio : " + oneHitRatio + " % ...\n")
    writer.append("One False Ratio : " + oneFalseRatio + " % ...\n")
    writer.append("One Lose Ratio : " + oneLoseRatio + " % ...\n")
    writer.close*/
  }

  /*def svmPredict(dataArray: Array[Array[svm_node]]) {
    var hit: Int = 0
    var ratio: Double = 0.0
    val predictResult = dataArray.map(data => svm.svm_predict(modelOfAllData, data))
    (predictResult zip targetArray).foreach {
      case (p, t) => {
        if (p == t) {
          hit += 1
          //print(p + ", ")
        }
      }
    }
    //println("\n" + hit + " " +predictResult.length)
    ratio = (hit.toDouble / predictResult.length.toDouble) * 100
    println("Training Ratio = " + ratio + "% ...")
  }*/

  def getModel: (svm_model, Array[svm_model]) = {
    (modelOfAllData, modelArray_forVote)
  }

  def extract_forVote: (Array[Int], Array[Array[Double]], Array[Int], Array[Array[Double]]) = {
    val a = (targetArray zip trainingArray).filter { case (x, y) => x == 0 }.unzip
    val b = (targetArray zip trainingArray).filter { case (x, y) => x == 1 }.unzip
    (a._1.toArray, a._2.toArray, b._1.toArray, b._2.toArray)
  }

  def transformData_forVote: Array[svm_problem] = {
    val rnd = new GenRandInt(0, citizen._1.length - 1)
    (0 until PredictHelper.voteNum).map(i => {
      var localLabelList = candidate._1.toList
      var localTrainingList = candidate._2.toList
      (0 until candidate._1.length).foreach(j => {
        val random = rnd.that
        localLabelList = localLabelList.+:(citizen._1(random))
        localTrainingList = localTrainingList.+:(citizen._2(random))
      })
      transformData(localLabelList.toArray, localTrainingList.toArray)
    }).toArray
  }

  def transformData(labelList: Array[Int], trainingList: Array[Array[Double]]): svm_problem = {

    //println("1")
    val prob = new svm_problem

    val dataNum = labelList.length

    val dataLen = trainingList(0).length
    val vy = (labelList).map(_.toDouble)
    val vx = new Array[Array[svm_node]](dataNum)

    for (i <- 0 to dataNum - 1) {
      val data = new Array[svm_node](dataLen)
      for (j <- 0 to dataLen - 1) {
        val svmNode = new svm_node
        svmNode.index = j + 1
        svmNode.value = trainingList(i)(j)
        data(j) = svmNode
      }
      vx(i) = data
    }
    val max_index = trainingList(0).length
    prob.l = dataNum // data number
    prob.x = vx
    prob.y = vy
    prob.max_index = max_index
    prob
  }
}
case class GenRandInt(lb: Int, ub: Int) {
  private val rnd = new scala.util.Random
  def that(): Int = { lb + rnd.nextInt(ub) }
}// class random
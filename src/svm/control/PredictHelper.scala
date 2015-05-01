package svm.control

import svm.libsvm.svm_node
import svm.libsvm.svm_model
import svm.libsvm.svm

object PredictHelper {
  var voteNum = 80
  var percentageOfElectVote = 100/100.toDouble
  def predictVote(svm_nodeArray: Array[svm_node], modelArray: Array[svm_model]): Int = {
    val voteSum = modelArray.map(m => svm.svm_predict(m, svm_nodeArray).toInt).sum
    //println("svm_nodeArray: ")
    //svm_nodeArray.map(v => print(v.value.toString))
    //println("voteSum: " + voteSum)
    //println("VoteNum : " + voteNum + "     Percentage : " + percentageOfElectVote)
    if (voteSum >= voteNum * percentageOfElectVote) 1 else 0
  }
}
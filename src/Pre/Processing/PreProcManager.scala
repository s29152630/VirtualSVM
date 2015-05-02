package Pre.Processing;

import Miner.MinerConfig

object PreProcManager {

  var sigmaConfirm = false

  def excute(minerConfig: MinerConfig): (Any, Any) = {
    val dataSetBuilder = new DataSetBuilder(minerConfig)
    //targetList.foreach(println)

    var dataSet: (Any, Any) = (null, null)
    val preProcType = minerConfig.preProcType

    if (preProcType == "OneSet") {
      dataSet = dataSetBuilder.getOneSet
    } else if (preProcType == "RandomSubSet") {
      dataSet = dataSetBuilder.getRandomSubset
    } else if (preProcType == "RatioSubSet") {
      dataSet = dataSetBuilder.getRatioSubset
    } else if (preProcType == "RatioRanSubSet") {
      dataSet = dataSetBuilder.getRatioRanSubset
    } else {
      println("Wrong PreProcessing Type!!!")
    }

    dataSet
  }
}
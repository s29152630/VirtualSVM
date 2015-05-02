package PredictDataSet.Builder

import Miner.MinerConfig
import Ratio.Sorted.Tree.RatioSortedTree
import scala.util.Random

class PredictDataSetBuilder(minerConfig: MinerConfig) {
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

  def getPredictDataSet(BeginDate: String, EndDate: String): (List[String], List[Int]) = {

    //initialize the dat List variables

    val ratioSortion = new RatioSortedTree(region, market, symbolID, timeGranularity, BeginDate, EndDate, ratio)

    val num = ratioSortion.getNSize // the number of data
    val sortedTree = ratioSortion.getRatioTree
    var targetOneQuantity = ratioSortion.getOneQuantity
    var folderName = Random.nextInt(10000).toString;

    var timeList: List[String] = List()
    var lableList: List[Int] = List()

    var tempKey = ""

    for (i <- 1 to num) {
      if (i == 1) {
        tempKey = sortedTree.FirstKey()
      } else {
        tempKey = sortedTree.NextKey(tempKey)
      }
      if ((num - i + 1) <= targetOneQuantity) {
        lableList = lableList.:::(List(1))
      } else {
        lableList = lableList.:::(List(0))
      }
      timeList = timeList.:::(List(sortedTree.get(tempKey)))
    }

    (timeList, lableList)
  }
}
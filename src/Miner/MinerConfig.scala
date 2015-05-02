package Miner

class MinerConfig(config: Map[String, String]) {

  val minerID = config.getOrElse("minerID", "") //minerID
  val k = config.getOrElse("k", "").toInt //10 or 20
  val d = config.getOrElse("d", "").toInt
  val percentage = config.getOrElse("percentage", "").toDouble //What percentage Ratio greater than one should Label take

  val region = config.getOrElse("region", "")
  val market = config.getOrElse("market", "")
  val symbolID = config.getOrElse("symbolID", "")
  val timeGranularity = config.getOrElse("timeGranularity", "")
  val beginDate = config.getOrElse("beginDate", "")
  val endDate = config.getOrElse("endDate", "")
  val ratio = config.getOrElse("ratio", "") // the Ratio TA name ex.RATIO_010_SHORT,RATIO_010_LONG...

  val preProcType = config.getOrElse("preProcType", "") //The Type of DataSet ex.RandomSubSet , RatioSubSet , RatioRanSubSet

}
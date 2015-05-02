package Miner

class MinerConfigGenerator {

  def generateMinerConfig(minerID: java.lang.String, k: java.lang.Integer, d: java.lang.Integer, percentage: java.lang.Double, region: java.lang.String, market: java.lang.String, symbolID: java.lang.String, timeGranularity: java.lang.String, beginDate: java.lang.String, endDate: java.lang.String, ratio: java.lang.String, preProcType: java.lang.String, MiningType: java.lang.String): MinerConfig = {
    var configMap: Map[String, String] = Map()
    configMap += ("minerID" -> minerID)
    configMap += ("k" -> k.toString) //unused
    configMap += ("d" -> d.toString) //unused
    configMap += ("percentage" -> percentage.toString)
    configMap += ("region" -> region)
    configMap += ("market" -> market)
    configMap += ("symbolID" -> symbolID)
    configMap += ("timeGranularity" -> timeGranularity)
    configMap += ("beginDate" -> beginDate)
    configMap += ("endDate" -> endDate)
    configMap += ("ratio" -> ratio)
    //        configMap += ("group" -> "") unused
    configMap += ("preProcType" -> preProcType) //RandomSubSet, RatioSubSet, RatioRanSubSet
    configMap += ("MiningType" -> MiningType)
    new MinerConfig(configMap)
    
  }
}
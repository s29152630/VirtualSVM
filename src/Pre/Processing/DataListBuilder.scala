package Pre.Processing


abstract class DataListBuilder {
  
  val region: String
  val market: String;
  val symbolID: String;
  val timeGranularity: String;
  val beginDate: String;
  val endDate: String;
}
package Ratio.Sorted.Tree.pre

import NET.sourceforge.BplusJ.BplusJ._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.GregorianCalendar
import HBase.Helper.HBaseHelperS
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import scala.util.Random
import java.io.File
import java.util.TimeZone;

class RatioSortedTree(Region: String, Market: String, SymbolID: String, TimeGranularity: String, BeginDate: String, EndDate: String, Ratio: String) {

  var nSize = 0 // num of row
  var oneQuantity = 0
  var dateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss")
  var dateFormat2 = new SimpleDateFormat("yyyyMMdd")
  var dateFormat3 = new SimpleDateFormat("yyyyMMdd")
  dateFormat3.setTimeZone(TimeZone.getTimeZone("GMT0"))
  var beginDate = dateFormat1.parse(BeginDate)
  var endDate = dateFormat1.parse(EndDate)
  var hb = HBaseHelperS

  var tempCalendar = new GregorianCalendar()
  tempCalendar.setTime(beginDate)
  tempCalendar.setTimeZone(TimeZone.getTimeZone("GMT0"))
  var endCalendar = new GregorianCalendar()
  endCalendar.setTime(endDate)

  var path = new File("/home/kmlab/RatioTree/")
  path.mkdir()
  var fileName = "/home/kmlab/RatioTree/" + getRandomInt()
  @throws[Exception]()
  var RatioTree = BplusTree.Initialize(fileName, fileName, 1024)

  var rowkeyIndex = 1
  var tempRowkey = ""
  var rowResult: org.apache.hadoop.hbase.client.Result = null;

  while (tempCalendar.compareTo(endCalendar) < 1) {

    var rowList: List[Double] = List()
    var rowkey = Region + "/" + Market + "/" + SymbolID + "/" + TimeGranularity + "/" + dateFormat3.format(tempCalendar.getTime()).toString() + "/" + Ratio + "/" + (tempCalendar.get(Calendar.HOUR_OF_DAY) + 1).toString;
    if (!rowkey.equals(tempRowkey)) {
      rowResult = hb.getRow(Bytes.toBytes(rowkey), "RatioTable") // get Row Data
      tempRowkey = rowkey
    }

    if (rowResult.size() != 0) {

      if (rowResult.containsColumn(Bytes.toBytes("Ratio"), Bytes.toBytes(dateFormat1.format(tempCalendar.getTime()).toString))) {
        var tempRatio = rowResult.getValue(Bytes.toBytes("Ratio"), Bytes.toBytes(dateFormat1.format(tempCalendar.getTime()).toString))
        if (!tempRatio.equals("-999")) {
          var value = Bytes.toString(tempRatio).split("\\.");
          var RTKey = getNum(value(0).toInt, 5) + value(1);
          var RTKeyIndex = 0;
          while (RatioTree.ContainsKey(RTKey + getNum(RTKeyIndex, 8))) {
            RTKeyIndex += 1
          }
          RatioTree.Set(RTKey + getNum(RTKeyIndex, 8), dateFormat1.format(tempCalendar.getTime()).toString())

          nSize += 1
          rowkeyIndex += 1

          if (Bytes.toString(tempRatio).toDouble >= 1) {
            oneQuantity += 1
          }
        }

      }

    }
    tempCalendar.add(Calendar.SECOND, 1) //add one second
    //if time is 13:40:00 then add 19 Hours & 10 Minute then it will become next day 08:50:00
    if (tempCalendar.get(Calendar.HOUR) == 13 && tempCalendar.get(Calendar.MINUTE) == 40) {
      tempCalendar.add(Calendar.HOUR, 19)
      tempCalendar.add(Calendar.MINUTE, 10)
    }
    //if Saturday then add 2 Days
    if (tempCalendar.get(Calendar.DAY_OF_WEEK) == 7) {
      tempCalendar.add(Calendar.DATE, 2)
    }
  }

  def getRatioTree(): BplusTree = {

    return RatioTree
  }
  def clear() {
    RatioTree.Shutdown()
  }
  def getNSize(): Int = {

    return nSize
  }
  def getOneQuantity(): Int = {

    return oneQuantity
  }
  def getRandomInt(): String = {
    var num = Random.nextInt(99999)
    var result = num.toString
    if (num < 10000) {
      result = "0" + result
    }
    if (num < 1000) {
      result = "0" + result
    }
    if (num < 100) {
      result = "0" + result
    }
    if (num < 10) {
      result = "0" + result
    }

    result
  }
  private def getNum(num: Int, stringLength: Int): String = {
    var result = num.toString

    while (result.length() < stringLength) {
      result = "0" + result
    }
    result
  }

}
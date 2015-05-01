package Ratio.Sorted.Tree

import NET.sourceforge.BplusJ.BplusJ._
import java.text.SimpleDateFormat
import java.util.Calendar
import HBase.Helper.HBaseHelperS
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import scala.util.Random
import java.io.File

class RatioSortedTree(Region: String, Market: String, SymbolID: String, TimeGranularity: String, BeginDate: String, EndDate: String, Ratio: String) {

  var nSize = 0 // num of row
  var oneQuantity = 0
  var dateFormat1 = new SimpleDateFormat("yyyyMMddhhmmss")
  var dateFormat2 = new SimpleDateFormat("yyyyMMdd")
  var beginDate = dateFormat1.parse(BeginDate)
  var endDate = dateFormat1.parse(EndDate)
  var hb = HBaseHelperS

  var tempCalendar = Calendar.getInstance()
  tempCalendar.setTime(beginDate)
  var endCalendar = Calendar.getInstance()
  endCalendar.setTime(endDate)

  var path = new File("/home/kmlab/RatioTree/")
  path.mkdir()
  var fileName = "/home/kmlab/RatioTree/" + getRandomInt()
  @throws[Exception]()
  var RatioTree = BplusTree.Initialize(fileName, fileName, 1024)

  var rowkeyIndex = 1

  while (tempCalendar.compareTo(endCalendar) != 1) {

    var rowList: List[Double] = List()
    var rowkey = Region + "/" + Market + "/" + SymbolID + "/" + TimeGranularity + "/" + dateFormat1.format(tempCalendar.getTime()).toString()
    var rowResult = hb.getRow(Bytes.toBytes(rowkey), "RatioTable") // get Row Data

    if (rowResult.size() != 0) {

      var tempRatio = rowResult.getValue(Bytes.toBytes("State"), Bytes.toBytes(Ratio))
      RatioTree.Set(Bytes.toString(tempRatio), dateFormat1.format(tempCalendar.getTime()).toString())

      nSize += 1
      rowkeyIndex += 1

      if (Bytes.toString(tempRatio).toDouble >= 1) {
        oneQuantity += 1
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

}
package Virtual.Matrix

import java.text.SimpleDateFormat
import java.util.Calendar

import HBase.Helper.HBaseHelper
import HDFS.Helper.HDFSHelper

import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._

import scala.util.Random

import java.io._

import org.apache.hadoop.fs.Path;

class VirtualMatrix() {

  val vMatrixID = getRandomInt()
  var mSize = 0 // num of column
  var nSize = 0 // num of row
  private var dateFormat1 = new SimpleDateFormat("yyyyMMddhhmmss")
  private var dateFormat2 = new SimpleDateFormat("yyyyMMdd")
  private var hb = HBaseHelper.getHBaseHelperInstance()
  private var HDFSHelper = new HDFSHelper()
  private val localDirPath = "/home/kmlab/HDFSFiles/VirtualMatrix/"
  private val localFileRPath = localDirPath + vMatrixID + "R.txt"
  private val localFileCPath = localDirPath + vMatrixID + "C.txt"
  private val serverDirPath = "hdfs://nccu-master:8020/home/kmlab/VirtualMatrix/"
  private val serverFileRPath = "hdfs://nccu-master:8020/home/kmlab/VirtualMatrix/" + vMatrixID + "R.txt"
  private val serverFileCPath = "hdfs://nccu-master:8020/home/kmlab/VirtualMatrix/" + vMatrixID + "C.txt"
  private var alterCount = 0
  private var alterTempData: scala.collection.mutable.Map[Int, List[(Int, String)]] = scala.collection.mutable.Map()
  private var addRowCount = 0
  private var addRowTempData: List[List[String]] = List()
  var getColumn = new this.getColumn()
  private var tempResultFileCWriterArray: Array[java.io.BufferedWriter] = Array()
  private var VMType = ""

  def getDataFromDataBase(Region: String, Market: String, SymbolID: String, TimeGranularity: String, StateTag: List[String], DateTimeList: List[String]) {
    //vertical rowkey region/market/SymbolID/TG/time
    //horizontal rawkey region/market/SymbolID/TG/date/Tag/SegNo

    if (!VMType.equals("")) {
      throw new Exception("Virtual Matrix Already put Data in other Mode , Please Check!")
    } else {
      VMType = "getDataFromDataBase"
    }

    mSize = StateTag.length

    var rowkeyIndex = 1
    var tempResultFileR = new File(localFileRPath)
    var tempResultFileC = new File(localFileCPath)

    try {
      tempResultFileR.getParentFile().mkdirs();
      var tempResultFileRWriter = new BufferedWriter(new FileWriter(tempResultFileR, false))
      tempResultFileCWriterArray = new Array[java.io.BufferedWriter](StateTag.length)

      // Initialize Row-Based Matrix
      for (i <- 0 to DateTimeList.length - 1) {

        var rowList: List[Double] = List()
        var rowkey = Region + "/" + Market + "/" + SymbolID + "/" + TimeGranularity + "/" + DateTimeList(i)
        var rowResult = hb.getRow(Bytes.toBytes(rowkey), "Stock") // get Row Data

        //      var VMRatioRowkey = vMatrixID + "/" + rowResult.getValue(Bytes.toBytes("State"), Bytes.toBytes(Ratio)) + "/" + getRowNumString(rowkeyIndex)
        if (i == 0) {
          for (k <- 0 to StateTag.length - 1) {

            var tempResultFileCSplit = new File(localDirPath + vMatrixID + "C" + k + ".txt")
            tempResultFileCWriterArray(k) = new BufferedWriter(new FileWriter(tempResultFileCSplit, true))

          }
        }

        if (rowResult.size() != 0) {

          for (k <- 0 to StateTag.length - 1) {

            var tempStateValue = rowResult.getValue(Bytes.toBytes("State"), Bytes.toBytes(StateTag(k)))

            if (k != 0) {
              tempResultFileRWriter.write(" ")
            }
            if (i != 0) {
              tempResultFileCWriterArray(k).write(" ")
            }
            if (tempStateValue != null) {

              tempResultFileRWriter.write(Bytes.toString(tempStateValue))
              tempResultFileCWriterArray(k).write(Bytes.toString(tempStateValue))

            } else {
              tempResultFileRWriter.write("0.0")
              tempResultFileCWriterArray(k).write("0.0")

            }

          }
          //      hb.putVMRatio(VMRatioPut)

          tempResultFileRWriter.newLine()
          nSize += 1

        }

        if (i == DateTimeList.length - 1) {
          for (k <- 0 to StateTag.length - 1) {
            tempResultFileCWriterArray(k).close()
          }

        }
      }
      tempResultFileRWriter.close()
      tempResultFileCWriterArray = Array()

      //      // Initialize Column-Based Matrix

      var tempResultFileCWriter = new BufferedWriter(new FileWriter(tempResultFileC, false))
      var tempResultFileCSplitReader: java.io.BufferedReader = null;

      for (i <- 0 to mSize - 1) {
        var tempResultFileCSplit = new File(localDirPath + vMatrixID + "C" + i + ".txt")
        tempResultFileCSplitReader = new BufferedReader(new FileReader(tempResultFileCSplit));
        tempResultFileCWriter.write(tempResultFileCSplitReader.readLine())
        tempResultFileCWriter.newLine()
        tempResultFileCSplitReader.close()
        tempResultFileCSplit.delete()

      }
      tempResultFileCWriter.close()

    } catch {
      case e: IOException => e.printStackTrace()
    }


    HDFSHelper.createDirectoryOnHDFS(serverDirPath)
    HDFSHelper.HDFSUploadFile(localFileRPath, serverDirPath)
    HDFSHelper.HDFSUploadFile(localFileCPath, serverDirPath)

    try {
      tempResultFileR.delete()
      tempResultFileC.delete()
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def getRow(RowNum: Int): List[Double] = {

    var rowList = HDFSHelper.readFileOnHDFS(serverFileRPath, RowNum).split(" ").toList.take(mSize).map(f => f.toDouble)

    //    scanner.close()

    rowList
  }
  
  def alter(double1:Double, i:Int, j:Int) {
  // TODO Auto-generated method stub
  
  }
  
  class getColumn {
    //    var hTable = hb.getHTable("VirtualMatrixRatio")

    var fileSystem: org.apache.hadoop.fs.FileSystem = null
    var fPath: org.apache.hadoop.fs.Path = null
    var in: org.apache.hadoop.fs.FSDataInputStream = null
    var buff: java.io.BufferedReader = null

    var columnNum = 0
    def setColumnNum(ColumnNum: Int) {
      columnNum = ColumnNum;
      fileSystem = HDFSHelper.getFileSystem();
      fPath = new Path(serverFileCPath);
      in = fileSystem.open(fPath);
      buff = new BufferedReader(new InputStreamReader(in));
      for (i <- 1 to columnNum - 1) {
        buff.readLine();
      }
    }
    def Next(): Double = {
      if (columnNum == 0) {
        throw new Exception("Please SetColumnNum First!")
      }

      var str = ""
      var read = buff.read()
      while (read != -1 && read != 32 && read != 10) {
        str += read.toChar
        read = buff.read()
      }
      if (read == 10) {
        Close()
      }

      str.toDouble
    }

    def Close() {

      buff.close();
      in.close();
      fileSystem.close();
      columnNum = 0
    }

  }
  def getColumnList(ColumnNum: Int): List[Double] = {

    var columnList = HDFSHelper.readFileOnHDFS(serverFileCPath, ColumnNum).split(" ").toList.take(nSize).map(f => f.toDouble)

    //    scanner.close()

    columnList
  }
  //  def alterRow(RowNum: Int, RowData: List[Double]) {
  //
  //    var tempRowData: List[(Int, String)] = List()
  //    for (i <- 1 to RowData.length) {
  //      tempRowData = List((i, RowData(i).toString)).:::(tempRowData)
  //    }
  //
  //    if (alterTempData.contains(RowNum)) {
  //      alterTempData.update(RowNum, tempRowData)
  //    } else {
  //      alterTempData += (RowNum -> tempRowData)
  //    }
  //    alterCount += RowData.length
  //
  //    if (alterCount >= 50000) {
  //      alterDataToHDFS()
  //    }
  //
  //  }
  //  def alterData(RowNum: Int, ColumnNum: Int, ColumnData: Double) {
  //
  //    alterCount += 1
  //    if (alterTempData.contains(RowNum)) {
  //      alterTempData.updated(RowNum, alterTempData(RowNum).:::(List((ColumnNum, ColumnData.toString))))
  //    } else {
  //      alterTempData += (RowNum -> List((ColumnNum, ColumnData.toString)))
  //    }
  //    if (alterCount >= 50000) {
  //      alterDataToHDFS()
  //    }
  //  }
  //  def alterDataToHDFS() {
  //
  //    HDFSHelper.downloadFileorDirectoryOnHDFS(serverFileRPath, localDirPath)
  //    HDFSHelper.deleteFileOnHDFS(serverFileRPath)
  //
  //    var temp = ""
  //    try {
  //      var localFile = new File(localFileRPath);
  //      var fis = new FileInputStream(localFile);
  //      var isr = new InputStreamReader(fis);
  //      var br = new BufferedReader(isr);
  //      var buf = new StringBuffer();
  //
  //      for (j <- 1 to nSize) {
  //        if (alterTempData.contains(j)) {
  //          var tempRowArray = br.readLine().split(" ")
  //          var tempRow = ""
  //          alterTempData(j).foreach(t => tempRowArray(t._1 - 1) = t._2)
  //
  //          for (k <- 0 to tempRowArray.length - 1) {
  //            if (k != 0) {
  //              tempRow += " "
  //            }
  //            tempRow += tempRowArray(k)
  //          }
  //          buf = buf.append(tempRow);
  //          buf = buf.append(System.getProperty("line.separator"));
  //
  //        } else {
  //
  //          temp = br.readLine();
  //          buf = buf.append(temp);
  //          buf = buf.append(System.getProperty("line.separator"));
  //
  //        }
  //      }
  //
  //      br.close();
  //      var fos = new FileOutputStream(localFile);
  //      var pw = new PrintWriter(fos);
  //      pw.write(buf.toString().toCharArray());
  //      pw.flush();
  //      pw.close();
  //
  //      alterTempData.clear()
  //
  //      HDFSHelper.HDFSUploadFile(localFileRPath, serverDirPath)
  //      localFile.delete()
  //    } catch {
  //      case e => e.printStackTrace();
  //    }
  //
  //  }
  def addRow(RowData: List[Double]) {
    if (!VMType.equals("") && !VMType.equals("addRow")) {
      throw new Exception("Virtual Matrix Already put Data in other Mode , Please Check!")
    } else if (VMType.equals("")) {
      VMType = "addRow"
    }

    var addTime = 2
    if (RowData.length != mSize) {
      if (mSize == 0) {
        addTime = 1
        mSize = RowData.length
        tempResultFileCWriterArray = new Array[java.io.BufferedWriter](mSize)

        for (k <- 0 to mSize - 1) {

          var tempResultFileCSplit = new File(localDirPath + vMatrixID + "C" + k + ".txt")
          tempResultFileCWriterArray(k) = new BufferedWriter(new FileWriter(tempResultFileCSplit, true))

        }
      } else {
        throw new Exception("RowData Error : Length not Equal , Please Check the Row Data!")
      }
    }

    addRowTempData = List(RowData.map(D => D.toString)).:::(addRowTempData)
    addRowCount += 1
    nSize += 1

    if (addRowCount >= 300) {
      addRowToHDFS(addTime)
    }
  }
  private def addRowToHDFS(addTime: Int) {

    var tempResultFile = new File(localFileRPath)

    try {
      tempResultFile.getParentFile().mkdirs();
      var tempResultFileWriter = new BufferedWriter(new FileWriter(tempResultFile, true))

      for (i <- 0 to addRowTempData.length - 1) {

        for (k <- 0 to mSize - 1) {

          if (k != 0) {
            tempResultFileWriter.write(" ")
          }
          if (i != 0 || addTime != 1) {
            tempResultFileCWriterArray(k).write(" ")
          }

          tempResultFileWriter.write(addRowTempData(i)(k))
          tempResultFileCWriterArray(k).write(addRowTempData(i)(k))

        }
        //      hb.putVMRatio(VMRatioPut)
        tempResultFileWriter.newLine()
        nSize += 1

      }
      tempResultFileWriter.close()

      if (addTime == 3) {
        for (k <- 0 to mSize - 1) {
          tempResultFileCWriterArray(k).close()
        }
        tempResultFileCWriterArray = Array()
      }

    } catch {
      case e: IOException => e.printStackTrace()
    }
    addRowTempData = List()

    addRowCount = 0;

  }
  def addRowFlush() {

    addRowToHDFS(3)

    var tempResultFileC = new File(localFileCPath)
    var tempResultFileCWriter = new BufferedWriter(new FileWriter(tempResultFileC, false))
    var tempResultFileCSplitReader: java.io.BufferedReader = null;

    for (i <- 0 to mSize - 1) {
      var tempResultFileCSplit = new File(localDirPath + vMatrixID + "C" + i + ".txt")
      tempResultFileCSplitReader = new BufferedReader(new FileReader(tempResultFileCSplit));
      tempResultFileCWriter.write(tempResultFileCSplitReader.readLine())
      tempResultFileCWriter.newLine()
      tempResultFileCSplitReader.close()
      tempResultFileCSplit.delete()

    }
    tempResultFileCWriter.close()

    HDFSHelper.createDirectoryOnHDFS(serverDirPath)
    HDFSHelper.HDFSUploadFile(localFileRPath, serverDirPath)
    HDFSHelper.HDFSUploadFile(localFileCPath, serverDirPath)

    try {
      var tempResultFile = new File(localFileRPath)
      tempResultFile.delete()
      tempResultFileC.delete()
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def multify(BVirtualMatrix: VirtualMatrix): VirtualMatrix = {

    if (mSize != BVirtualMatrix.nSize && nSize != BVirtualMatrix.mSize) {
      throw new Exception("The size of B VirtualMatrix not correct!")
    }

    var ResultVirtualMatrix = new VirtualMatrix()

    for (i <- 1 to nSize) {

      var ARow = getRow(i)
      var tempRow: List[Double] = List()

      for (j <- 1 to BVirtualMatrix.mSize) {
        var BColumn = BVirtualMatrix.getColumnList(j)
        var tempValue = 0.0

        for (k <- 0 to ARow.length - 1) {
          tempValue += ARow(k) * BColumn(k)
        }

        tempRow = List(tempValue).:::(tempRow)
      }
      ResultVirtualMatrix.addRow(tempRow)
    }
    ResultVirtualMatrix.addRowFlush

    ResultVirtualMatrix
  }

  def clear() {
    HDFSHelper.deleteFileOnHDFS(serverFileRPath)
    HDFSHelper.deleteFileOnHDFS(serverFileCPath)
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
  def getColumnNumString(Num: Int): String = {
    var result = Num.toString
    if (Num < 1000) {
      result = "0" + result
    }
    if (Num < 100) {
      result = "0" + result
    }
    if (Num < 10) {
      result = "0" + result
    }

    result
  }
  def getRowNumString(Num: Int): String = {
    var result = Num.toString
    var i = 10000000

    while (i != 1) {
      if (i > Num) {
        result = "0" + result
      }
      i = i / 10
    }
    result
  }
}


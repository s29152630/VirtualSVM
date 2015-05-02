package svm.demo
import Miner._
import Pre.Processing._
import HDFS.Helper.HDFSHelper
import Virtual.Matrix.ListTrans
import Virtual.Matrix.VirtualMatrix
import svm.VMlibsvm._
import scala.collection.JavaConverters._

object VMdemo {
    val minerConfigGenerator = new MinerConfigGenerator()
    val minerConfig = minerConfigGenerator.generateMinerConfig("103356011", 10, 10, 10, "taiwan", "future", "TXK2", "1s", "20140414084500", "20140417134500", "11004", "RatioRanSubSet", "SVM")
    val result = PreProcManager.excute(minerConfig)
    
    val y = result._2
    
    val HDFSH = new HDFSHelper()
    val VM = new VirtualMatrix()
    
    val Region = "taiwan"
    val Market = "future"
    val SymbolID = "TXK2"
    val TimeGranularity = "1s"
    val stateTag = List("1", "2", "3")
    val ya = List(1, 2, 3).asJava
    
    VM.getDataFromDataBase(Region, Market, SymbolID, TimeGranularity, stateTag, stateTag)
    
    val param = new svm_parameter()
    val start = svm.svm_train(VM, param, ya)
   
    
}
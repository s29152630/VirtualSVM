package HBase.Helper.pre

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client._;

object HBaseHelperS {

  val config = HBaseConfiguration.create();
  config.set("hbase.zookeeper.quorum", "nccu-n06,nccu-n04,nccu-n05");
  val reportTable = new HTable(config, "Stock");
  val VMTable = new HTable(config, "VirtualMatrix");
  val VMRatioTable = new HTable(config, "RatioTable");
  val RKTable = new HTable(config, "VMRowkey");

  def getRow(row: Array[Byte], tableName: String): Result = {
    val g = new Get(row);
    var htable: HTable = null;
    if (tableName.equals("Stock")) {
      htable = reportTable;
    } else if (tableName.equals("VirtualMatrix")) {
      htable = VMTable;
    } else if (tableName.equals("RatioTable")) {
      htable = VMRatioTable;
    } else if (tableName.equals("VMRowkey")) {
      htable = RKTable;
    }

    var rowResult: Result = null;
    try {
      rowResult = htable.get(g);
    } catch {
      case e: IOException => e.printStackTrace();
    }

    return rowResult;
  }


}
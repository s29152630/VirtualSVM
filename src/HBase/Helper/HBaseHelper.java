package HBase.Helper;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.*;

public class HBaseHelper {
	private static HBaseHelper hbaseHelperInstance;
	private static HTable reportTable;
	private static HTable VMTable;
	private static HTable VMRatioTable;
	private static HTable RKTable;

	private HBaseHelper() {
		try {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "nccu-n06,nccu-n04,nccu-n05");
			reportTable = new HTable(config, "Stock");
			VMTable = new HTable(config, "VirtualMatrix");
			VMRatioTable = new HTable(config, "RatioTable");
			RKTable = new HTable(config,"VMRowkey");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HBaseHelper getHBaseHelperInstance() {
		return (reportTable == null || VMTable == null || VMRatioTable == null || RKTable == null) ? (hbaseHelperInstance = new HBaseHelper())
				: hbaseHelperInstance;
	}

	public Result getRow(byte[] row, String tableName) {
		Get g = new Get(row);
		HTable htable = null;
		if (tableName.equals("Stock")) {
			htable = reportTable;
		} else if (tableName.equals("VirtualMatrix")) {
			htable = VMTable;
		} else if (tableName.equals("RatioTable")) {
			htable = VMRatioTable;
		} else if (tableName.equals("VMRowkey")) {
			htable = RKTable;
		}

		Result rowResult = null;
		try {
			rowResult = htable.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rowResult;
	}
	public Result getCell(byte[] row,byte[] family,byte[] qualifier, String tableName) {
		Get g = new Get(row);
		g.addColumn(family, qualifier);
		HTable htable = null;
		if (tableName.equals("Stock")) {
			htable = reportTable;
		} else if (tableName.equals("VirtualMatrix")) {
			htable = VMTable;
		} else if (tableName.equals("RatioTable")) {
			htable = VMRatioTable;
		} else if (tableName.equals("VMRowkey")) {
			htable = RKTable;
		}

		Result rowResult = null;
		try {
			rowResult = htable.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rowResult;
	}

	public HTable getHTable(String tableName) {

		HTable htable = null;
		if (tableName.equals("Stock")) {
			htable = reportTable;
		} else if (tableName.equals("VirtualMatrix")) {
			htable = VMTable;
		} else if (tableName.equals("RatioTable")) {
			htable = VMRatioTable;
		} else if (tableName.equals("VMRowkey")) {
			htable = RKTable;
		}

		return htable;
	}

	public void putVM(Put put) {
		try {
			VMTable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void putVMRatio(Put put) {
		try {
			VMRatioTable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void putRK(Put put) {
		try {
			RKTable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteVMRow(String Row, String tableName) {
		HTable htable = null;
		if (tableName.equals("Stock")) {
			htable = reportTable;
		} else if (tableName.equals("VirtualMatrix")) {
			htable = VMTable;
		} else if (tableName.equals("RatioTable")) {
			htable = VMRatioTable;
		}
		try {
			Delete delete = new Delete(Bytes.toBytes(Row));
			VMTable.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

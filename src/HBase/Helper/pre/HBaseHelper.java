package HBase.Helper.pre;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

public class HBaseHelper {
	private static HBaseHelper hbaseHelperInstance;
	private static Configuration config = HBaseConfiguration.create();
	private static HTable reportTable;
	private static HTable RatioTable;

	private HBaseHelper() {
		try {
			config.set("hbase.zookeeper.quorum", "nccu-n06,nccu-n04,nccu-n05");

			reportTable = new HTable(config, "Stock");
			RatioTable = new HTable(config, "RatioTable");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HBaseHelper getHBaseHelperInstance() {
		return (reportTable == null || RatioTable == null) ? (hbaseHelperInstance = new HBaseHelper())
				: hbaseHelperInstance;
	}

	public Result getRow(byte[] row, String tableName) {
		Get g = new Get(row);
		HTable htable = null;
		if (tableName.equals("Stock")) {
			htable = reportTable;
		} else if (tableName.equals("RatioTable")) {
			htable = RatioTable;
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
		} else if (tableName.equals("RatioTable")) {
			htable = RatioTable;
		}

		return htable;
	}
}
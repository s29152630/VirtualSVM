package Virtual.Matrix;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import HDFS.Helper.HDFSHelper;
import Virtual.Matrix.VirtualMatrix.getColumn;

public class VMTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// List<String> stateTagList = new ArrayList<String>();
		// StateTaRuleMap a = new StateTaRuleMap();
		// java.util.Map<String, Integer> s = a.getMap();
		//
		// for (Map.Entry<String, Integer> entry : s.entrySet()) {
		// stateTagList.add(entry.getValue().toString()); 
		// }
		// StateTaRuleMap a = new StateTaRuleMap();
		// Map<String, Integer> s = a.getMap();
		//
		// for (Map.Entry<String, Integer> entry : s.entrySet()) {
		// stateTagList.add(entry.getValue().toString());
		// }
		//
		// List<String> dateTimeList = new ArrayList<String>();
		// List<String> label = new ArrayList<String>();
		// Long t = 20150115000000L;
		// int index = 0;
		// for (int k = 9; k <= 12; k++) {
		// for (int i = 0; i <= 59; i++) {
		// for (int j = 0; j <= 59; j++) {
		// label.add(index, "1");
		// dateTimeList.add(index,
		// String.valueOf(t + k * 10000 + i * 100 + j));
		// index++;
		// }
		// }
		// }

		HDFSHelper HDFSH = new HDFSHelper();
		VirtualMatrix VM = new VirtualMatrix();

		// DateFormat dateFormat = new SimpleDateFormat("mm:ss:SSS");
		//
		// long inStartTime = System.currentTimeMillis();
		//
		// VM.getDataFromDataBase("taiwan", "future", "TXF", "1s",
		// ListTrans.TransListString(stateTagList),
		// ListTrans.TransListString(dateTimeList));
		//
		// long inTotTime = System.currentTimeMillis() - inStartTime;
		//
		// System.out.println("Finished Initialize!!");
		//
		// long gCStartTime = System.currentTimeMillis();
		//
		// getColumn gC = VM.getColumn();
		//
		// gC.setColumnNum(5);
		// for (int i = 1; i <= VM.nSize(); i++) {
		// gC.Next();
		// }
		// long gCTotTime = System.currentTimeMillis() - gCStartTime;
		//
		// gC.Close();
		// gC.setColumnNum(5);
		// for (int i = 1; i <= VM.nSize(); i++) {
		// System.out.println(gC.Next() + " ");
		// }
		//
		// // long gCLStartTime = System.currentTimeMillis();
		// //
		// // scala.collection.immutable.List gCL = VM.getColumnList(5);
		// //
		// // long gCLTotTime = System.currentTimeMillis() - gCLStartTime;
		// //
		// // for (int i = 0; i <= gCL.length() - 1; i++) {
		// // System.out.print(gCL.apply(i) + " ");
		// // }
		//
		// // long roStartTime = System.currentTimeMillis();
		// //
		// // scala.collection.immutable.List<String> row = VM.getRow(2);
		// //
		// // long roTotTime = System.currentTimeMillis() - roStartTime;
		// //
		// // for (int i = 0; i < row.length(); i++) {
		// // System.out.print(row.apply(i));
		// // }
		// // System.out.println();
		// // long coStartTime = System.currentTimeMillis();
		// //
		// // VM.getColumn().setColumnNum(2);
		// // for (int i = 0; i < VM.nSize(); i++) {
		// // System.out.print(VM.getColumn().Next() + ",");
		// // }
		// //
		// // long coTotTime = System.currentTimeMillis() - coStartTime;
		// System.out.println(VM.nSize() + "x" + VM.mSize() + "Matrix!");
		// System.out.println("Initialize Time:" +
		// dateFormat.format(inTotTime));
		// System.out.println("getColumn Time:" + dateFormat.format(gCTotTime));
		// // System.out.println("getColumnList Time:"
		// // + dateFormat.format(gCLTotTime));
		// // System.out.println("getRow Time:" + dateFormat.format(roTotTime));
		// // System.out.println("getColumn Time:" +
		// dateFormat.format(coTotTime));
		// // System.out.println("VMatrix size :" + VM.nSize() + "x" +
		// VM.mSize());

	}
}

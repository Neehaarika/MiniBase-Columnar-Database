/* Author - SUHAS */
package tests2;

import columnar.Columnarfile;
import columnar.TupleScan;
import global.*;
import heap.Tuple;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class BatchInsert2 {

	public static void main(String[] args) throws Exception  {
		if (args.length < 4) {
			throw new Exception("Invalid number of attributes.");
		} else {
			BufferedInputStream readStream = null;
			BufferedReader reader = null;
			String datafilename = args[0];
			String columndbname = args[1];
			String columnarfilename = args[2];

			int numcolumns;
			try {
				numcolumns = Integer.parseInt(args[3]);
			} catch (Exception e) {
				throw new Exception("NUMCOULMNS is not integer.");
			}

			AttrType[] attrTypes = new AttrType[numcolumns];
			String[] attrNames = new String[numcolumns];
			short[] attrSizes = new short[numcolumns];
			ArrayList<TID> tids = new ArrayList<TID>();
			byte[] recordByteData = null;
			int recordLength = 0;
			String line = null;


			try {
				/* Create a DB */
				SystemDefs columnDb = new SystemDefs(columndbname,500000,1000,null);

				/* Parse the input file - Prefix of the path needs to be hard-coded (Adding data file in a specific folder) */
				readStream = new BufferedInputStream(new FileInputStream(datafilename));
				reader = new BufferedReader(new InputStreamReader(readStream));

				/* Parse the column header in the data file */
				String headerRow = reader.readLine();
				String[] headers = headerRow.split("\t");
				for (int i = 0; i < numcolumns; i++) {
					String[] column = headers[i].split(":"); // column name and type split (Refer am input data file)
					attrNames[i] = column[0];
					if (column[1].equals("int")) {
						attrTypes[i] = new AttrType(AttrType.attrInteger);
						attrSizes[i] = 4;
						recordLength += 4; //integer occupies 4 bytes in java
					} else if(column[1].startsWith("char")) {
						String size = column[1].substring("char(".length(), column[1].length()-1);
						attrTypes[i] = new AttrType(AttrType.attrString);
						attrSizes[i] = Short.parseShort(size);
						recordLength += Short.parseShort(size);
					} else {
						throw new Exception("column attr type is not supported.");
					}
				}

				/* Create a columnar file */
				Columnarfile columnfile = new Columnarfile(columnarfilename, numcolumns, attrNames, attrTypes, attrSizes);

				/* Start parsing from the next row which holds data */
				int count = 0;
				List<TID> deleteTids = new ArrayList<>();
				while ((line = reader.readLine()) != null) {
					count += 1;
					String[] columnValues = line.split("\t");
					int span = 0;
					/* Create tuple for each data record */
					recordByteData = new byte[recordLength];

					for (int i = 0; i < numcolumns; i++) {
						if (attrTypes[i].attrType == AttrType.attrInteger) {
							Convert.setIntValue(Integer.parseInt(columnValues[i]), span, recordByteData);
							span += attrSizes[i];
						} else {
							Convert.setStrValue(columnValues[i], span, recordByteData);
							span += attrSizes[i];
						}
					}

					/* Insert tuple into columnar file */
					TID tid = columnfile.insertTuple(recordByteData);
//					System.out.println("RID ->");
//					for(RID rid: tid.recordIDs) {
//						System.out.println("Page "+rid.pageNo.pid+" "+"Slot: "+rid.slotNo);
//					}
					tids.add(tid);
					if(count==20) break;
				}

//				int k=0;
//				for(TID t: tids) {
//					int j=0;
//					for (RID rid: t.recordIDs) {
//						columnfile.getHeapfiles()[j].deleteRecord(rid);
//						j += 1;
//					}
//					k += 1;
//					if(k==34) break;
//				}
//
//
				RID rid = tids.get(10).recordIDs[1];
				System.out.println("Page "+rid.pageNo.pid+" "+"Slot: "+rid.slotNo);
				int p = columnfile.getHeapfiles()[1].findPosition(rid);
				System.out.println("Position "+p);
				RID rid2 = columnfile.getHeapfiles()[2].findRID(p);
				System.out.println("Page "+rid2.pageNo.pid+" "+"Slot: "+rid.slotNo);
//				System.out.println("Position "+columnfile.getHeapfiles()[2].findPosition(rid, 125));
//				System.out.println("Position "+columnfile.getHeapfiles()[3].findPosition(rid, 125));


				/* Scanning each tuple */
				TupleScan scanner = new TupleScan(columnfile);

				TID tid = new TID(columnfile.getFieldCount());
				Tuple tuple;
				while ((tuple=scanner.getNext(tid))!=null) {
					System.out.println(Convert.getStrValue(0, tuple.returnTupleByteArray(), 25)
							+", "+Convert.getStrValue(25, tuple.returnTupleByteArray(), 25)
							+", "+Convert.getIntValue(50, tuple.returnTupleByteArray())
							+", "+Convert.getIntValue(54, tuple.returnTupleByteArray()));
				}
				/* Printing each inserted tuple */
//				for(TID tid : tids) {
////					Tuple record = scanner.getNext(tid);
////					record.print(attrTypes);
//				}
			} catch (Exception ex) {

				/* Any exception is caught here */
				ex.printStackTrace();

			} finally {
				readStream.close();
				reader.close();
			}
		}

	}

}
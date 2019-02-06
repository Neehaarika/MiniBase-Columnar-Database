package bitmap;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.Heapfile;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DemoBitmap {

	/**
	 * Parses the data file and inserts the data into heap files corresponding to columns
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			args = new String[] {"sampledata.txt", "testdb", "cf", "6"};
			if (args.length < 4) {
				throw new Exception("Invalid number of attributes.");
			} else {
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
				byte[] recordByteData = null;
				int recordLength = 0;
				String line = null;

				try {
					PCounter.initialize();
					int startReadCount = PCounter.rcounter;
					int startWriteCount = PCounter.wcounter;
					/* Create a DB */
					if(new File(columndbname).exists()) {
						SystemDefs columnDb = new SystemDefs(columndbname, 0, 1024, null);
					} else {
						SystemDefs columnDb = new SystemDefs(columndbname, 1024*1024, 1024, null);
					}

					/* Create a columnar file */
					Columnarfile columnfile = new Columnarfile(columnarfilename);
//					columnfile.createBitMapIndex(5);
//					Heapfile hf = columnfile.getHeapfiles()[0];
					String filename =  "my.6.bm";
//					BitMapFile bitMapFile = new BitMapFile(filename, columnfile, 0, new StringValue("South_Dakota"));
//					BitMapFile bitMapFile = new BitMapFile(filename, columnfile);
//					int[] posArr = {10, 1500, 2500, 5000, 5010, 7500, 10000, 12000, 16000, 20000, 24000};
//					for(int pos:posArr) {
//						bitMapFile.set(pos);
//					}
//					bitMapFile.saveToDisk();

					//Flushing all written data to disk.
					//TODO handle flush gracefully
					try {
						SystemDefs.JavabaseBM.flushAllPages();
					} catch (Exception e) {
//						e.printStackTrace();
					}
//					List<Integer> list = new ArrayList<>();
//					list.add(5000);
//					list.add(5010);
//					list.add(7500);
//					List<Integer> rangeList = new ArrayList<>();
//					List<Integer> delDirOffsets = new ArrayList<>();
//					delDirOffsets.add(2);
//					delDirOffsets.add(1);
//					int totalPositionsInDir = 83 * 32;
//					Collections.sort(delDirOffsets);
//					for(int offset: delDirOffsets) {
//						rangeList.add(offset*totalPositionsInDir);
//						rangeList.add((offset+1)*totalPositionsInDir);
//					}
					BitMapFile bitMapFile2 = new BitMapFile(filename);
//					bitMapFile2.purgeDelete(list, rangeList);
//					bitMapFile2.delete(20000);
//					bitMapFile2.delete(5000);
					//TODO handle flush gracefully
					try {
						SystemDefs.JavabaseBM.flushAllPages();
					} catch (Exception e) {
//						e.printStackTrace();
					}
					int endReadCount = PCounter.rcounter;
					int endWriteCount = PCounter.wcounter;
					System.out.println("Read Page Count: "+(endReadCount-startReadCount));
					System.out.println("Write Page Count: "+(endWriteCount-startWriteCount));
				} catch (Exception ex) {
					throw ex;
				} finally {

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
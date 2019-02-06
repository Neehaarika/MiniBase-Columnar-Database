package input;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.Convert;
import global.SystemDefs;
import heap.Heapfile;

import java.io.*;

public class DemoInfo {

	/**
	 * Parses the data file and inserts the data into heap files corresponding to columns
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			String columndbname = "db";
			String columnarfilename = "cf";
			int numcolumns = 4;
			PCounter.initialize();
			int startReadCount = PCounter.rcounter;
			int startWriteCount = PCounter.wcounter;

					/* Create a DB */
			if(new File(columndbname).exists()) {
				SystemDefs columnDb = new SystemDefs(columndbname, 0, 1024, null);
			} else {
				SystemDefs columnDb = new SystemDefs(columndbname, 1024*1024, 1024, null);
			}

			Columnarfile columnfile = new Columnarfile(columnarfilename);
			Heapfile hf = new Heapfile(columnarfilename+".hdr");
			System.out.println("*************Header File*****************");
			hf.printDirMetaInfo();
			hf = new Heapfile(columnarfilename+".dtid");
			System.out.println("*************Deleted TID File*****************");
			hf.printDirMetaInfo();
			for(int i=0; i<numcolumns; i++) {
				hf = columnfile.getHeapfiles()[i];
				System.out.println("*************COL File "+i+"*****************");
				hf.printDirMetaInfo();
			}


			//Flushing all written data to disk.
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
			ex.printStackTrace();
		} finally {

		}
	}
}
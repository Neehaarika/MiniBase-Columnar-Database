package input;

import java.io.*;
import java.util.*;

import diskmgr.PCounter;
import global.*;
import heap.*;
import columnar.*;

public class BatchInsert {

	/**
	 * Parses the data file and inserts the data into heap files corresponding to columns
	 * @param args
	 */
	public void insert(String[] args) {
		try {
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
				byte[] recordByteData = null;
				int recordLength = 0;
				String line = null;

				try {

					/* Parse the input file - Prefix of the path needs to be hard-coded (Adding data file in a specific folder) */
					readStream = new BufferedInputStream(new FileInputStream(datafilename));
					reader = new BufferedReader(new InputStreamReader(readStream));

					PCounter.initialize();
					int startReadCount = PCounter.rcounter;
					int startWriteCount = PCounter.wcounter;

					/* Create a DB */
					if(new File(columndbname).exists()) {
						SystemDefs columnDb = new SystemDefs(columndbname, 0, 1024, null);
					} else {
						SystemDefs columnDb = new SystemDefs(columndbname, 1024*1024, 1024, null);
					}

					/* Parse the column header in the data file */
					String headerRow = reader.readLine();
					String[] headers = headerRow.split("\t");
					if(headers.length > numcolumns || numcolumns == 0)
						throw new Exception("Number of columns specified does not match the number of columns in data file");
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
							attrSizes[i] = (short) Integer.parseInt(size);
							recordLength += attrSizes[i]+2; //strlen+2
						} else {
							throw new Exception("column attr type is not supported.");
						}
					}

					/* Create a columnar file */
					Columnarfile columnfile = new Columnarfile(columnarfilename, numcolumns, attrNames, attrTypes, attrSizes);

					/* Start parsing from the next row which holds data */
					while ((line = reader.readLine()) != null) {
						String[] columnValues = line.split("\t");
						int span = 0;
						/* Create tuple for each data record */
						recordByteData = new byte[recordLength];

						for (int i = 0; i < numcolumns; i++) {
							if (attrTypes[i].attrType == AttrType.attrInteger) {
								Convert.setIntValue(Integer.parseInt(columnValues[i]), span, recordByteData);
								span += attrSizes[i];
							} else {
								if(columnValues[i].length()>attrSizes[i]) {
									throw new Exception("column value exceeds size limit");
								}
								Convert.setStrValue(columnValues[i], span, recordByteData);
								span += attrSizes[i]+2; //strlen+2
							}
						}

						/* Insert tuple into columnar file */
						columnfile.insertTuple(recordByteData);
					}

					System.out.println("Record count: "+columnfile.getTupleCnt());

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
					System.out.println("Read Pages: "+PCounter.readPages);
					System.out.println("Wrote Pages: "+PCounter.writePages);
//					System.out.println("Pinned Pages: "+PCounter.currentlyPinnedPages);
				} catch (Exception ex) {
					throw ex;
				} finally {
					if(readStream!=null) {
						readStream.close();
						reader.close();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
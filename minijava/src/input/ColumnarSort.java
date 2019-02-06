package input;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import btree.UnpinPageException;
import bufmgr.BufferPoolExceededException;

import java.util.Random;
import java.util.Set;

import columnar.Columnarfile;
import diskmgr.PCounter;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;

public class ColumnarSort {
	AttrType[] in;
	short len_in;
	short[] str_sizes;
	private Columnarfile columnarFile;
	int sort_fld;
	int sort_fld_len;
	int n_pages;

	private String columnDBName;
	private int startReadCount;
	private int startWriteCount;

	private String columnarFileName;
	private String constraintStr;
	private String projectionStr;
	private String[] targetColNames;
	private String[] projectionColNames;
	private TupleOrder sort_order;
	private Integer numBuf;
	private Integer numBufForSort;
	private String heapFilePrefix;

	ColumnarSort() {
	}

	ColumnarSort(AttrType[] in, short len_in, short[] str_sizes, java.lang.String ColumnarFileName, int sort_fld,
			TupleOrder sort_order, int sort_fld_len, int n_pages) {

	}

	public void execute(String[] sortArgs) {

		// sort COLUMNDB COLUMNARFILE SORTCOLUMN SORT_ORDER PROJECTION_ORDER
		// TOTAL_BUFFER_ALLOCATED NUMBUF
		try {
			if (sortArgs.length < 7) {
				throw new Exception("Invalid number of attributes.");
			} else {
				columnDBName = sortArgs[0];
				if (!new File(columnDBName).exists()) {
					throw new Exception("Database does not exist.");
				}

				columnarFileName = sortArgs[1];

				constraintStr = sortArgs[2];
				if (!(constraintStr.startsWith("[") && constraintStr.endsWith("]"))) {
					throw new Exception("[TARGETCOLUMNNAMES] format invalid.");
				} else {
					targetColNames = constraintStr.substring(1, constraintStr.length() - 1).trim().split("\\s*,\\s*");
					if (targetColNames.length < 1) {
						throw new Exception("No target columns given.");
					}
				}

				projectionStr = sortArgs[3];
				if (!(projectionStr.startsWith("[") && projectionStr.endsWith("]"))) {
					throw new Exception("[PROJECTIONCOLUMNNAMES] format invalid.");
				} else {
					projectionColNames = projectionStr.substring(1, projectionStr.length() - 1).trim()
							.split("\\s*,\\s*");
					if (projectionColNames.length < 1) {
						throw new Exception("No projection columns given.");
					}
				}

				String sortOrder = sortArgs[4];
				if (sortOrder.equals("ASC")) {
					sort_order = new TupleOrder(TupleOrder.Ascending);
				} else if (sortOrder.equals("DSC")) {
					sort_order = new TupleOrder(TupleOrder.Descending);
				} else {
					throw new Exception("This sorting order is not supported");
				}

				try {
					numBuf = Integer.parseInt(sortArgs[5]);
				} catch (Exception e) {
					throw new Exception("NUMBUF is not integer.");
				}
				if (numBuf < 1) {
					throw new Exception("NUMBUF is not more than 1.");
				}

				try {
					numBufForSort = Integer.parseInt(sortArgs[6]);
				} catch (Exception e) {
					throw new Exception("NUMBUF is not integer.");
				}
				if (numBufForSort < 3) {
					System.out.println("NUMBUF_SORT is less than 3. External Sort Merge needs minimum 3 pages for the operation");
				    return;	
				}

				SystemDefs columnDb = new SystemDefs(columnDBName, 0, numBuf, null);

				columnarFile = new Columnarfile(columnarFileName);
				// Initialize counter cycle
				PCounter.initialize();
				startReadCount = PCounter.rcounter;
				startWriteCount = PCounter.wcounter;

				////// LOGIC TO SORT ///////

				Random rand = new Random();

				// Generate random integers in range 0 to 999
				int rand_int1 = rand.nextInt(1000);
				heapFilePrefix = columnarFileName + rand_int1 + "_Run_";
				System.out.println("HeapFilePrefix:" + heapFilePrefix);

				// PREPROCESSING INPUT
				String consAttr[] = constraintStr.substring(1, constraintStr.length() - 1).trim().split(",");
				int[] columnIndexesToBeSorted = new int[consAttr.length];
				int[] columnIndexesToBeProjected = new int[projectionColNames.length];
				for (int f = 0; f < projectionColNames.length; f++) {
					columnIndexesToBeProjected[f] = columnarFile.colNameToIndex(projectionColNames[f]);
				}
				AttrType[] attributeTypes = new AttrType[consAttr.length];
				short[] attrSizes = new short[consAttr.length];
				for (int t = 0; t < consAttr.length; t++) {
					columnIndexesToBeSorted[t] = columnarFile.colNameToIndex(consAttr[t]);
					attributeTypes[t] = columnarFile.getAttributeTypes()[columnIndexesToBeSorted[t]];
					attrSizes[t] = columnarFile.getAttrSizes()[columnIndexesToBeSorted[t]];
				}

				// ALL COMPARATORS REQUIRED FOR SORTING
				Comparator<Tuple> columnarSortComparator = new Comparator<Tuple>() {
					@Override
					public int compare(Tuple one, Tuple two) {
						int span = 0;
						int returnValue = 0;
						try {
							for (int i = 0; i < attrSizes.length; i++) {
								if (attributeTypes[i].attrType == AttrType.attrInteger) {
									int ival1 = Convert.getIntValue(span, one.returnTupleByteArray());
									int ival2 = Convert.getIntValue(span, two.returnTupleByteArray());
									span += attrSizes[i];
									if (Integer.compare(ival2, ival1) != 0) {
										if (sort_order.tupleOrder == TupleOrder.Descending) {
											returnValue = Integer.compare(ival2, ival1);
										} else {
											returnValue = Integer.compare(ival1, ival2);
										}
										break;
									}
								} else {
									String sval1 = Convert.getStrValue(span, one.returnTupleByteArray(), attrSizes[i]);
									String sval2 = Convert.getStrValue(span, two.returnTupleByteArray(), attrSizes[i]);
									span += attrSizes[i] + 2;
									if (sval1.compareTo(sval2) != 0) {
										if (sort_order.tupleOrder == TupleOrder.Descending) {
											returnValue = sval2.compareTo(sval1);
										} else {
											returnValue = sval1.compareTo(sval2);
										}
										break;
									}
								}
							}
						} catch (Exception e) {
						}
						return returnValue;
					}
				};

				Comparator<Map.Entry<Integer, Tuple>> tupleMapComparator = new Comparator<Map.Entry<Integer, Tuple>>() {
					@Override
					public int compare(Entry<Integer, Tuple> o1, Entry<Integer, Tuple> o2) {
						if (o1.getValue() == null) {
							return (o2.getValue() == null) ? 0 : 1;
						}
						if (o2.getValue() == null) {
							return -1;
						}
						return columnarSortComparator.compare(o1.getValue(), o2.getValue());
					}

				};

				///////////////////////////////////////////////////////////////////////////////////////////////////////

				// Putting the values from different columnar file based on the columns
				// specified on which
				// the ordering has to be done and the positions into a single file.

				Heapfile heapfile = new Heapfile(heapFilePrefix + "rec-pos.");
				createTupleWithPositionFromColumnarFile(heapfile, columnarFile, columnIndexesToBeSorted, attributeTypes,
						attrSizes);

				///////////////////////////////////////////////////////////////////////////////////////////////////////
				// External-Merge-Sort

				List<PageId> initialPageOnlySortedPageIds = new ArrayList<PageId>();
				initialPageOnlySortedPageIds = heapfile.getListOfPagesInTheHeapFile();
				int inputBuffer = numBufForSort;
				int numOfPages = initialPageOnlySortedPageIds.size();
				int availableInputBuffer = inputBuffer - 1;
				int runLength = (int) Math.ceil(Math.log(numOfPages) / Math.log(availableInputBuffer));
				Heapfile resultHeapFile = null;
				for (int i = 0; i < runLength; i++) {
					System.out.println("PASS NO " + i);
					List<PageId> runSortedPageIds = new ArrayList<PageId>();
					Heapfile heapfileRunPrev = null;
					if (i == 0) {
						runSortedPageIds = initialPageOnlySortedPageIds;
						heapfileRunPrev = heapfile;
					} else {
						heapfileRunPrev = new Heapfile(heapFilePrefix + (i - 1));
						runSortedPageIds = heapfileRunPrev.getListOfPagesInTheHeapFile();
						System.out.println("New Page IDs:" + runSortedPageIds + "Size:" + runSortedPageIds.size());

					}
					Heapfile heapfileRunCurrent = new Heapfile(heapFilePrefix + i);
					int runSize = (int) Math.pow(availableInputBuffer, i);
					int noOfSplits = numOfPages / runSize;
					int lastSplit = numOfPages % runSize;
					if (lastSplit > 0) {
						noOfSplits++;
					}

					// Dividing the pages of the heap file according to the run length of each pass
					List<List<PageId>> runPageArray = new ArrayList<List<PageId>>();
					for (int j = 0; j < noOfSplits; j++) {
						int startPageIdIndex = j * runSize;
						int endPageIDIndex = startPageIdIndex + runSize;
						if (j == noOfSplits - 1) {
							runPageArray.add(runSortedPageIds.subList(startPageIdIndex, numOfPages));
						} else {
							runPageArray.add(runSortedPageIds.subList(startPageIdIndex, endPageIDIndex));
						}
					}
					System.out.println("RunArray:" + runPageArray + "Size:" + runPageArray.size());

					// External Merge sort of m pages from a single page run with n another page
					// runs
					for (int k = 0; k < runPageArray.size(); k = k + availableInputBuffer) {
						RID[] currentRidArray = new RID[availableInputBuffer];
						PageId[] currentPageIdArray = new PageId[availableInputBuffer];
						HFPage[] currentDataPage = new HFPage[availableInputBuffer];
						int pos = 0;
						for (int m = k; m < k + availableInputBuffer && m < runPageArray.size(); m++) {
							PageId runPageId = runPageArray.get(m).get(0);
							currentPageIdArray[pos] = runPageId;
							currentDataPage[pos] = getDataPageOfGivenPageId(runPageId.pid);
							currentRidArray[pos] = currentDataPage[pos].firstRecord();
							pos++;
						}
						// Sort and merge n buffer unsorted pages in the first run
						if (i == 0) {
							List<Tuple> tupleList = new ArrayList<Tuple>();
							while (checkAllPointerAreNotNull(currentRidArray)) {
								for (RID currentRid : currentRidArray) {
									Tuple ter = null;
									if (currentRid != null) {
										ter = heapfileRunPrev.getRecord(currentRid);
									}
									if (ter != null) {
										tupleList.add(ter);
									}

								}
								for (int m = 0; m < currentRidArray.length; m++) {
									if (currentRidArray[m] != null) {
										currentRidArray[m] = currentDataPage[m].nextRecord(currentRidArray[m]);
									}
								}

							}
							Collections.sort(tupleList, columnarSortComparator);
							for (Tuple tuple : tupleList) {
								heapfileRunCurrent.insertRecord(tuple.getTupleByteArray());
							}
							for (int m = 0; m < currentPageIdArray.length; m++) {
								if (currentPageIdArray[m] != null) {
									unpinPage(currentPageIdArray[m], false);
								}
							}

						} else {// All the subsequent passes are sort and merge passes
							while (checkAllPointerAreNotNull(currentRidArray)) {
								int position = getTupleRiDToBeInsertedFromAllPages(heapfileRunPrev, currentRidArray,
										tupleMapComparator);

								// Insert Into the Heap File
								heapfileRunCurrent.insertRecord(
										heapfileRunPrev.getRecord(currentRidArray[position]).getTupleByteArray());

								currentRidArray[position] = currentDataPage[position]
										.nextRecord(currentRidArray[position]);
								if (currentRidArray[position] == null) {
									if (currentPageIdArray[position] != null) {
										unpinPage(currentPageIdArray[position], false);
									}
									currentPageIdArray[position] = getNextPageFromRunArrayForGivenPageID(runPageArray,
											currentPageIdArray[position]);
									if (currentPageIdArray[position] != null) {
										currentDataPage[position] = getDataPageOfGivenPageId(
												currentPageIdArray[position].pid);
										currentRidArray[position] = currentDataPage[position].firstRecord();
									}

								}
							}
						}
					}
					if (i == runLength - 1) {
						resultHeapFile = heapfileRunCurrent;
					}
					heapfileRunPrev.deleteFile();
				}

				// printRecordsByPages(resultHeapFile,attrSizes,attributeTypes);

				// Printing the resultant Heap File
				Scan tempHeapScan = resultHeapFile.openScan();
				RID ridtmp = new RID();
				Tuple outTuple = null;
				int fcounter = 0;
				System.out.println("SORTED COLUMNS");
				while ((outTuple = tempHeapScan.getNext(ridtmp)) != null) {
					projectAndPrintDataOfGivenTuple(columnarFile, columnIndexesToBeSorted, columnIndexesToBeProjected,
							attrSizes, attributeTypes, outTuple);
					fcounter++;
					// break;
				}
				System.out.println(fcounter);
				resultHeapFile.deleteFile();
				// Flushing all written data to disk.
				// TODO handle flush gracefully
				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception em) {
					// e.printStackTrace();
				}

				printReadWriteCounter(startReadCount, startWriteCount, null);
				System.out.println("=======================EXTRA METAINFO===============================");
				System.out.println(columnarFile.getTupleCnt());
			}
		} 
		catch (BufferPoolExceededException | HFBufMgrException e) {
			System.out.println("There were not enough buffers available to run this operation."+"\n"
					+ "Please make sure that the sort buffer should be atleast 1 less that the total buffer size given in the input.");
		}
        catch (Exception e) {
			e.printStackTrace();
		}
		// TODO Auto-generated method stub

	}

	/**
	 * This function just prints the read and write counts.
	 * 
	 * @param startReadCount
	 * @param startWriteCount
	 * @param printString
	 */
	public void printReadWriteCounter(int startReadCount, int startWriteCount, String printString) {
		int endReadCount = PCounter.rcounter;
		int endWriteCount = PCounter.wcounter;
		if (printString != null) {
			System.out.println(printString + ":Read Page Count: " + (PCounter.getReleventReadCount())
					+ " Write Page Count: " + (endWriteCount - startWriteCount));
		} else {
			System.out.println("Read Page Count: " + (PCounter.getReleventReadCount()));
			System.out.println("Write Page Count: " + (endWriteCount - startWriteCount));
		}
		System.out.println("Read Pages: "+PCounter.readPages);
		System.out.println("Wrote Pages: "+PCounter.writePages);
	}

	/**
	 * Given a tuple object it will print the values from the tuple, then if the
	 * projected indexes are not available in the tuple it will fetch from the
	 * columnar file and prints.
	 * 
	 * @param columnarFile
	 * @param columnIndexesToBeSorted
	 * @param columnIndexesToBeProjected
	 * @param attrSizes
	 * @param attributeTypes
	 * @param outTuple
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	private void projectAndPrintDataOfGivenTuple(Columnarfile columnarFile, int[] columnIndexesToBeSorted,
			int[] columnIndexesToBeProjected, short[] attrSizes, AttrType[] attributeTypes, Tuple outTuple)
			throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException,
			HFDiskMgrException, Exception {

		Set<Integer> s2 = getColumnIndexesToBeFetchedFromColumnarFile(columnIndexesToBeSorted,
				columnIndexesToBeProjected);
		AttrType[] _in1 = columnarFile.getAttributeTypes();
		Map<Integer, Object> outputMap = new HashMap<Integer, Object>();

		int span = 0;
		for (int i = 0; i < columnIndexesToBeSorted.length; i++) {
			try {
				switch (attributeTypes[i].attrType) {
				case AttrType.attrInteger:
					int ival = Convert.getIntValue(span, outTuple.returnTupleByteArray());
					outputMap.put(columnIndexesToBeSorted[i], ival);
					span += attrSizes[i];
					break;
				case AttrType.attrString:
					String sval = Convert.getStrValue(span, outTuple.returnTupleByteArray(), attrSizes[i]);
					outputMap.put(columnIndexesToBeSorted[i], sval);
					span += attrSizes[i] + 2;
					break;
				case AttrType.attrReal:
				case AttrType.attrNull:
				case AttrType.attrSymbol:
					break;
				}
			} finally {

			}
		}
		int position = Convert.getIntValue(span, outTuple.returnTupleByteArray());

		for (int i : columnIndexesToBeProjected) {
			if (s2.contains(i)) {
				Heapfile HfTmp = columnarFile.getHeapfiles()[i];
				// System.out.println("i"+i);
				RID ridFromPos = null;
				Tuple tup = null;
				ridFromPos = HfTmp.findRID(position);
				if (ridFromPos != null) {
					tup = HfTmp.getRecord(ridFromPos);
				} else {
					System.out.print("NULL");
					System.out.print(position);
				}
				if (tup != null) {
					printDataOfGivenTupleWithoutPosition(i, _in1[i], tup);
				}
			} else {
				if (_in1[i].attrType == AttrType.attrInteger) {
					Integer temp = (Integer) outputMap.get(i);
					System.out.print(temp + " ");
				} else {
					String temp2 = (String) outputMap.get(i);
					System.out.print(temp2 + " ");
				}
			}

		}
		System.out.print(":" + position);
		System.out.println();

	}

	/**
	 * This function gets the columnar index from the Columnar file
	 * 
	 * @param columnIndexesToBeSorted
	 * @param columnIndexesToBeProjected
	 * @return
	 */
	public Set<Integer> getColumnIndexesToBeFetchedFromColumnarFile(int[] columnIndexesToBeSorted,
			int[] columnIndexesToBeProjected) {
		List<Integer> sorteColumnlist = new ArrayList<>();
		for (int i : columnIndexesToBeSorted) {
			sorteColumnlist.add(i);
		}

		List<Integer> projColumnlist = new ArrayList<>();
		for (int i : columnIndexesToBeProjected) {
			projColumnlist.add(i);
		}
		Set<Integer> s = new HashSet<Integer>(sorteColumnlist);
		Set<Integer> s2 = new HashSet<Integer>(projColumnlist);
		s2.removeAll(s);
		return s2;
	}

	/**
	 * This function takes the columnar file, and extract values for all the columns
	 * needed and add the position of each tuple to the tuple and saves it into the
	 * given input heap file.
	 * 
	 * @param heapfile
	 * @param columnarFile2
	 * @param columnIndexesToBeSorted
	 * @param attributeTypes
	 * @param attrSizes
	 * @throws Exception
	 */
	private void createTupleWithPositionFromColumnarFile(Heapfile heapfile, Columnarfile columnarFile2,
			int[] columnIndexesToBeSorted, AttrType[] attributeTypes, short[] attrSizes) throws Exception {
		Heapfile[] columnarHeapFile = new Heapfile[columnIndexesToBeSorted.length];
		RID rid = new RID();
		Scan[] columnScans = new Scan[columnIndexesToBeSorted.length];
		Tuple[] tuples = new Tuple[columnIndexesToBeSorted.length];
		for (int i = 0; i < columnIndexesToBeSorted.length; i++) {
			columnarHeapFile[i] = columnarFile2.getHeapfiles()[columnIndexesToBeSorted[i]];
			columnScans[i] = columnarHeapFile[i].openScan();
		}
		int count = 0;
		boolean breakLoop = false;
		while (true) {
			for (int j = 0; j < columnIndexesToBeSorted.length; j++) {
				tuples[j] = columnScans[j].getNext(rid);
				if (tuples[j] == null) {
					breakLoop = true;
					break;
				}
			}
			if (!breakLoop) {
				byte[] tmpli = createTupleWithPositionForAllGivenTuples(attributeTypes, attrSizes, tuples, count);
				heapfile.insertRecord(tmpli);
				count++;
			} else {
				// System.out.println("Records Saved with position:" + count);
				break;
			}
		}
		for (int i = 0; i < columnIndexesToBeSorted.length; i++) {
			columnScans[i].closescan();
		}
	}

	/**
	 * This function prints records page by page given a heap file.
	 * 
	 * @param heapfile
	 * @param attrSizes
	 * @param attrTypes
	 * @throws InvalidTupleSizeException
	 * @throws IOException
	 */
	private void printRecordsByPages(Heapfile heapfile, short[] attrSizes, AttrType[] attrTypes)
			throws InvalidTupleSizeException, IOException {
		Tuple outputTuple;
		Scan HeapFileRunScan = heapfile.openScan();
		int tempcount = 0;
		int pageCount = 1;
		RID ridtmp = new RID();
		while (true) {
			while ((outputTuple = HeapFileRunScan.getNextInSinglePage(ridtmp)) != null) {
				int span = 0;
				for (int i = 0; i < attrTypes.length; i++) {
					try {
						switch (attrTypes[i].attrType) {
						case AttrType.attrInteger:
							int ival = Convert.getIntValue(span, outputTuple.returnTupleByteArray());
							System.out.print(ival + " ");
							span += attrSizes[i];
							break;
						case AttrType.attrString:
							String sval = Convert.getStrValue(span, outputTuple.returnTupleByteArray(),
									columnarFile.getAttrSizes()[i]);
							System.out.print(sval + " ");
							span += attrSizes[i] + 2;
							break;
						case AttrType.attrReal:
						case AttrType.attrNull:
						case AttrType.attrSymbol:
							break;
						}
					} finally {

					}
				}
				System.out.println();
				tempcount++;
			}
			System.out.println("pageCount:" + pageCount + "Count:" + tempcount);
			if (HeapFileRunScan.loadNextPage()) {
				pageCount++;
				tempcount = 0;
			} else {
				break;
			}
		}
	}

	/**
	 * This function gets the next page in a Run Array if it exists.
	 * 
	 * @param runPageArray
	 * @param pageID
	 * @return
	 */
	private PageId getNextPageFromRunArrayForGivenPageID(List<List<PageId>> runPageArray, PageId pageID) {
		for (List<PageId> list : runPageArray) {
			if (list.contains(pageID)) {
				int index = list.indexOf(pageID);
				if (index + 1 < list.size()) {
					return list.get(index + 1);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	/**
	 * This function takes the rids of different page, fetch data from the pages,
	 * sort it and return the least element after sorting. Sorting order can be
	 * ascending or descending.
	 * 
	 * @param heapFile
	 * @param currentPageArray
	 * @param tupleMapComparator
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private int getTupleRiDToBeInsertedFromAllPages(Heapfile heapFile, RID[] currentPageArray,
			Comparator tupleMapComparator) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
			HFDiskMgrException, HFBufMgrException, Exception {
		Map<Integer, Tuple> positionTuple = new HashMap<Integer, Tuple>(currentPageArray.length);
		int i = 0;
		for (RID rid : currentPageArray) {
			Tuple ter = null;
			if (rid != null) {
				ter = heapFile.getRecord(rid);
			}
			positionTuple.put(i, ter);
			i++;
		}
		List<Map.Entry<Integer, Tuple>> list = new LinkedList<Map.Entry<Integer, Tuple>>(positionTuple.entrySet());
		Collections.sort(list, tupleMapComparator);
		return list.get(0).getKey();
	}

	/**
	 * This function checks whether all of the rids's are null or not.
	 * 
	 * @param currentPageArray
	 * @return
	 */
	private boolean checkAllPointerAreNotNull(RID[] currentPageArray) {
		for (RID rid : currentPageArray) {
			if (rid != null) {
				return true;
			}
		}
		return false;
	}

	/**
	 * It returns the data page of the given ppid.
	 * 
	 * @param ppid
	 * @return
	 * @throws HFBufMgrException
	 */
	public HFPage getDataPageOfGivenPageId(int ppid) throws HFBufMgrException {
		PageId currentPageId = new PageId(ppid);
		HFPage currentPage = new HFPage();
		pinPage(currentPageId, currentPage, false/* read disk */);
		return currentPage;
	}

	/**
	 * It prints the data for a given tuple for which position is not needed, as it
	 * will already contain the data.
	 * 
	 * @param colIndex
	 * @param attrType
	 * @param temporaryTuple
	 * @throws IOException
	 */
	public void printDataOfGivenTupleWithoutPosition(int colIndex, AttrType attrType, Tuple temporaryTuple)
			throws IOException {
		try {
			switch (attrType.attrType) {
			case AttrType.attrInteger:
				int ival = Convert.getIntValue(0, temporaryTuple.returnTupleByteArray());
				System.out.print(ival + " ");
				break;
			case AttrType.attrString:
				String sval = Convert.getStrValue(0, temporaryTuple.returnTupleByteArray(),
						columnarFile.getAttrSizes()[colIndex]);
				System.out.print(sval + " ");
				break;
			case AttrType.attrReal:
			case AttrType.attrNull:
			case AttrType.attrSymbol:
				break;
			}
		} finally {

		}
	}

	/**
	 * This function creates tuples with all the attributes mentioned in the order
	 * by tuple in the query and adds position of these tuples and returns a single
	 * tuple.
	 * 
	 * @param attrTypes
	 * @param attrSizes
	 * @param outputTuples
	 * @param count
	 * @return
	 * @throws IOException
	 * @throws InvalidTypeException
	 * @throws InvalidTupleSizeException
	 * @throws FieldNumberOutOfBoundException
	 */
	private byte[] createTupleWithPositionForAllGivenTuples(AttrType[] attrTypes, short[] attrSizes,
			Tuple[] outputTuples, int count)
			throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException {
		int recordLength = 0;
		int span = 0;
		for (int j = 0; j < attrTypes.length; j++) {
			if (attrTypes[j].attrType == AttrType.attrInteger) {
				recordLength = recordLength + attrSizes[j];
			} else {
				recordLength = recordLength + attrSizes[j] + 2;
			}
		}
		recordLength = recordLength + 4;
		byte[] result = new byte[recordLength];
		for (int i = 0; i < attrTypes.length; i++) {
			try {
				switch (attrTypes[i].attrType) {
				case AttrType.attrInteger:
					int ival = Convert.getIntValue(0, outputTuples[i].returnTupleByteArray());
					Convert.setIntValue(ival, span, result);
					span += attrSizes[i];
					// System.out.println(ival);
					break;
				case AttrType.attrString:
					String sval = Convert.getStrValue(0, outputTuples[i].returnTupleByteArray(), attrSizes[i]);
					Convert.setStrValue(sval, span, result);
					span += attrSizes[i] + 2;
					// System.out.println(sval);
					break;
				}
			} finally {

			}
		}
		Convert.setIntValue(count, span, result);
		return result;
	}

	private void pinPage(PageId pageno, Page page, boolean emptyPage) throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
		}

	}

	private static void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private static void unpinPageModified(PageId pageno, boolean dirty) throws UnpinPageException {
		while (true) {
			try {
				SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
			} catch (Exception e) {
				break;
			}
		}
	}

}

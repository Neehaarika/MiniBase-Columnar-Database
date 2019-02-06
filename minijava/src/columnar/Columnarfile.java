package columnar;

import bitmap.BitMapFile;
import btree.*;

import global.*;
import heap.*;

import java.util.*;

/**
 * This class represents a columnar file.
 */
public class Columnarfile implements GlobalConst {

	private int numColumns;
	private AttrType[] attrTypes;
	private String[] attrNames;
	private short[] attrSizes;
	private short[] stringSizes;
	private int tupleLength;
	private short[] attrOffsets;
	private boolean _file_deleted;
	private String _fileName;
	private Heapfile[] colHeapfiles;
	private Heapfile hdrHeapfile;
	private BitMapFile markedDeleted;
	private Heapfile deletedTids;
	private BTreeFile[] bTreeFiles;
	private HashMap<String, BitMapFile> bmFiles;
	private Set[] bmValues;
	private byte[] bTreeExist;
	private RID bTreeRID;
	private byte[] bitmapExist;
	private RID bitmapRID;

	/**
	 * Creates a heap file for each of the columns and creates a header file to
	 * maintain meta data.
	 *
	 * @param name       - Columnar file name
	 * @param numColumns - number of columns
	 * @param attrNames  - attribute names
	 * @param attrTypes  - attribute types
	 * @param attrSizes  - attribute sizes
	 * @throws Exception
	 */

	public Columnarfile(String name, int numColumns, String[] attrNames, AttrType[] attrTypes, short[] attrSizes)
			throws Exception {
		_file_deleted = true;

		if (name.length() > MAXFILENAME) {
			throw new Exception("File name too long.");
		}
		if (attrNames.length != numColumns || attrTypes.length != numColumns || attrSizes.length != numColumns) {
			throw new Exception("Columns Meta Info lengths are not equal to num columns.");
		}

		try {
			Heapfile headerFile = new Heapfile(name + ".hdr");
			int headerRecordCount = headerFile.getRecCnt();

			if (headerRecordCount == 0) { // If file is new

				byte[] nameArray = new byte[numColumns * (MAXATTRNAME + 2)];
				int offset = 0;
				for (String attrName : attrNames) {
					if (attrName.length() > MAXATTRNAME) {
						throw new Exception("Attribute name too long.");
					}
					Convert.setStrValue(attrName, offset, nameArray);
					offset += MAXATTRNAME + 2;
				}

				byte[] typeArray = new byte[numColumns * INT_SIZE];
				offset = 0;
				for (AttrType attrType : attrTypes) {
					Convert.setIntValue(attrType.attrType, offset, typeArray);
					offset += INT_SIZE;
				}

				byte[] sizeArray = new byte[numColumns * INT_SIZE];
				offset = 0;
				for (short attrSize : attrSizes) {
					Convert.setIntValue(attrSize, offset, sizeArray);
					offset += INT_SIZE;
				}

				// Insert num of cols
				byte[] numColumnsByteArray = new byte[INT_SIZE];
				Convert.setIntValue(numColumns, 0, numColumnsByteArray);
				headerFile.insertRecord(numColumnsByteArray);
				headerFile.insertRecord(typeArray);
				headerFile.insertRecord(sizeArray);
				headerFile.insertRecord(nameArray);

				bTreeExist = new byte[numColumns];
				headerFile.insertRecord(bTreeExist);

				bitmapExist = new byte[numColumns];
				headerFile.insertRecord(bitmapExist);

				bmValues = new HashSet[numColumns];
				for(int i=0; i<numColumns; i++) {
					bmValues[i] = new HashSet<>();
				}

			} else { // If file already exist. Validate Header information
				Scan scan = headerFile.openScan();
				RID rid = new RID();
				Tuple tuple = scan.getNext(rid);
				int numColFound = Convert.getIntValue(0, tuple.returnTupleByteArray());
				if (numColFound != numColumns) {
					throw new Exception("Existing file has diff num of cols");
				}

				tuple = scan.getNext(rid);
				int offset = 0;
				for (AttrType attrType : attrTypes) {
					int typeFound = Convert.getIntValue(offset, tuple.returnTupleByteArray());
					if (typeFound != attrType.attrType) {
						throw new Exception("Unmatched Type");
					}
					offset += INT_SIZE;
				}

				tuple = scan.getNext(rid);
				offset = 0;
				for (short attrSize : attrSizes) {
					int sizeFound = Convert.getIntValue(offset, tuple.returnTupleByteArray());
					if (sizeFound != attrSize) {
						throw new Exception("Unmatched Size");
					}
					offset += INT_SIZE;
				}

				tuple = scan.getNext(rid);
				offset = 0;
				for (String attrName : attrNames) {
					String nameFound = Convert.getStrValue(offset, tuple.returnTupleByteArray(), MAXATTRNAME + 2);
					if (!nameFound.equals(attrName)) {
						throw new Exception("Unmatched Name");
					}
					offset += MAXATTRNAME + 2;
				}

				tuple = scan.getNext(rid);
				bTreeExist = tuple.getTupleByteArray();
				bTreeRID = new RID();
				bTreeRID.copyRid(rid);

				tuple = scan.getNext(rid);
				bitmapExist = tuple.getTupleByteArray();
				bitmapRID = new RID();
				bitmapRID.copyRid(rid);

				bmValues = new HashSet[numColumns];
				for(int i=0; i<numColumns; i++) {
					if (attrTypes[i].attrType == AttrType.attrString) {
						bmValues[i] = new HashSet<String>();
					} else {
						bmValues[i] = new HashSet<Integer>();
					}
				}
				while ((tuple = scan.getNext(rid)) != null) {
					String[] data = Convert.getStrValue(0, tuple.getTupleByteArray(), tuple.getLength()).split("\\.");
					int colIndex = Integer.parseInt(data[0]);
					if (attrTypes[colIndex].attrType == AttrType.attrString) {
						bmValues[colIndex].add(data[1]);
					} else {
						int value = Integer.parseInt(data[1]);
						bmValues[colIndex].add(value);
					}
				}

				scan.closescan();
			}
			hdrHeapfile = headerFile;
		} catch (Exception e) {
			throw e;
		}

		try {
			_fileName = name;
			this.numColumns = numColumns;
			this.attrTypes = attrTypes;
			this.attrNames = attrNames;
			this.attrSizes = attrSizes;
			this.attrOffsets = new short[numColumns];
			this.tupleLength = 0;
			short offset = 0;
			int stringColCount = 0;
			colHeapfiles = new Heapfile[numColumns];
			for (int i = 0; i < numColumns; i++) {
				this.attrOffsets[i] = offset;

				if (attrTypes[i].attrType == AttrType.attrString) {
					colHeapfiles[i] = new Heapfile(name + "." + i, attrSizes[i] + 2);
					stringColCount++;
					tupleLength += attrSizes[i] + 2;
					offset += attrSizes[i] + 2;
				} else {
					colHeapfiles[i] = new Heapfile(name + "." + i, attrSizes[i]);
					tupleLength += attrSizes[i];
					offset += attrSizes[i];
				}
			}
			this.markedDeleted = new BitMapFile(name + ".md", true);
			this.deletedTids = new Heapfile(name + ".dtid");

			int i = 0;
			int j = 0;
			this.stringSizes = new short[stringColCount];
			for (AttrType attrType : attrTypes) {
				if (attrTypes[i].attrType == AttrType.attrString) {
					this.stringSizes[j] = this.attrSizes[i];
					j++;
				}
				i++;
			}

			// Will use them for lazy lookup only
			bTreeFiles = new BTreeFile[numColumns];
			bmFiles = new HashMap<>();
		} catch (Exception e) {
			throw e;
		}

		_file_deleted = false;
	}

	/**
	 * Creates a columnar file
	 *
	 * @param name - columnar file name
	 * @throws Exception
	 */
	public Columnarfile(String name) throws Exception {
		_file_deleted = true;

		if (name.length() > MAXFILENAME) {
			throw new Exception("Columnar File name too long.");
		}

		try {
			_fileName = name;
			int stringColCount = 0;
			Heapfile headerFile = new Heapfile(name + ".hdr");
			int headerRecordCount = headerFile.getRecCnt();

			if (headerRecordCount == 0) { // If file is new
				throw new Exception("Columnar File does not exist.");
			} else { // If file already exist. Validate Header information
				Scan scan = headerFile.openScan();
				RID rid = new RID();
				Tuple tuple = scan.getNext(rid);
				this.numColumns = Convert.getIntValue(0, tuple.returnTupleByteArray());

				tuple = scan.getNext(rid);
				this.attrTypes = new AttrType[this.numColumns];
				int offset = 0;
				for (int i = 0; i < this.numColumns; i++) {
					int typeFound = Convert.getIntValue(offset, tuple.returnTupleByteArray());
					this.attrTypes[i] = new AttrType(typeFound);
					offset += INT_SIZE;
					if (typeFound == AttrType.attrString)
						stringColCount++;
				}
				tuple = scan.getNext(rid);
				this.attrSizes = new short[this.numColumns];
				this.attrOffsets = new short[this.numColumns];
				this.tupleLength = 0;
				offset = 0;
				for (int i = 0; i < this.numColumns; i++) {
					int sizeFound = Convert.getIntValue(offset, tuple.returnTupleByteArray());
					this.attrSizes[i] = (short) sizeFound;
					this.attrOffsets[i] = (short) this.tupleLength;
					if (attrTypes[i].attrType == AttrType.attrString) {
						this.tupleLength += this.attrSizes[i] + 2;
					} else {
						this.tupleLength += this.attrSizes[i];
					}
					offset += INT_SIZE;
				}

				tuple = scan.getNext(rid);
				this.attrNames = new String[this.numColumns];
				offset = 0;
				for (int i = 0; i < this.numColumns; i++) {
					String nameFound = Convert.getStrValue(offset, tuple.returnTupleByteArray(), MAXATTRNAME + 2);
					this.attrNames[i] = nameFound;
					offset += MAXATTRNAME + 2;
				}

				tuple = scan.getNext(rid);
				bTreeExist = tuple.getTupleByteArray();
				bTreeRID = new RID();
				bTreeRID.copyRid(rid);

				tuple = scan.getNext(rid);
				bitmapExist = tuple.getTupleByteArray();
				bitmapRID = new RID();
				bitmapRID.copyRid(rid);

				bmValues = new HashSet[numColumns];
				for(int i=0; i<numColumns; i++) {
					if (attrTypes[i].attrType == AttrType.attrString) {
						bmValues[i] = new HashSet<String>();
					} else {
						bmValues[i] = new HashSet<Integer>();
					}
				}
				while ((tuple = scan.getNext(rid)) != null) {
					String[] data = Convert.getStrValue(0, tuple.getTupleByteArray(), tuple.getLength()).split("\\.");
					int colIndex = Integer.parseInt(data[0]);
					if (attrTypes[colIndex].attrType == AttrType.attrString) {
						bmValues[colIndex].add(data[1]);
					} else {
						int value = Integer.parseInt(data[1]);
						bmValues[colIndex].add(value);
					}
				}

				scan.closescan();
			}
			hdrHeapfile = headerFile;

			this.colHeapfiles = new Heapfile[this.numColumns];
			for (int i = 0; i < this.numColumns; i++) {
				if (attrTypes[i].attrType == AttrType.attrString) {
					colHeapfiles[i] = new Heapfile(name + "." + i, attrSizes[i]+2);
				} else {
					colHeapfiles[i] = new Heapfile(name + "." + i, attrSizes[i]);
				}

			}
			this.markedDeleted = new BitMapFile(name + ".md");
			this.deletedTids = new Heapfile(name + ".dtid");

			int i = 0;
			int j = 0;
			this.stringSizes = new short[stringColCount];
			for (AttrType attrType : attrTypes) {
				if (attrTypes[i].attrType == AttrType.attrString) {
					this.stringSizes[j] = this.attrSizes[i];
					j++;
				}
				i++;
			}

			bTreeFiles = new BTreeFile[numColumns];
			bmFiles = new HashMap<>();
		} catch (Exception e) {
			throw e;
		}

		_file_deleted = false;
	}

	/**
	 * Deletes columnar file
	 *
	 * @throws Exception
	 */
	public void deleteColumnarFile() throws Exception {
		try {
			if (!_file_deleted) {
				for(int i=0; i<numColumns; i++) {
					Set uniqueValues = bmValues[i];
					for (Object name : uniqueValues) {
						BitMapFile bm = new BitMapFile(_fileName + ".bm." + i + "." + name);
						bm.destroyBitMapFile();
					}
				}

				int i = 0;
				for (byte exist : bTreeExist) {
					if (exist == 1) {
						BTreeFile file = new BTreeFile(_fileName + ".btree." + i);
						file.destroyFile();
					}
					i++;
				}
				for (Heapfile heapfile : this.colHeapfiles) {
					heapfile.deleteFile();
				}
				this.markedDeleted.destroyBitMapFile();
				this.deletedTids.deleteFile();
				this.hdrHeapfile.deleteFile();
			}
		} catch (Exception e) {
			throw e;
		}
		_file_deleted = true;
	}

	/**
	 * Inserts the tuple
	 *
	 * @param tuplePtr - byte array which holds the data to be inserted
	 * @return Tid of the tuple inserted
	 * @throws Exception
	 */
	public TID insertTuple(byte[] tuplePtr) throws Exception {
		try {
			//TODO convert to bitmap if possible
			int position = -1;
			Set<Integer> calPositions = new HashSet<>();
			RID[] recordIDs = new RID[this.numColumns];
			for (int i = 0; i < this.numColumns; i++) {
				if (attrTypes[i].attrType == AttrType.attrString) {
					byte[] recPtr = new byte[this.attrSizes[i] + 2];
					System.arraycopy(tuplePtr, this.attrOffsets[i], recPtr, 0, this.attrSizes[i] + 2);
					String value = Convert.getStrValue(0, recPtr, recPtr.length);
					RID rid = this.colHeapfiles[i].insertRecord(recPtr);
					position = this.colHeapfiles[i].findPosition(rid);
					calPositions.add(position);
					recordIDs[i] = rid;
					if (bTreeExist[i] == 1) { //Add entry to btree if exist for this column
						BTreeFile file;
						if (bTreeFiles[i] != null) {
							file = bTreeFiles[i];
						} else {
							file = new BTreeFile(_fileName + ".btree." + i);
							bTreeFiles[i] = file;
						}
						file.insert(new StringKey(value), rid);
					}
					String bmName = i + "." + value;
					if (bmValues[i].contains(value)) { //Add entry to bitmap if exist for this column and value
						BitMapFile file;
						if (bmFiles.containsKey(bmName)) {
							file = bmFiles.get(bmName);
						} else {
							file = new BitMapFile(_fileName + ".bm." + bmName);
							bmFiles.put(bmName, file);
						}
						file.insert(position);
					} else {
						if (bitmapExist[i] == 1) { //Create new bitmap if flag is true for this column and does not exist for value
							createBitMapIndexWithoutScan(i, new StringValue(value), position);
						}
					}
				} else {
					byte[] recPtr = new byte[this.attrSizes[i]];
					System.arraycopy(tuplePtr, this.attrOffsets[i], recPtr, 0, this.attrSizes[i]);
					int value = Convert.getIntValue(0, recPtr);
					RID rid = this.colHeapfiles[i].insertRecord(recPtr);
					position = this.colHeapfiles[i].findPosition(rid);
					calPositions.add(position);
					recordIDs[i] = rid;
					if (bTreeExist[i] == 1) {
						BTreeFile file;
						if (bTreeFiles[i] != null) {
							file = bTreeFiles[i];
						} else {
							file = new BTreeFile(_fileName + ".btree." + i);
							bTreeFiles[i] = file;
						}
						file.insert(new IntegerKey(value), rid);
					}
					String bmName = i + "." + value;
					if (bmValues[i].contains(value)) { //Add entry to bitmap if exist for this column and value
						BitMapFile file;
						if (bmFiles.containsKey(bmName)) {
							file = bmFiles.get(bmName);
						} else {
							file = new BitMapFile(_fileName + ".bm." + bmName);
							bmFiles.put(bmName, file);
						}
						file.insert(position);
					} else {
						if (bitmapExist[i] == 1) { //Create new bitmap if flag is true for this column and does not exist for value
							createBitMapIndexWithoutScan(i, new IntegerValue(value), position);
						}
					}
				}
			}
			if(calPositions.size()>1) {
				throw new Exception("Invalid position calculations"+calPositions);
			}
			TID tid = new TID(this.numColumns, position, recordIDs);
			return tid;
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * Gets the tuple
	 *
	 * @param tid - Tuple's tid which has to be retrieved
	 * @return tuple - tuple having the specified tid
	 * @throws Exception
	 */
	public Tuple getTuple(TID tid) throws Exception {
		try {
			//TODO check if tid is valid or what parameters are missing.
			int position = tid.position;
			if (position < 0) {
				RID rid;
				if (markedDeleted.isSet(position)) {
					throw new Exception("Tuple deleted.");
				}
				byte data[] = new byte[tupleLength];
				for (int i = 0; i < numColumns; i++) {
					rid = this.colHeapfiles[i].findRID(position);
					Tuple ridTuple = this.colHeapfiles[i].getRecord(rid);
					if (attrTypes[i].attrType == AttrType.attrString) {
						System.arraycopy(ridTuple.getTupleByteArray(), 0, data, this.attrOffsets[i], this.attrSizes[i] + 2);
					} else {
						System.arraycopy(ridTuple.getTupleByteArray(), 0, data, this.attrOffsets[i], this.attrSizes[i]);
					}
				}
				Tuple tuple = new Tuple(data, 0, data.length);
				return tuple;
			} else {
				throw new Exception("Invalid position");
			}
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * Gets value of the tuple
	 *
	 * @param tid
	 * @param column
	 * @return
	 * @throws Exception
	 */
	public ValueClass getValue(TID tid, int column) throws Exception {
		ValueClass value = null;
		IntegerValue intVal = null;
		StringValue strVal = null;
		try {
			byte[] byteData = this.colHeapfiles[column].getRecord(tid.recordIDs[column]).returnTupleByteArray();

			if (attrTypes[column].attrType == AttrType.attrInteger) {

				intVal.setValue(Convert.getIntValue(0, byteData));
				value = intVal;
			} else if (attrTypes[column].attrType == AttrType.attrString) {

				strVal.setValue(Convert.getStrValue(0, byteData, byteData.length));
				value = strVal;
			}

		} catch (Exception e) {
			throw e;
		}

		return value;
	}

	/**
	 * Gets number of tuples
	 *
	 * @return
	 * @throws Exception
	 */
	public int getTupleCnt() throws Exception {
		return (colHeapfiles[0].getRecCnt() - deletedTids.getRecCnt());
	}

	/**
	 * Prints bitset of the deleted tuples
	 *
	 * @throws Exception
	 */
	public void printDeleteBitset() throws Exception {
		System.out.println(markedDeleted.getBitSet());
	}

	/**
	 * Opens tuple scan
	 *
	 * @return
	 * @throws Exception
	 */
	public TupleScan openTupleScan() throws Exception {
		return new TupleScan(this);
	}

	/**
	 * Opens column scan
	 *
	 * @param columnNo
	 * @return
	 * @throws Exception
	 */
	public Scan openColumnScan(int columnNo) throws Exception {
		return new ColumnScan(this, columnNo);
	}

	/**
	 * Updates tuple with the new tuple
	 *
	 * @param tid
	 * @param newtuple
	 * @return
	 * @throws Exception
	 */
	public boolean updateTuple(TID tid, Tuple newtuple) throws Exception {
		try {
			boolean recordUpdated = true;
			RID[] recordIDs = tid.recordIDs;
			for (int i = 0; i < numColumns; i++) {
				if (attrTypes[i].attrType == AttrType.attrString) {
					byte[] recPtr = new byte[this.attrSizes[i] + 2];
					System.arraycopy(newtuple.returnTupleByteArray(), this.attrOffsets[i], recPtr, 0, this.attrSizes[i] + 2);
					recordUpdated = this.colHeapfiles[i].updateRecord(recordIDs[i], new Tuple(recPtr, 0, recPtr.length));
				} else {
					byte[] recPtr = new byte[this.attrSizes[i]];
					System.arraycopy(newtuple.returnTupleByteArray(), this.attrOffsets[i], recPtr, 0, this.attrSizes[i] + 2);
					recordUpdated = this.colHeapfiles[i].updateRecord(recordIDs[i], new Tuple(recPtr, 0, recPtr.length));
				}
				//TODO update entry to index file as well if exist
			}
			return recordUpdated;

		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * Updates a specific column of the tuple
	 *
	 * @param tid
	 * @param newtuple
	 * @param column
	 * @return
	 * @throws Exception
	 */
	public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) throws Exception {
		try {
			RID[] recordIDs = tid.recordIDs;
			boolean columnUpdated = false;
			if (attrTypes[column].attrType == AttrType.attrString) {
				byte[] recPtr = new byte[this.attrSizes[column] + 2];
				System.arraycopy(newtuple.returnTupleByteArray(), this.attrOffsets[column], recPtr, 0, this.attrSizes[column] + 2);
				columnUpdated = this.colHeapfiles[column].updateRecord(recordIDs[column], new Tuple(recPtr, 0, recPtr.length));
			} else {
				byte[] recPtr = new byte[this.attrSizes[column]];
				System.arraycopy(newtuple.returnTupleByteArray(), this.attrOffsets[column], recPtr, 0, this.attrSizes[column] + 2);
				columnUpdated = this.colHeapfiles[column].updateRecord(recordIDs[column], new Tuple(recPtr, 0, recPtr.length));
			}
			//TODO update entry to index file as well if exist
			return columnUpdated;
		} catch (Exception e) {
			throw e;
		}
	}


	public boolean createBTreeIndex(int column) throws Exception {
		if (bTreeFiles[column] == null) {
			BTreeFile bTreeFile;
			String treefilename = _fileName + ".btree." + column;
			if (SystemDefs.JavabaseDB.get_file_entry(treefilename) != null) {
				bTreeFile = new BTreeFile(treefilename);
			} else {
				bTreeFile = new BTreeFile(treefilename, attrTypes[column].attrType, attrSizes[column], DeleteFashion.NAIVE_DELETE);
				//TODO scan column and insert data which is not deleted.
				Scan scan = openColumnScan(column);
				RID rid = new RID();
				Tuple tuple;
				while ((tuple = scan.getNext(rid)) != null) {
					//TODO Pending delete check
					KeyClass keyClass;
					if (attrTypes[column].attrType == AttrType.attrInteger) {
						keyClass = new IntegerKey(Convert.getIntValue(0, tuple.returnTupleByteArray()));
					} else {
						keyClass = new StringKey(Convert.getStrValue(0, tuple.returnTupleByteArray(), attrSizes[column]));
					}
					bTreeFile.insert(keyClass, rid);
					rid = new RID();
				}

			}
			bTreeFiles[column] = bTreeFile;
			bTreeExist[column] = 1;
			hdrHeapfile.updateRecord(bTreeRID, new Tuple(bTreeExist, 0, numColumns));
		}
		return true;
	}

	/**
	 * Creates a bit map index on the column specified by column no.
	 *
	 * @param columnNo
	 * @return
	 * @throws Exception
	 */
	public boolean createBitMapIndex(int columnNo) throws Exception {
		if (bitmapExist[columnNo] != 1) {
			Scan scan = openColumnScan(columnNo);
			if (attrTypes[columnNo].attrType == AttrType.attrInteger) {
				Map<Integer, BitMapFile> map = new HashMap<>();
				Tuple tuple;
				RID rid = new RID();
				while ((tuple = scan.getNext(rid)) != null) {
					int value = Convert.getIntValue(0, tuple.returnTupleByteArray());
					if(!map.containsKey(value)) {
						String name = columnNo + "." + value;
						String filename = _fileName + ".bm." + name;
						BitMapFile file = new BitMapFile(filename, this);
						map.put(value, file);
						byte[] recptr = new byte[name.length() + 2];
						Convert.setStrValue(name, 0, recptr);
						hdrHeapfile.insertRecord(recptr);
						bmFiles.put(name, file);
						bmValues[columnNo].add(value);
					}
					map.get(value).set(colHeapfiles[columnNo].findPosition(rid));
				}
				scan.closescan();
				for (int value : map.keySet()) {
					map.get(value).saveToDisk();
				}
			} else {
				Map<String, BitMapFile> map = new HashMap<>();
				Tuple tuple;
				RID rid = new RID();
				while ((tuple = scan.getNext(rid)) != null) {
					String value = Convert.getStrValue(0, tuple.returnTupleByteArray(), tuple.returnTupleByteArray().length);
					if(!map.containsKey(value)) {
						String name = columnNo + "." + value;
						String filename = _fileName + ".bm." + name;
						BitMapFile file = new BitMapFile(filename, this);
						map.put(value, file);
						byte[] recptr = new byte[name.length() + 2];
						Convert.setStrValue(name, 0, recptr);
						hdrHeapfile.insertRecord(recptr);
						bmFiles.put(name, file);
						bmValues[columnNo].add(value);
					}
					map.get(value).set(colHeapfiles[columnNo].findPosition(rid));
				}
				scan.closescan();
				for (String value : map.keySet()) {
					map.get(value).saveToDisk();
				}
			}
			//Updated header files
			bitmapExist[columnNo] = 1;
			hdrHeapfile.updateRecord(bitmapRID, new Tuple(bitmapExist, 0, numColumns));
		}
		return true;
	}

	/**
	 * Creates a bit map index on specified column and value
	 *
	 * @param columnNo
	 * @param value
	 * @return
	 * @throws Exception
	 */
	boolean createBitMapIndexWithoutScan(int columnNo, ValueClass value, int position) throws Exception {
		String name = columnNo + "." + value;
		String filename = _fileName + ".bm." + name;
		BitMapFile bitMapFile = new BitMapFile(filename, this);
		bitMapFile.set(position);
		bitMapFile.saveToDisk();
		byte[] recptr = new byte[name.length() + 2];
		Convert.setStrValue(name, 0, recptr);
		hdrHeapfile.insertRecord(recptr);
		bmFiles.put(name, bitMapFile);
		if (attrTypes[columnNo].attrType == AttrType.attrInteger) {
			bmValues[columnNo].add(((IntegerValue)value).getValue());
		} else {
			bmValues[columnNo].add(value.toString());
		}
		return true;
	}

	/**
	 * Creates a bit map index on specified column and value
	 *
	 * @param columnNo
	 * @param value
	 * @return
	 * @throws Exception
	 */
	boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception {
		String name = columnNo + "." + value;
		String filename = _fileName + ".bm." + name;
		BitMapFile bitMapFile = new BitMapFile(filename, this, columnNo, value);
		byte[] recptr = new byte[name.length() + 2];
		Convert.setStrValue(name, 0, recptr);
		hdrHeapfile.insertRecord(recptr);
		bmFiles.put(name, bitMapFile);
		if (attrTypes[columnNo].attrType == AttrType.attrInteger) {
			bmValues[columnNo].add(((IntegerValue)value).getValue());
		} else {
			bmValues[columnNo].add(value.toString());
		}
		return true;
	}

	/**
	 * Mark tuple as deleted
	 *
	 * @param tid
	 * @return
	 * @throws Exception
	 */
	public boolean markTupleDeleted(TID tid) throws Exception {
		int position = tid.position;
		System.out.println("Delete Position: " + (position + 1));
		if (position > -1) {
			RID rid;
			byte[] tidArr = new byte[numColumns * 8];
			int offset = 0;
			for (int i = 0; i < numColumns; i++) {
				rid = this.colHeapfiles[i].findRID(position);
				rid.writeToByteArray(tidArr, offset);
				offset += 8;
			}
			markedDeleted.insert(position);
			deletedTids.insertRecord(tidArr);
		} else {
			throw new Exception("Invalid position");
		}
		return true;
	}

	/**
	 * Delete all tuples which are marked as deleted
	 *
	 * @return
	 */
	public boolean purgeAllDeletedTuples() {
		try {
			Tuple tuple;
			RID rid = new RID();
			Scan scan = deletedTids.openScan();
			int counter = 0;
			while ((tuple = scan.getNext(rid)) != null) {
				byte[] tidArr = tuple.returnTupleByteArray();
				int offset = 0;
				for (int i = 0; i < numColumns; i++) {
					rid = new RID(new PageId(Convert.getIntValue(offset + 4, tidArr)), Convert.getIntValue(offset, tidArr));
					if (bTreeExist[i] == 1) {
						Tuple tuple1 = this.colHeapfiles[i].getRecord(rid);
						byte[] a = tuple1.returnTupleByteArray();
						if (attrTypes[i].attrType == AttrType.attrString) {
							String value = Convert.getStrValue(0, a, a.length);

							BTreeFile file;
							if (bTreeFiles[i] != null) {
								file = bTreeFiles[i];
							} else {
								file = new BTreeFile(_fileName + ".btree." + i);
								bTreeFiles[i] = file;
							}
							file.Delete(new StringKey(value), rid);
						} else {
							int value = Convert.getIntValue(0, a);
							BTreeFile file;
							if (bTreeFiles[i] != null) {
								file = bTreeFiles[i];
							} else {
								file = new BTreeFile(_fileName + ".btree." + i);
								bTreeFiles[i] = file;
							}
							file.Delete(new IntegerKey(value), rid);
						}
					}
					this.colHeapfiles[i].deleteRecord(rid);
					offset += 8;
				}
			}
			scan.closescan();

			List<Integer> list = new ArrayList<>();
			int pointer = 0;
			int pos = -1;
			while ((pos=markedDeleted.getBitSet().nextSetBit(pointer))!=-1) {
				list.add(pos);
				pointer = pos+1;
			}

			//Clearing positions from all bitmaps
			for (int i = 0; i < numColumns; i++) {
				List<Integer> rangeList = new ArrayList<>();
				List<Integer> delDirOffsets = colHeapfiles[i].getDeletedDirOffsets();
				int totalPositionsInDir = colHeapfiles[i].getNumRecordsPerDirPage()*colHeapfiles[i].getNumRecordsPerDataPage();
				Collections.sort(delDirOffsets);
				for(int offset: delDirOffsets) {
					rangeList.add(offset*totalPositionsInDir);
					rangeList.add((offset+1)*totalPositionsInDir);
				}
				for (Object value : bmValues[i]) {
					String bmName = i + "." + value;
					BitMapFile bmfile;
					if (bmFiles.containsKey(bmName)) {
						bmfile = bmFiles.get(bmName);
					} else {
						bmfile = new BitMapFile(_fileName + ".bm." + bmName);
						bmFiles.put(bmName, bmfile);
					}
					bmfile.purgeDelete(list, rangeList);
				}
				colHeapfiles[i].resetDeletedDirOffsetList();
				colHeapfiles[i].invalidatePositionBuffer();
			}

			for(int i: list) {
				markedDeleted.delete(i);
			}
			deletedTids.deleteFile();
			deletedTids = new Heapfile(_fileName + ".dtid");
			//TODO recreate or deleteAtPositions
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * To get a heap file from column name
	 *
	 * @param columnHeapFileName
	 * @return
	 */
	public Heapfile getHeapfile(String columnHeapFileName) {
		return null;
	}

	/**
	 * Returns all the heap files
	 *
	 * @return
	 */
	public Heapfile[] getHeapfiles() {
		return colHeapfiles;
	}

	/**
	 * Returns the attribute type by column name
	 *
	 * @param columnName
	 * @return
	 */
	public AttrType getAttributeTypeByColunnName(String columnName) {
		return null;
	}

	public AttrType getAttributeType(int columnNo) {
		return attrTypes[columnNo];
	}

	/**
	 * Returns all the attributes of the columnar file
	 *
	 * @return
	 */
	public AttrType[] getAttributeTypes() {
		return attrTypes;
	}

	/**
	 * Returns the length of the tuple
	 *
	 * @return
	 */
	public int getTupleLength() {
		return this.tupleLength;
	}

	/**
	 * Returns the field count
	 *
	 * @return
	 */
	public short getFieldCount() {
		return (short) this.numColumns;
	}

	/**
	 * Returns the string sizes
	 *
	 * @return
	 */
	public short[] getStringSizes() {
		return stringSizes;
	}


	/**
	 * Returns attribute sizes
	 *
	 * @return
	 */
	public short[] getAttrSizes() {
		return attrSizes;
	}

	/**
	 * Returns the attribute offsets array
	 *
	 * @return
	 */
	public short[] getAttrOffsets() {
		return attrOffsets;
	}

	/**
	 * Returns the attribute names
	 *
	 * @return
	 */
	public String[] getAttrNames() {
		return attrNames;
	}

	/**
	 * Returns the marked deleted heap file
	 *
	 * @return
	 */
	public BitMapFile getMarkedDeleted() {
		return markedDeleted;
	}


	/**
	 * Returns index of the column
	 *
	 * @param name
	 * @return
	 * @throws Exception
	 */
	public int colNameToIndex(String name) throws Exception {
		int index = 0;
		for (String colName : attrNames) {
			if (colName.equals(name)) {
				return index;
			}
			index++;
		}
		throw new Exception("Column Name '" + name + "' Invalid.");
	}

	/**
	 * Returns column name to index
	 *
	 * @param index
	 * @return
	 * @throws Exception
	 */
	public String indexToColName(int index) throws Exception {
		if (index < attrNames.length) {
			return attrNames[index];
		}
		throw new Exception("Column Index '" + index + "' out of bound.");
	}
	/**
	 * Returns AttrType of Column Index
	 *
	 * @param index
	 * @return
	 * @throws Exception
	 */
	public AttrType indexToColAttrType(int index) throws Exception {
		if (index < attrTypes.length) {
			return attrTypes[index];
		}
		throw new Exception("Column Index '" + index + "' out of bound.");
	}

	/**
	 * Checks whether btree index exists or not
	 *
	 * @param colNo
	 * @return
	 */
	public boolean btreeIndexExists(int colNo) {
		if (bTreeExist[colNo] == 1) {
			return true;
		}
		return false;
	}

	/**
	 * Check whether bit map index exists or not
	 *
	 * @param colNo
	 * @return
	 */
	public boolean bitmapIndexExists(int colNo) {
		if (bitmapExist[colNo] == 1) {
			return true;
		}
		return false;
	}

	public BitMapFile getBitmapIndex(int columnNo, ValueClass value) throws Exception {
		String name = columnNo + "." + value;
		if (bmFiles.containsKey(name)) {
			return bmFiles.get(name);
		} else {
			boolean valueExist = false;
			if(attrTypes[columnNo].attrType==AttrType.attrString) {
				if(bmValues[columnNo].contains(((StringValue)value).getValue())) {
					valueExist = true;
				}
			} else {
				if(bmValues[columnNo].contains(((IntegerValue)value).getValue())) {
					valueExist = true;
				}
			}
			if(valueExist) {
				String filename = _fileName + ".bm." + name;
				BitMapFile file = new BitMapFile(filename);
				bmFiles.put(name, file);
				return file;
			} else {
				return new BitMapFile();
			}
		}
	}

	public BTreeFile getBtreeIndex(int columnNo) throws Exception {
		if (bTreeFiles[columnNo] != null) {
			return bTreeFiles[columnNo];
		} else {
			bTreeFiles[columnNo] = new BTreeFile(_fileName + ".btree." + columnNo);
			return bTreeFiles[columnNo];
		}
	}

	public Set getBitmapValues(int columnNo) {
		return bmValues[columnNo];
	}

	/**
	 * Gets the file name
	 *
	 * @return
	 */
	public String get_fileName() {
		return _fileName;
	}
}
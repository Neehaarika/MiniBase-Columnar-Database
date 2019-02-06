package bitmap;

/**
 * Created by Chandana on 3/15/18.
 */

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import chainexception.ChainException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.*;

public class BitMapFile implements GlobalConst {

	private BMIndexPage headerPage;
	private PageId headerPageId;
	private String dbname;
	private Columnarfile columnarFile;
	private int columnNo;
	private int columnSize;
	private BitSet bitSet;
	private AttrType keyType;
	private HashMap<Integer, Integer> offsetToPage;

	private final static int MAGIC0 = 1989;
	private final static int RECORD_SIZE = MINIBASE_PAGESIZE - BMIndexPage.DPFIXED - BMIndexPage.SIZE_OF_SLOT;

	public BitMapFile() throws IOException, ChainException {
		bitSet = new BitSet();
	}
	/*
	 * This constructor is to open a BitMapFile with a given filename which is
	 * already created and whose entry is already made in the db.
	 */
	public BitMapFile(String filename) throws IOException, ChainException {
		if (filename.isEmpty()) {
			throw new IllegalArgumentException("File name passed is null. Provide a valid filename.");
		}

		headerPageId = get_file_entry(filename);
//		System.out.println("HeaderPAge ID 1: "+headerPageId);
		if (headerPageId == null) {
			throw new IllegalStateException(
					"The file " + filename + " does not exist. Please provide a vaild BitMap file name.");
		}
		headerPage = new BMIndexPage(headerPageId);
		dbname = new String(filename);
//		System.out.println("HeaderPAge ID"+(headerPage.getCurPage()));
//		System.out.println("HeaderPAge next ID"+(headerPage.getNextPage()));
		offsetToPage = new HashMap<>();
		bitSet = BM.readBitSet(headerPage, offsetToPage);
	}

	public BitMapFile(String filename, boolean create) throws IOException, ChainException {
		if (filename.isEmpty()) {
			throw new IllegalArgumentException("File name passed is null. Provide a valid filename.");
		}

		headerPageId = get_file_entry(filename);
//		System.out.println("HeaderPAge ID 1: "+headerPageId);
		if (headerPageId == null && !create) {
			throw new IllegalStateException(
					"The file " + filename + " does not exist. Please provide a vaild BitMap file name.");
		}
		if(headerPageId!=null) {
			headerPage = new BMIndexPage(headerPageId);
			offsetToPage = new HashMap<>();
			bitSet = BM.readBitSet(headerPage, offsetToPage);
		} else {
			initBitMapHeaderPage();
			add_file_entry(filename, headerPageId);
			//TODO check whether we need to unpin or not
			headerPage.insertRecord(new byte[RECORD_SIZE]);
			unpinPage(headerPageId, true);
			this.bitSet = new BitSet();
			offsetToPage = new HashMap<>();
			offsetToPage.put(0, headerPageId.pid);
		}

		dbname = new String(filename);
//		System.out.println("headerPgae id:"+headerPage.getCurPage());
	}

	/*
	 * 1. This constructor is to create the bitmap file with the give filename, for
	 * a given column in a columnar file, for a given unique value in the column. 2.
	 * A file entry is made in the db for the bitmap file created. 3. This index is
	 * created in the form of BitSet. The bitset for this value is stored in a
	 * BitMapPage and its metadata is maintained in the BitMapHeaderPage.
	 */
	public BitMapFile(String filename, Columnarfile columnfile, int columnNo, ValueClass value) throws Exception {
		if (columnNo < 0 || columnNo >= columnfile.getFieldCount()) {
			throw new IllegalArgumentException("Invalid column number." + columnNo);
		}
		if (value == null) {
			throw new IllegalArgumentException(
					"The column value passed is null. Please provide a valid value for the column "
							+ columnfile.getAttrNames()[columnNo]);
		}

		keyType = (value instanceof IntegerValue) ? new AttrType(AttrType.attrInteger)
				: new AttrType(AttrType.attrString);

		if (keyType.attrType != columnfile.getAttributeTypes()[columnNo].attrType) {
			throw new IllegalArgumentException("The value provided is of type " + keyType.toString()
					+ ". Please provide a value of type " + columnfile.getAttributeTypes()[columnNo]);
		}

		headerPageId = get_file_entry(filename);
		if (headerPageId != null) // file already exists
		{
			throw new IllegalArgumentException("The BitMapFile " + filename + " is already created.");
		}
		initBitMapHeaderPage();
		add_file_entry(filename, headerPageId);

		dbname = new String(filename);
		this.columnarFile = columnfile;
		this.columnNo = columnNo;

		Object keyValue = (value instanceof IntegerValue) ? ((IntegerValue) value).getValue()
				: ((StringValue) value).getValue();
		this.bitSet = new BitSet();

		getBitSetForValue(columnNo, keyType, keyValue);
		System.out.println("keyValueindexBits" + this.bitSet);
		offsetToPage = new HashMap<>();
		RID rid = BM.insertBitSet(this.headerPage, this.bitSet, offsetToPage);

		if (rid == null) {
			throw new IndexInsertRecException("Exception while inserting bitset into BM Index Page.");
		}
		//headerPage.setNextPage(rid.pageNo);
//		System.out.println("headerPgae id:"+headerPage.getCurPage());
//		System.out.println("headerPgae next id:"+rid.pageNo);
	}

	public BitMapFile(String filename, Columnarfile columnfile) throws Exception {
		headerPageId = get_file_entry(filename);
		if (headerPageId != null) // file already exists
		{
			throw new IllegalArgumentException("The BitMapFile " + filename + " is already created.");
		}
		initBitMapHeaderPage();
		add_file_entry(filename, headerPageId);

		dbname = new String(filename);
		this.columnarFile = columnfile;
		this.bitSet = new BitSet();
		offsetToPage = new HashMap<>();
		offsetToPage.put(0, headerPageId.pid);
//		System.out.println("headerPgae id:"+headerPage.getCurPage());
	}

	private BitSet getBitSetForValue(Object keyValue, ArrayList<Object> columnValues) {
		System.out.println("columnValues" + columnValues);

		BitSet indexBits = new BitSet(columnValues.size());
		for (int i = 0; i < columnValues.size(); i++) {
			//System.out.println("keyvalue " + keyValue + " clm value " + columnValues.get(i));
			if (keyValue.equals(columnValues.get(i))) {

				indexBits.set(i);
			}
		}
		System.out.println("INdex bits after insert at position: " + indexBits);
		return indexBits;
	}

	 private Page pinPage(PageId pageno)
			    throws PinPageException
			    {
			      try {
			        Page page=new Page();
			        SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			        return page;
			      }
			      catch (Exception e) {
				e.printStackTrace();
				throw new PinPageException(e,"");
			      }
			    }

	private void unpinPage(PageId pageno, boolean dirty)
		    throws UnpinPageException
		    {
		      try{
		        SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		      }
		      catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		      }
		    }

	private void initBitMapHeaderPage() throws ConstructPageException, IOException {
		headerPage = new BMIndexPage();
		headerPageId = headerPage.getCurPage();
		headerPage.setType(NodeType.BMHEAD);
		headerPage.setPrevPage(new PageId(INVALID_PAGE));
		headerPage.setNextPage(new PageId(INVALID_PAGE));
	}

	// Operate ColumnnarScan in order to formulate bitmap index for given value in the column.
	private void getBitSetForValue(int columnNo, AttrType columnAttributeType, Object keyValue)
			throws IOException, FieldNumberOutOfBoundException, InvalidTupleSizeException, Exception {
		Tuple tuple;
		RID rid = new RID();
		Heapfile hf = columnarFile.getHeapfiles()[columnNo];
		Scan columnScan = hf.openScan();
		while ((tuple = columnScan.getNext(rid)) != null) {
			try {
				switch (columnAttributeType.attrType) {
				case AttrType.attrInteger:

					int ival = Convert.getIntValue(0, tuple.returnTupleByteArray());
					if (keyValue.equals(ival)) {
						this.bitSet.set(hf.findPosition(rid));
					}
					break;
				case AttrType.attrString:
					String sval = Convert.getStrValue(0, tuple.returnTupleByteArray(),
							columnarFile.getAttrSizes()[columnNo]);
					if (keyValue.equals(sval)) {
						this.bitSet.set(hf.findPosition(rid));
					}
					break;
				case AttrType.attrReal:
				case AttrType.attrNull:
				case AttrType.attrSymbol:
					break;
				}
			} finally {

			}
		}

	}

	/*
	 * This method marks the value of the column at a given position deleted by
	 * clearing the bit at that position in the bitmap index.
	 */

	public boolean delete(int position) {
		try {
			if(bitSet.get(position)) {
				this.bitSet.clear(position);
				int offset = position/8000;
				if(bitSet.nextSetBit(offset*8000)==-1 && offset!=0) {
					int curOffset = offset;
					while(bitSet.nextSetBit(curOffset*8000)==-1 && curOffset!=0) {
						freePage(new PageId(offsetToPage.get(curOffset)));
						offsetToPage.remove(curOffset);
						curOffset--;
					}
					BMIndexPage prevBitMapPage = new BMIndexPage(new PageId(offsetToPage.get(curOffset)));
					prevBitMapPage.setNextPage(new PageId(INVALID_PAGE));
					unpinPage(prevBitMapPage.getCurPage(), true);
					return true;
				}
				PageId id = new PageId(offsetToPage.get(offset));
				BMIndexPage currentBitMapPage = new BMIndexPage(id);
				RID rid1 = currentBitMapPage.firstRecord();
				Tuple atuple = currentBitMapPage.returnRecord(rid1);
				//TODO Bug here, copy data first to temp array. ArrayOutOfBounds
				byte[] data = bitSet.toByteArray();
				byte[] rec = new byte[1000];
				int start = offset*1000;
				int end = data.length<(offset+1)*1000 ? data.length-start : 1000;
				System.arraycopy(data, start, rec, 0, end);
				Tuple newtuple = new Tuple(rec, 0, 1000);
				atuple.tupleCopy(newtuple);
				unpinPage(id, true);
			}
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
		return true;
	}

	public void set(int position) {
		bitSet.set(position);
	}

	public void clear(int position) {
		bitSet.clear(position);
	}

	public boolean isSet(int position) {
		return bitSet.get(position);
	}

	public boolean isClear(int position) {
		return !bitSet.get(position);
	}

	public void saveToDisk() throws Exception {
		RID rid = BM.insertBitSet(this.headerPage, this.bitSet, offsetToPage);
		if (rid == null) {
			throw new IndexInsertRecException("Exception while inserting bitset into BM Index Page.");
		}
	}

	/*
	 * If the value for which the bitmap file is created is inserted at a given
	 * position, this method sets the bitmap index at that position. Subsequent bits
	 * after the position are shifted. The column size is updated.
	 */
	public boolean insert(int position) throws IndexInsertRecException, IOException {
		try {
			bitSet.set(position);
			byte[] data = bitSet.toByteArray();
			int offset = position/8000;
			if(offsetToPage.containsKey(offset)) {
				PageId id = new PageId(offsetToPage.get(offset));
				BMIndexPage currentBitMapPage = new BMIndexPage(id);
				RID rid1 = currentBitMapPage.firstRecord();
				Tuple atuple = currentBitMapPage.returnRecord(rid1);
				byte[] rec = new byte[1000];
				int start = offset*1000;
				int end = data.length<(offset+1)*1000 ? data.length-start : 1000;
				System.arraycopy(data, start, rec, 0, end);
				Tuple newtuple = new Tuple(rec, 0, 1000);
				atuple.tupleCopy(newtuple);
				unpinPage(id, true);
			} else {
				int prevOffset = offset-1;
				while(!offsetToPage.containsKey(prevOffset)) {
					prevOffset--;
				}
				int startOffset = prevOffset+1;
				int endOffset = offset;
				PageId prevId = new PageId(offsetToPage.get(prevOffset));
				BMIndexPage prevBitMapPage = new BMIndexPage(prevId);
				for(int i=startOffset; i<=endOffset; i++) {
					BMIndexPage currentBitMapPage = new BMIndexPage();
					byte[] rec = new byte[1000];
					int start = i*1000;
					int end = data.length<(i+1)*1000 ? data.length-start : 1000;
					System.arraycopy(data, start, rec, 0, end);
					currentBitMapPage.insertRecord(rec, prevId, new PageId(INVALID_PAGE));
					prevBitMapPage.setNextPage(currentBitMapPage.getCurPage());
					unpinPage(prevId, true);
					prevId = currentBitMapPage.getCurPage();
					prevBitMapPage = currentBitMapPage;
					offsetToPage.put(i, currentBitMapPage.getCurPage().pid);
				}
				unpinPage(prevBitMapPage.getCurPage(), true);
			}
			return true;
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
	}

	public boolean purgeDelete(List<Integer> positions, List<Integer> rangeList) {
		try {
			if(rangeList!=null && rangeList.size()>0) {
				for(int pos: positions) {
					bitSet.clear(pos);
				}
				BitSet tempBitset = new BitSet();
				int size = rangeList.size();
				int start = 0;
				int position = 0;
				for(int i=0; i<size; i=i+2) {
					int end = rangeList.get(i);
					for(int j=start; j<end; j++) {
						tempBitset.set(position, bitSet.get(j));
						position++;
					}
					start = rangeList.get(i+1);
 				}
 				int end = bitSet.length();
				for(int j=start; j<end; j++) {
					tempBitset.set(position, bitSet.get(j));
					position++;
				}
				bitSet = tempBitset;
				BM.updateBitSet(headerPage, bitSet, offsetToPage);
			} else {
				for(int pos: positions) {
					delete(pos);
				}
			}
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
		return true;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private static void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private static void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}
	}

	/*
	 * 1. Destroy the bitmap file. Remove the file entry from the db. 2. Free all
	 * the pages associated with the file. 3. Free the header page.
	 */
	public void destroyBitMapFile() throws ConstructPageException, IOException, UnpinPageException, FreePageException,
			DeleteFileEntryException {
		BMIndexPage nextBitMapPage;

		nextBitMapPage = new BMIndexPage(headerPage.getCurPage());
		while (nextBitMapPage.getNextPage().pid != new PageId(INVALID_PAGE).pid) {
			PageId nextPageId = nextBitMapPage.getNextPage();
			unpinPage(nextBitMapPage.getCurPage());
			freePage(nextBitMapPage.getCurPage());
			nextBitMapPage = new BMIndexPage(nextPageId);
		}
		unpinPage(nextBitMapPage.getCurPage());
		freePage(nextBitMapPage.getCurPage());
		delete_file_entry(dbname);
	}

	// Close the bitmap file by unpinning the bitmap header page.
	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	public BitSet getBitSet() {
		return bitSet;
	}

	public void setBitSet(BitSet bitSet) {
		this.bitSet = bitSet;
	}

	public BitSet getClonedBitSet() {
		return (BitSet) bitSet.clone();
	}
}

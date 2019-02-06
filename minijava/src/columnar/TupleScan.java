package columnar;

import java.io.IOException;
import java.util.*;

import global.*;
import heap.*;

/**
 * 
 * Represents the tuple scan
 *
 */
public class TupleScan {

	private Columnarfile columnarFile;
	private ArrayList<Scan> heapFileScanners;
	private AttrType[] attrTypes;
	private short[] attrSizes;
	private short[] attrOffsets;
	private BitSet markedDeleted;
	private int position;

	/**
	 * Initializes a scan on all the column heap files
	 * @param columnarFile
	 * @throws Exception
	 */
	public TupleScan(Columnarfile columnarFile) throws Exception {

		this.columnarFile = columnarFile;
		this.heapFileScanners = new ArrayList<Scan>();
		this.attrTypes = columnarFile.getAttributeTypes();
		this.attrSizes = columnarFile.getAttrSizes();
		this.attrOffsets = columnarFile.getAttrOffsets();
		this.position = 0;

		/* Initializing tuple scan */
		this.markedDeleted = columnarFile.getMarkedDeleted().getBitSet();
		for (Heapfile columnFile: columnarFile.getHeapfiles()) {
			try {
				heapFileScanners.add(new Scan(columnFile));
			} catch (Exception ex) {
				throw ex;
			}
		}
	}

	/**
	 * Returns the next tuple
	 * @param tid
	 * @return
	 * @throws Exception
	 */
	public Tuple getNext(TID tid) throws Exception {
		while(true) {
			Tuple scannedTuple = new Tuple();
			scannedTuple.setHdr(columnarFile.getFieldCount(), attrTypes,
					columnarFile.getStringSizes());
			int i=0;
			for (Scan scan: this.heapFileScanners) {
				RID rid = new RID();
				Tuple tuple = scan.getNext(rid);
				if(tuple!=null) {
					if (attrTypes[i].attrType == AttrType.attrInteger) {
						int intFieldValue = Convert.getIntValue(0,tuple.returnTupleByteArray());
						scannedTuple.setIntFld(i + 1, intFieldValue);
					} else {
						String stringFieldValue = Convert.getStrValue(0, tuple.returnTupleByteArray(), attrSizes[i]+2);
						scannedTuple.setStrFld(i + 1, stringFieldValue);
					}
					tid.setRID(i, rid);
				} else {
					return null;
				}
				i++;
			}
			int pos = -1;
			try {
				pos = columnarFile.getHeapfiles()[0].findPosition(tid.recordIDs[0]);
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}
			tid.setPosition(pos);
			if(!markedDeleted.get(pos)) {
				return scannedTuple;
			}
		}
	}
    
	/**
	 * Closes the scan on all the heap files
	 */
	public void closetuplescan() {
		for (Scan scanner : heapFileScanners) {
			scanner.closescan();
		}
	}

	/**
	 * Positions the scan cursors to the specified rid 
	 * @param tid
	 * @return
	 * @throws Exception
	 */
	public boolean position(TID tid) throws Exception {
		int column = 0;
		for (Scan scanner : heapFileScanners) {
			scanner.position(tid.recordIDs[column]);
			column++;
		}
		return true;
	}
}

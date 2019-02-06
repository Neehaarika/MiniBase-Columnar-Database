package columnar;

import java.io.IOException;
import java.util.BitSet;

import global.*;
import heap.*;
/**
 * 
 * Represents column scan
 *
 */
public class ColumnScan extends Scan {

	private BitSet markedDeleted;
	private int columnNo;
	private AttrType[] attrTypes;
	private Columnarfile columnarfile;
	private Heapfile columnHeapfile;
	
	/**
	 * Creates a scan object on the column specified by column no. and also initializes scan on marked deleted file
	 * @param file
	 * @param columnNo
	 * @throws Exception
	 */
	public ColumnScan(Columnarfile file, int columnNo) throws Exception {
		super(file.getHeapfiles()[columnNo]);
		try {
			this.columnNo = columnNo;
			columnarfile = file;
			attrTypes = file.getAttributeTypes();
			columnHeapfile = file.getHeapfiles()[columnNo];
			if (columnHeapfile != null) {
				this.markedDeleted = file.getMarkedDeleted().getBitSet();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Returns the next tuple
	 * @param rid
	 * @return
	 * @throws InvalidTupleSizeException
	 * @throws IOException
	 */
	public Tuple getNext(RID rid) throws InvalidTupleSizeException, IOException {
		Tuple tuple = super.getNext(rid);
		while(tuple != null) {
			int pos = -1;
			try {
				pos = columnHeapfile.findPosition(rid);
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}
			if(!markedDeleted.get(pos)) {
				return tuple;
			} else {
				tuple = super.getNext(rid);
			}
		}
		return null;
	}
}

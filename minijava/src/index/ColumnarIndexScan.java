package index;

import columnar.Columnarfile;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.*;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;

/**
 * Columnar Index Scan iterator will directly access the required tuple by 
 * scanning multiple indexes using the provided keys. It will also perform 
 * selections and projections. information about the tuples and the index 
 * are passed to the constructor, then the user calls <code>get_next()</code> 
 * to get the tuples.
 */
public class ColumnarIndexScan extends Iterator {

	public FldSpec[] perm_mat;
	private AttrType[] _types;
	private short[] _s_sizes;
	private CondExpr[] _selects;
	private int _noInFlds;
	private int _noOutFlds;
	private int[] outIndexes;
	private Columnarfile f;
	private Tuple Jtuple;
	private int[] _fldNum;
	private boolean index_only;
	private IndexType[] index;
	private int currentBitMapPos;
	private BitSet outputPositions;
	private HashMap<String,BitSet> duplicateConstraints;
	private String queryStr;

	/**
	 * class constructor. set up the columnar index scan.
	 * 
	 * @param columnarfile
	 *            the input relation
	 * @param fldNum
	 *            field numbers of the indexed fields
	 * @param indexTypes
	 *            types of the index (B_Index, Bitmap)
	 * @param indNames
	 *            names of the input index
	 * @param types
	 *            array of types in this relation
	 * @param str_sizes
	 *            array of string sizes (for attributes that are string)
	 * @param noInFlds
	 *            number of fields in input tuple
	 * @param noOutFlds
	 *            number of fields in output tuple
	 * @param out_indexes
	 * 			  the target column numbers
	 * @param outFlds
	 *            fields to project
	 * @param selects
	 *            conditions to apply, first one is primary
	 * @param indexOnly
	 *            whether the answer requires only the key or the tuple
	 * @exception IndexException
	 *                error from the lower layer
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception InvalidTupleSizeException
	 *                tuple size not valid
	 * @exception UnknownIndexTypeException
	 *                index type unknown
	 * @exception IOException
	 *                from the lower layer
	 */
	public ColumnarIndexScan(
			Columnarfile columnarFile, 
			final int[] fldNums, 
			IndexType[] indexTypes,
			final String[] indNames, 
			AttrType[] types, 
			short[] str_sizes, 
			int noInFlds, 
			int noOutFlds,
			int[] out_indexes, 
			FldSpec[] outFlds, 
			CondExpr[] selects, 
			final boolean indexOnly
			) 
		throws Exception {
		_fldNum = fldNums;
		_noInFlds = noInFlds;
		_types = types;
		_s_sizes = str_sizes;
		outIndexes = out_indexes;
		index = indexTypes;

		AttrType[] Jtypes = new AttrType[noOutFlds];
		short[] ts_sizes;
		Jtuple = new Tuple();

		try {
			ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
		} catch (TupleUtilsException e) {
			throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
		} catch (InvalidRelation e) {
			throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
		}

		_selects = selects;
		perm_mat = outFlds;
		index_only = indexOnly;
		_noOutFlds = noOutFlds;
		try {
			f = columnarFile;
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile not created");
		}
		int i=0;
		CondExpr temp_ptr;
		currentBitMapPos = 0;
		int constrCount=0;
		boolean duplicateFlag=false;
		duplicateConstraints = new HashMap<String,BitSet>();
		String constraint = new String();
		buildInputQueryString();
		while(selects[i]!=null){
			//outer loop to iterate the conjuncts
			temp_ptr=selects[i];
			BitSet positions = new BitSet();
			while(temp_ptr!=null){
				//inner loop to iterate the disjuncts
				int fieldNo;
				if(temp_ptr.type1.attrType==AttrType.attrSymbol&&temp_ptr.type2.attrType!=AttrType.attrSymbol)
					fieldNo= temp_ptr.operand1.symbol.offset;
				else if(temp_ptr.type2.attrType==AttrType.attrSymbol&&temp_ptr.type1.attrType!=AttrType.attrSymbol)
					fieldNo= temp_ptr.operand2.symbol.offset;
				else
					throw new IndexException("IndexScan.java: invalid constraint");
				constraint=f.indexToColName(temp_ptr.operand1.symbol.offset-1)
						.concat(temp_ptr.op.toString()).concat(temp_ptr.type2.attrType==AttrType.attrInteger
						?Integer.toString(temp_ptr.operand2.integer):temp_ptr.operand2.string)
						.concat(temp_ptr.indexType.toString());
				if(!duplicateConstraints.containsKey(constraint))
					duplicateFlag=checkDuplicateConstraint(constraint);
				else 
					duplicateFlag=true;
				if(!duplicateFlag || !duplicateConstraints.containsKey(constraint)){
					//building single constraint for the column index scan if the constraint is not a duplicate
					CondExpr[] colIndexScanFilter = new CondExpr[2];
					colIndexScanFilter[0] = new CondExpr();
					colIndexScanFilter[0].op=temp_ptr.op;
					colIndexScanFilter[0].type1=temp_ptr.type1;
					colIndexScanFilter[0].type2=temp_ptr.type2;
					colIndexScanFilter[0].operand1.symbol=new FldSpec (new RelSpec(RelSpec.outer), 1);
					colIndexScanFilter[0].operand2=temp_ptr.operand2;
					colIndexScanFilter[0].indexType=temp_ptr.indexType;
					colIndexScanFilter[0].next=null;
					colIndexScanFilter[1]=null;
					ColumnIndexScan indexScan = new ColumnIndexScan(indexTypes[constrCount], f,
							indNames[constrCount],_types,_s_sizes,
							_noInFlds, colIndexScanFilter, fieldNo);
					positions.or(indexScan.getPositionsOfIndexScan());
					indexScan.close();
					if(duplicateFlag)
						duplicateConstraints.put(constraint, positions);
				} else{
					positions.or(duplicateConstraints.get(constraint));
				}
				temp_ptr=temp_ptr.next;
				constrCount++;
			}
			if(i==0)
				outputPositions=positions;
			else 
				outputPositions.and(positions);
			i++;
		}
	}

	//Only for bitmap
	public ColumnarIndexScan(
			Columnarfile columnarFile,
			final int[] fldNums,
			IndexType[] indexTypes,
			final String[] indNames,
			AttrType[] types,
			short[] str_sizes,
			int noInFlds,
			CondExpr[] selects
	)
			throws Exception {
		_fldNum = fldNums;
		_noInFlds = noInFlds;
		_types = types;
		_s_sizes = str_sizes;
		index = indexTypes;

		_selects = selects;
		try {
			f = columnarFile;
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile not created");
		}
		int i=0;
		CondExpr temp_ptr;
		currentBitMapPos = 0;
		int constrCount=0;
		boolean duplicateFlag=false;
		duplicateConstraints = new HashMap<String,BitSet>();
		String constraint = new String();
		buildInputQueryString();
		while(selects[i]!=null){
			//outer loop to iterate the conjuncts
			temp_ptr=selects[i];
			BitSet positions = new BitSet();
			while(temp_ptr!=null){
				//inner loop to iterate the disjuncts
				int fieldNo;
				if(temp_ptr.type1.attrType==AttrType.attrSymbol&&temp_ptr.type2.attrType!=AttrType.attrSymbol)
					fieldNo= temp_ptr.operand1.symbol.offset;
				else if(temp_ptr.type2.attrType==AttrType.attrSymbol&&temp_ptr.type1.attrType!=AttrType.attrSymbol)
					fieldNo= temp_ptr.operand2.symbol.offset;
				else
					throw new IndexException("IndexScan.java: invalid constraint");
				constraint=f.indexToColName(temp_ptr.operand1.symbol.offset-1)
						.concat(temp_ptr.op.toString()).concat(temp_ptr.type2.attrType==AttrType.attrInteger
								?Integer.toString(temp_ptr.operand2.integer):temp_ptr.operand2.string)
						.concat(temp_ptr.indexType.toString());
				if(!duplicateConstraints.containsKey(constraint))
					duplicateFlag=checkDuplicateConstraint(constraint);
				else
					duplicateFlag=true;
				if(!duplicateFlag || !duplicateConstraints.containsKey(constraint)){
					//building single constraint for the column index scan if the constraint is not a duplicate
					CondExpr[] colIndexScanFilter = new CondExpr[2];
					colIndexScanFilter[0] = new CondExpr();
					colIndexScanFilter[0].op=temp_ptr.op;
					colIndexScanFilter[0].type1=temp_ptr.type1;
					colIndexScanFilter[0].type2=temp_ptr.type2;
					colIndexScanFilter[0].operand1.symbol=new FldSpec (new RelSpec(RelSpec.outer), 1);
					colIndexScanFilter[0].operand2=temp_ptr.operand2;
					colIndexScanFilter[0].indexType=temp_ptr.indexType;
					colIndexScanFilter[0].next=null;
					colIndexScanFilter[1]=null;
					ColumnIndexScan indexScan = new ColumnIndexScan(indexTypes[constrCount], f,
							indNames[constrCount],_types,_s_sizes,
							_noInFlds, colIndexScanFilter, fieldNo);
					positions.or(indexScan.getPositionsOfIndexScan());
					indexScan.close();
					if(duplicateFlag)
						duplicateConstraints.put(constraint, positions);
				} else{
					positions.or(duplicateConstraints.get(constraint));
				}
				temp_ptr=temp_ptr.next;
				constrCount++;
			}
			if(i==0)
				outputPositions=positions;
			else
				outputPositions.and(positions);
			i++;
		}
	}

	public BitSet getOutputPositions() {
		return outputPositions;
	}

	

	/**
	 * returns the next tuple with the target column values based on the position
	 * 
	 * @return the tuple
	 * @exception IndexException
	 *                error from the lower layer
	 * @exception UnknownKeyTypeException
	 *                key type unknown
	 * @exception IOException
	 *                from the lower layer
	 */
	public Tuple get_next() throws Exception {
		int position = outputPositions.nextSetBit(currentBitMapPos);
		try {
			if(position!=-1){
	            int j = 1;
	            for(int i : outIndexes) {
	                Heapfile file = f.getHeapfiles()[i];
	                RID id = file.findRID(position);
	                Tuple tuple = file.getRecord(id);
	                Jtuple.setFld(j, tuple.returnTupleByteArray());
	                j++;
	            }
	            currentBitMapPos = position+1;
	            return Jtuple;
			} else {
				return null;
			}
        }
        catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
	}

	  /**
	   * Cleaning up the index scan, does not remove either the original
	   * relation or the index from the database.
	   * @exception IndexException error from the lower layer
	   * @exception IOException from the lower layer
	   */
	  public void close() throws IOException, IndexException
	  {

	  }

	public void restart() throws FileScanException {
		currentBitMapPos = 0;
	}

	public int getTupleSize() {
		return Jtuple.size();
	}
	  
	  
	  private void buildInputQueryString() throws Exception {
		  int i=0;
		  CondExpr temp_ptr;
		  queryStr = new String();
		  while(_selects[i]!=null){
			  temp_ptr=_selects[i];
				while(temp_ptr!=null){
					queryStr=queryStr.concat(f.indexToColName(temp_ptr.operand1.symbol.offset-1))
							.concat(temp_ptr.op.toString())
							.concat(temp_ptr.type2.attrType==AttrType.attrInteger?Integer.toString(temp_ptr.operand2.integer):temp_ptr.operand2.string)
							.concat(temp_ptr.indexType.toString());
					if(temp_ptr.next!=null){
						queryStr=queryStr.concat("|");
					} else if(_selects[i+1]!=null){
						queryStr=queryStr.concat("^");
					}
					temp_ptr=temp_ptr.next;
				}
				i=i+1;
		  }
	  }
	  
	  private boolean checkDuplicateConstraint(final String constraint){
		  int count = 0;
		  int lastIndex = 0;
		  while(lastIndex != -1){

		      lastIndex = queryStr.indexOf(constraint,lastIndex);

		      if(lastIndex != -1){
		          count ++;
		          lastIndex += constraint.length();
		      }
		  }
		  if(count>1){
			  return true;
		  } else{
			  return false;
		  }
	  }
}

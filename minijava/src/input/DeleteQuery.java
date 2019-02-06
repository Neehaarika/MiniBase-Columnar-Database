package input;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.*;
import heap.Tuple;
import index.ColumnIndexScan;
import iterator.*;

import java.io.File;

public class DeleteQuery {
	
	private String columnDBName;
	private String columnarFileName;
	private String targetColsStr;
	private String constraintStr;
	private String[] targetColNames;
	private String accessType;
	private Integer numBuf;
	private String columnName;
	private AttrType[] types;
	private Columnarfile columnarFile;
	private int outCount;
	private int[] outAttrTypes;
	private int[] out_indexes;
	private String deleteType;

	public void execute(String[] args) {
		try {
			if (args.length < 6) {
				throw new Exception("Invalid number of attributes.");
			} else {
				columnDBName = args[0];
				if(!new File(columnDBName).exists()) {
					throw new Exception("Database does not exist.");
				}

				columnarFileName = args[1];

				constraintStr = args[2];
				if(!(constraintStr.startsWith("{") && constraintStr.endsWith("}"))) {
					throw new Exception("VALUECONSTRAINT format invalid.");
				}

				try {
					numBuf = Integer.parseInt(args[3]);
					if(numBuf<1) {
						throw new Exception("NUMBUF is not more than 1.");
					}
				} catch (Exception e) {
					throw new Exception("NUMBUF is not integer.");
				}

				accessType = args[4];
				if(!(accessType.equalsIgnoreCase("FILESCAN")
						|| accessType.equalsIgnoreCase("COLUMNSCAN")
						|| accessType.equalsIgnoreCase("BTREE")
						|| accessType.equalsIgnoreCase("BITMAP"))) {
					throw new Exception("access type invalid.");
				}

				deleteType = args[5];
				if(!(deleteType.equalsIgnoreCase("md") || deleteType.equalsIgnoreCase("pd"))) {
					throw new Exception("delete type invalid.");
				}



				SystemDefs columnDb = new SystemDefs(columnDBName, 0, numBuf, null);
				columnarFile = new Columnarfile(columnarFileName);

				PCounter.initialize();
				int startReadCount = PCounter.rcounter;
				int startWriteCount = PCounter.wcounter;

				if(accessType.equalsIgnoreCase("FILESCAN")) {
					executeFileScan();
				} else if(accessType.equalsIgnoreCase("COLUMNSCAN")) {
					executeColumnScan();
				} else if(accessType.equalsIgnoreCase("BTREE")) {
					executeBtreeScan();
				} else {
					executeBitmapScan();
				}

				//Flushing all written data to disk.
				//TODO handle flush gracefully
				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception e) {
//					e.printStackTrace();
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
				System.out.println("Read Pages: "+PCounter.readPages);
				System.out.println("Wrote Pages: "+PCounter.writePages);
//				System.out.println("Pinned Pages: "+PCounter.currentlyPinnedPages);
				System.out.println("=======================EXTRA METAINFO===============================");
				System.out.println(columnarFile.getTupleCnt());
				columnarFile.printDeleteBitset();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void executeFileScan() throws Exception {
		CondExpr[] outFilter = new CondExpr[2];
		buildQueryCondExpr(outFilter);
		ColumnarFileScan fs = new ColumnarFileScan(columnarFileName, columnarFile.getAttributeTypes(),
				columnarFile.getStringSizes(), columnarFile.getFieldCount(), outFilter);

		TID tid=null;
		while((tid=fs.get_next_tid())!=null){
			columnarFile.markTupleDeleted(tid);
		}
		fs.close();
		if(deleteType.equalsIgnoreCase("pd")) {
			columnarFile.purgeAllDeletedTuples();
		}
	}

	public void executeColumnScan() throws Exception {
		CondExpr[] outFilter = new CondExpr[2];
		buildQueryCondExprColscan(outFilter);
		String consAttr[] = constraintStr.substring(1,constraintStr.length()-1).trim().split(",");
		int colIndex = columnarFile.colNameToIndex(consAttr[0]);
		ColumnarColumnScan colScan = new ColumnarColumnScan(columnarFile,
				colIndex,
				outFilter);

		TID tid=null;

		while((tid=colScan.get_next_tid())!=null){
			columnarFile.markTupleDeleted(tid);
		}
		colScan.close();
		if(deleteType.equalsIgnoreCase("pd")) {
			columnarFile.purgeAllDeletedTuples();
		}
	}

	public void executeBtreeScan() throws Exception {
		CondExpr[] outFilter = new CondExpr[2];
		buildQueryCondExprColscan(outFilter);
		String consAttr[] = constraintStr.substring(1,constraintStr.length()-1).trim().split(",");
		int colIndex = columnarFile.colNameToIndex(consAttr[0]);

		if(columnarFile.btreeIndexExists(colIndex)) {

			ColumnIndexScan indexScan = new ColumnIndexScan(new IndexType(IndexType.B_Index), columnarFile,
					columnarFile.get_fileName()+".btree." + colIndex,
					columnarFile.getAttributeTypes(),
					columnarFile.getStringSizes(),
					columnarFile.getFieldCount(), outFilter, colIndex+1);

			TID tid=null;

			while((tid=indexScan.get_next_tid())!=null){
				columnarFile.markTupleDeleted(tid);
			}
			indexScan.close();
			if(deleteType.equalsIgnoreCase("pd")) {
				columnarFile.purgeAllDeletedTuples();
			}

		} else {
			throw new Exception("BTREE index does not exist on column "+consAttr[0]);
		}
	}

	public void executeBitmapScan() throws Exception {
		CondExpr[] outFilter = new CondExpr[2];
		buildQueryCondExprColscan(outFilter);
		String consAttr[] = constraintStr.substring(1,constraintStr.length()-1).trim().split(",");
		int colIndex = columnarFile.colNameToIndex(consAttr[0]);

		if(columnarFile.bitmapIndexExists(colIndex)) {

			ColumnIndexScan indexScan = new ColumnIndexScan(new IndexType(IndexType.Bitmap), columnarFile,
					columnarFile.get_fileName()+".bm." + colIndex + "." + consAttr[2],
					columnarFile.getAttributeTypes(),
					columnarFile.getStringSizes(),
					columnarFile.getFieldCount(), outFilter, colIndex+1);
			TID tid=null;

			while((tid=indexScan.get_next_tid())!=null){
				columnarFile.markTupleDeleted(tid);
			}
			indexScan.close();
			if(deleteType.equalsIgnoreCase("pd")) {
				columnarFile.purgeAllDeletedTuples();
			}
		} else {
			throw new Exception("Bitmap index does not exist on column "+consAttr[0]);
		}
	}
	
	private void buildQueryCondExpr(CondExpr[] expr) throws Exception{
		String consAttr[] = constraintStr.substring(1,constraintStr.length()-1).trim().split(",");
		if(consAttr.length!=3) {
			throw new Exception("Invalid VALUECONSTRAINT elements");
		}
		int colIndex = columnarFile.colNameToIndex(consAttr[0]);
		int colOffset = colIndex+1;

		expr[0]=new CondExpr();
		expr[0].op = AttrOperator.findOperator(consAttr[1]);
		expr[0].next  = null;
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    if(columnarFile.getAttributeTypes()[colIndex].attrType == AttrType.attrInteger){
	    	expr[0].type2 = new AttrType(AttrType.attrInteger);
	    } else {
	    	expr[0].type2 = new AttrType(AttrType.attrString);
	    }
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), colOffset); //offset to be checked
	    if(expr[0].type2.attrType==AttrType.attrInteger){
	    	expr[0].operand2.integer=Integer.parseInt(consAttr[2]);
	    } else {
	    	expr[0].operand2.string=consAttr[2];
	    }
	    expr[1] = null;
	}

	private void buildQueryCondExprColscan(CondExpr[] expr) throws Exception{
		String consAttr[] = constraintStr.substring(1,constraintStr.length()-1).trim().split(",");
		if(consAttr.length!=3) {
			throw new Exception("Invalid VALUECONSTRAINT elements");
		}
		int colIndex = columnarFile.colNameToIndex(consAttr[0]);

		expr[0]=new CondExpr();
		expr[0].op = AttrOperator.findOperator(consAttr[1]);
		expr[0].next  = null;
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		if(columnarFile.getAttributeTypes()[colIndex].attrType == AttrType.attrInteger){
			expr[0].type2 = new AttrType(AttrType.attrInteger);
		} else {
			expr[0].type2 = new AttrType(AttrType.attrString);
		}
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 1); //offset to be checked
		if(expr[0].type2.attrType==AttrType.attrInteger){
			expr[0].operand2.integer=Integer.parseInt(consAttr[2]);
		} else {
			expr[0].operand2.string=consAttr[2];
		}
		expr[1] = null;
	}
}

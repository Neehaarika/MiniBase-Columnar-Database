package input;

import java.io.File;
import java.util.SortedSet;
import java.util.TreeSet;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.*;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;

public class MultiIndexQuery {
	
	private String columnDBName;
	private String columnarFileName;
	private String targetColsStr;
	private String constraintStr;
	private String[] targetColNames;
	private IndexType[] indexTypes;
	private String[] indexNames;
	private Integer numBuf;
	private Columnarfile columnarFile;
	private int outCount;
	private int[] outAttrTypes;
	private int[] out_indexes;
	private int[] fldNums;

	public void execute(String[] args) {
		try {
			if (args.length < 5) {
				throw new Exception("Invalid number of attributes.");
			} else {
				columnDBName = args[0];
				if(!new File(columnDBName).exists()) {
					throw new Exception("Database does not exist.");
				}

				columnarFileName = args[1];

				targetColsStr = args[2];
				if(!(targetColsStr.startsWith("[") && targetColsStr.endsWith("]"))) {
					throw new Exception("[TARGETCOLUMNNAMES] format invalid.");
				} else {
					targetColNames = targetColsStr.substring(1,targetColsStr.length()-1).trim().split("\\s*,\\s*");
					if(targetColNames.length<1) {
						throw new Exception("No target columns given.");
					}
					outCount = (short) targetColNames.length;
				}

				constraintStr = args[3];

				try {
					numBuf = Integer.parseInt(args[4]);
					if(numBuf<1) {
						throw new Exception("NUMBUF is not more than 1.");
					}
				} catch (Exception e) {
					throw new Exception("NUMBUF is not integer.");
				}

				SystemDefs columnDb = new SystemDefs(columnDBName, 0, numBuf, null);

				columnarFile = new Columnarfile(columnarFileName);

				PCounter.initialize();
				int startReadCount = PCounter.rcounter;
                int startWriteCount = PCounter.wcounter;

				executeColmunarIndexScan();

				//Flushing all written data to disk.
				//TODO handle flush gracefully
				try {
					SystemDefs.JavabaseBM.flushAllPages();
				} catch (Exception e) {
				//e.printStackTrace();
				}

				int endReadCount = PCounter.rcounter;
                int endWriteCount = PCounter.wcounter;
                System.out.println("Read Page Count: "+(endReadCount-startReadCount));
                System.out.println("Write Page Count: "+(endWriteCount-startWriteCount));
				System.out.println("Read Pages: "+PCounter.readPages);
				System.out.println("Wrote Pages: "+PCounter.writePages);
//				System.out.println("Pinned Pages: "+PCounter.currentlyPinnedPages);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
     * Performs the columnar index scan by scanning multiple indexes to evaluate a CNF query
     * @throws Exception
     */
	private void executeColmunarIndexScan() throws Exception {
		FldSpec [] projectionList = new FldSpec[outCount];
		buildProjection(projectionList);
		CondExpr[] outFilter = new CondExpr[10];
		buildCNFQueryCondExpr(outFilter);
		ColumnarIndexScan indexScan = new ColumnarIndexScan(columnarFile, fldNums,
				indexTypes, indexNames,
				columnarFile.getAttributeTypes(),
				columnarFile.getStringSizes(),
				columnarFile.getFieldCount(), outCount, out_indexes,
				projectionList, outFilter, true);
		Tuple outputTuple=null;

		int resultCount = 0;
		for(int i=0; i<outCount; i++) {
			System.out.print(targetColNames[i]);
			if(i<outCount-1) System.out.print(", ");
		}
		System.out.println();
		while((outputTuple=indexScan.get_next())!=null){
			for(int i=0; i<outCount; i++) {
				if(outAttrTypes[i]==AttrType.attrInteger) {
					System.out.print(outputTuple.getIntFld(i+1));
				} else {
					System.out.print(outputTuple.getStrFld(i+1));
				}
				if(i<outCount-1) System.out.print(", ");
			}
			System.out.println();
			resultCount++;
		}
		indexScan.close();
		System.out.println();
		System.out.println("************************************************************************");
		System.out.println("Total Results Count By Query: "+resultCount);
		System.out.println("************************************************************************");
		System.out.println();
	}
	
	/**
     * Builds the output projection list for the target columns specified in the command line input
     * @throws Exception
     */
	private void buildProjection(FldSpec[] projection) throws Exception {
		out_indexes = new int[outCount];
		outAttrTypes = new int[outCount];
		for(int i=0;i<outCount;i++){
			int colIndex = columnarFile.colNameToIndex(targetColNames[i]);
			out_indexes[i] = colIndex;
			int colOffset = colIndex+1;
			projection[i]=new FldSpec(new RelSpec(RelSpec.outer), colOffset);
			outAttrTypes[i] = columnarFile.getAttributeTypes()[colIndex].attrType;
		}
	}
	
	/**
     * Parses the command line CNF query string in order to build 
     * @param cnfExpr the conjuncts of the CNF query 
     * @throws Exception
     */
	private void buildCNFQueryCondExpr(CondExpr[] cnfExpr) throws Exception {
		String[] conjuncts = constraintStr.split("\\^");
		SortedSet<Integer> fldNums = new TreeSet<Integer>(); // will be removed if not found to be useful in the invoked methods
		indexTypes = new IndexType[20]; // will be removed if index type is added in CondExpr.java
		indexNames = new String[20];	// will be removed as index name will not be present for range query bitsets  
		int constrCount=0;
		for (int conjunctIndex = 0; conjunctIndex < conjuncts.length; conjunctIndex++) {
			//outer loop to iterate the conjuncts
			if (!(conjuncts[conjunctIndex].startsWith("{") && conjuncts[conjunctIndex].endsWith("}")))
				throw new Exception("Invalid query format");
			
			cnfExpr[conjunctIndex]=new CondExpr();
			String[] disjuncts = conjuncts[conjunctIndex].substring(1, conjuncts[conjunctIndex].length() - 1).split("\\|");
			CondExpr temp;
			CondExpr innerExpr = null;
			for (int innerIndex = 0; innerIndex < disjuncts.length; innerIndex++) {
				//inner loop to iterate the disjuncts
				if (!(disjuncts[innerIndex].startsWith("(") && disjuncts[innerIndex].endsWith(")")))
					throw new Exception("Invalid query format");
				
				String[] consAttr = disjuncts[innerIndex].substring(1, disjuncts[innerIndex].length() - 1).trim().split("\\s*,\\s*");
				if (consAttr.length != 4)
					throw new Exception("Invalid VALUECONSTRAINT elements");
				if (!(consAttr[3].equalsIgnoreCase("BT")||consAttr[3].equalsIgnoreCase("BM")))
					throw new Exception("Index type invalid");
				
				temp=new CondExpr();
				int colIndex = columnarFile.colNameToIndex(consAttr[0]);
				fldNums.add(colIndex+1);
				temp.op=AttrOperator.findOperator(consAttr[1]);
				temp.type1 = new AttrType(AttrType.attrSymbol);
				if(columnarFile.getAttributeTypes()[colIndex].attrType == AttrType.attrInteger){
					temp.type2 = new AttrType(AttrType.attrInteger);
				} else {
					temp.type2 = new AttrType(AttrType.attrString);
				}
				temp.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), colIndex+1);
				if(temp.type2.attrType==AttrType.attrInteger){
					temp.operand2.integer=Integer.parseInt(consAttr[2]);
				} else {
					temp.operand2.string=consAttr[2];
				}
				if(consAttr[3].equalsIgnoreCase("BT")) {
					if(!columnarFile.btreeIndexExists(colIndex)) {
						throw new Exception("BTREE index does not exist on column "+consAttr[0]);
					}
					temp.indexType=new IndexType(IndexType.B_Index); //check which is needed
					indexTypes[constrCount]=new IndexType(IndexType.B_Index);
					indexNames[constrCount] = columnarFile.get_fileName()+".btree." + colIndex;
				}
				else {
					if(!columnarFile.bitmapIndexExists(colIndex)) {
						throw new Exception("Bitmap index does not exist on column "+consAttr[0]);
					}
					temp.indexType=new IndexType(IndexType.Bitmap);
					indexTypes[constrCount]=new IndexType(IndexType.Bitmap);
					indexNames[constrCount] = columnarFile.get_fileName()+".bm." + colIndex + "." + consAttr[2];
				}
				innerExpr=appendExpr(innerExpr,temp);
				constrCount++;
			}
			cnfExpr[conjunctIndex]=innerExpr;
			cnfExpr[conjunctIndex+1]=null;
		}
		// set the field numbers for the input columns specified in the query 
		int i=0;
		this.fldNums = new int[fldNums.size()];
		for(Integer fldno:fldNums){
			this.fldNums[i]=fldno;
			i++;
		}
	}
	
	/**
     * Appends the given conditional expression as the next element to the 
     * linked list of conditional expressions
     * @param innerExpr linked list of disjuncts
     * @param tempExpr the next OR condition to be appended 
     */
	private CondExpr appendExpr(CondExpr innerExpr, CondExpr tempExpr) {
		
		if(innerExpr==null)
			innerExpr=tempExpr;
		else {
			CondExpr temp = innerExpr;
			while(temp.next!=null){			
				temp = temp.next;
			}
			temp.next=tempExpr;
		}
		return innerExpr;
	}
	
}

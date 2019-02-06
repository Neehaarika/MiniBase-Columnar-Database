package input;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;
import iterator.Iterator;

import java.io.File;
import java.util.*;

public class NljQuery {
	String columnDBName;
	int numBuf;
	int amt_of_memory;
	Columnarfile outerColumnarFile;
	String outerColumnarFileName;
	String innerColumnarFileName;
	Columnarfile innerColumnarFile;
	Set<Integer> outerTargetColIndexes = new TreeSet<>();
	Set<Integer> innerTargetColIndexes = new TreeSet<>();
	List<CondExpr> outerLeftOver;
	List<CondExpr> innerLeftOver;

	public void execute(String[] args) {
		try {
			if (args.length < 11) {
				throw new Exception("Invalid number of attributes.");
			} else {
				columnDBName = args[0];
				if(!new File(columnDBName).exists()) {
					throw new Exception("Database does not exist.");
				}

				numBuf = -1;
				try {
					numBuf = Integer.parseInt(args[9]);
					if(numBuf<1) {
						throw new Exception("NUMBUF is not more than 1.");
					}
				} catch (Exception e) {
					throw new Exception("NUMBUF is not integer.");
				}

				amt_of_memory = -1;
				try {
					amt_of_memory = Integer.parseInt(args[10]);
					if(amt_of_memory<2) {
						throw new Exception("amt_of_memory is not more than 1.");
					}
				} catch (Exception e) {
					throw new Exception("amt_of_memory is not integer.");
				}

				SystemDefs columnDb = new SystemDefs(columnDBName, 0, numBuf, null);
				outerColumnarFileName = args[1];
				outerColumnarFile = new Columnarfile(outerColumnarFileName);
				innerColumnarFileName = args[2];
				innerColumnarFile = new Columnarfile(innerColumnarFileName);

				String outerConstraintStr = args[3];
				String innerConstraintStr = args[4];
				String joinConstraintStr = args[5];
				String outerAccessType = args[6];
				if(!(outerAccessType.equalsIgnoreCase("FILESCAN")
						|| outerAccessType.equalsIgnoreCase("COLUMNSCAN")
						|| outerAccessType.equalsIgnoreCase("BTREE")
						|| outerAccessType.equalsIgnoreCase("BITMAP"))) {
					throw new Exception("outerAccessType invalid.");
				}
				String innerAccessType = args[7];
				if(!(innerAccessType.equalsIgnoreCase("FILESCAN")
						|| innerAccessType.equalsIgnoreCase("COLUMNSCAN")
						|| innerAccessType.equalsIgnoreCase("BTREE")
						|| innerAccessType.equalsIgnoreCase("BITMAP"))) {
					throw new Exception("innerAccessType invalid.");
				}

				String targetColsStr = args[8];
                String[] targetColNames = null;
				if(!(targetColsStr.startsWith("[") && targetColsStr.endsWith("]"))) {
					throw new Exception("[TARGETCOLUMNNAMES] format invalid.");
				} else {
					targetColNames = targetColsStr.substring(1,targetColsStr.length()-1).trim().split("\\s*,\\s*");
					if(targetColNames.length<1) {
						throw new Exception("No target columns given.");
					}
					for(int i=0; i<targetColNames.length; i++) {
						String[] names = targetColNames[i].split("\\.");
						if(names[0].equals(outerColumnarFileName)) {
							int colIndex = outerColumnarFile.colNameToIndex(names[1]);
							outerTargetColIndexes.add(colIndex);
						} else {
							int colIndex = innerColumnarFile.colNameToIndex(names[1]);
							innerTargetColIndexes.add(colIndex);
						}
					}
				}
				findConsTargetCols(outerConstraintStr, outerAccessType, outerColumnarFile, outerTargetColIndexes);
				findConsTargetCols(innerConstraintStr, innerAccessType, innerColumnarFile, innerTargetColIndexes);
				findJoinTargetCols(joinConstraintStr);

				outerLeftOver = new ArrayList<>();
				CondExpr[] outFilter = buildCNFQueryCondExpr(outerConstraintStr, outerColumnarFile, outerAccessType, outerLeftOver, outerTargetColIndexes);
				CondExpr[] outer_pending_filter = null;
				if(outerLeftOver.size()>1) {
					outer_pending_filter = new CondExpr[outerLeftOver.size()];
					outer_pending_filter = outerLeftOver.toArray(outer_pending_filter);
				}

				innerLeftOver = new ArrayList<>();
				CondExpr[] innerFilter = buildCNFQueryCondExpr(innerConstraintStr, innerColumnarFile, innerAccessType, innerLeftOver, innerTargetColIndexes);
				CondExpr[] inner_pending_filter = null;
				if(innerLeftOver.size()>1) {
					inner_pending_filter = new CondExpr[innerLeftOver.size()];
					inner_pending_filter = innerLeftOver.toArray(inner_pending_filter);
				}

				CondExpr[] joinFilter = buildCNFJoinCondExpr(joinConstraintStr);

                List<AttrType> outAttrTypes = new ArrayList<>();
                List<FldSpec> proj_list = new ArrayList<>();
				for(int i=0; i<targetColNames.length; i++) {
					String[] names = targetColNames[i].split("\\.");
					if(names[0].equals(outerColumnarFileName)) {
						int colIndex = outerColumnarFile.colNameToIndex(names[1]);
						outAttrTypes.add(outerColumnarFile.getAttributeTypes()[colIndex]);
						int colOffset = findFieldOffset(colIndex, outerTargetColIndexes);
						proj_list.add(new FldSpec(new RelSpec(RelSpec.outer), colOffset));
					} else {
						int colIndex = innerColumnarFile.colNameToIndex(names[1]);
						outAttrTypes.add(innerColumnarFile.getAttributeTypes()[colIndex]);
						int colOffset = findFieldOffset(colIndex, innerTargetColIndexes);
						proj_list.add(new FldSpec(new RelSpec(RelSpec.innerRel), colOffset));
					}
				}

				PCounter.initialize();
				int startReadCount = PCounter.rcounter;
				int startWriteCount = PCounter.wcounter;

				Iterator outerItr = getIterator(outFilter, outerColumnarFile, outerTargetColIndexes, outerAccessType);
				Iterator innerItr = getIterator(innerFilter, innerColumnarFile, innerTargetColIndexes, innerAccessType);
				FldSpec[] proj = new FldSpec[proj_list.size()];
				proj = proj_list.toArray(proj);

				int in1_len = outerTargetColIndexes.size();
				AttrType[] in1 = new AttrType[in1_len];
				short[] t1_str_sizes = new short[in1_len];
				int k=0;
				int str=0;
				for(Integer colIndex: outerTargetColIndexes){
					in1[k] = outerColumnarFile.getAttributeTypes()[colIndex];
					if(in1[k].attrType==AttrType.attrString) {
						t1_str_sizes[str] = outerColumnarFile.getAttrSizes()[colIndex];
						str++;
					}
					k++;
				}
				int in2_len = innerTargetColIndexes.size();
				AttrType[] in2 = new AttrType[in2_len];
				short[] t2_str_sizes = new short[in2_len];
				k=0;
				str=0;
				for(Integer colIndex: innerTargetColIndexes){
					in2[k] = innerColumnarFile.getAttributeTypes()[colIndex];
					if(in2[k].attrType==AttrType.attrString) {
						t2_str_sizes[str] = innerColumnarFile.getAttrSizes()[colIndex];
						str++;
					}
					k++;
				}
				ColumnarNestedLoopJoins nlj = new ColumnarNestedLoopJoins(outerColumnarFile,
						innerColumnarFile, in1, in1_len, t1_str_sizes, in2, in2_len, t2_str_sizes, outerItr, innerItr, outer_pending_filter, inner_pending_filter,
						joinFilter,  proj, proj_list.size(), amt_of_memory);

				Tuple outputTuple=null;
				int outCount = targetColNames.length;
				for(int i=0; i<outCount; i++) {
					System.out.print(targetColNames[i]);
					if(i<outCount-1) System.out.print(", ");
				}
				System.out.println();
				int resultCount = 0;
				while((outputTuple=nlj.get_next())!=null){
					for(int i=0; i<outCount; i++) {
						if(outAttrTypes.get(i).attrType==AttrType.attrInteger) {
							System.out.print(outputTuple.getIntFld(i+1));
						} else {
							System.out.print(outputTuple.getStrFld(i+1));
						}
						if(i<outCount-1) System.out.print(", ");
					}
					System.out.println();
					resultCount++;
				}
				nlj.close();
				System.out.println();
				System.out.println("************************************************************************");
				System.out.println("Total Results Count By Query: "+resultCount);
				System.out.println("************************************************************************");
				System.out.println();
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

	private Iterator getIterator(CondExpr[] constraint, Columnarfile columnarFile, Set<Integer> targetColIndexes, String accessType) throws Exception {
		int outCount = targetColIndexes.size();
		FldSpec[] projection = new FldSpec[outCount];
		int[] out_indexes = new int[outCount];
		int[] outAttrTypes = new int[outCount];
		int i=0;
		for(Integer colIndex: targetColIndexes){
			out_indexes[i] = colIndex;
			int colOffset = colIndex+1;
			projection[i]=new FldSpec(new RelSpec(RelSpec.outer), colOffset);
			outAttrTypes[i] = columnarFile.getAttributeTypes()[colIndex].attrType;
			i++;
		}

		if(accessType.equalsIgnoreCase("FILESCAN")) {
			ColumnarFileScan fs = new ColumnarFileScan(columnarFile.get_fileName(), columnarFile.getAttributeTypes(),
					columnarFile.getStringSizes(), columnarFile.getFieldCount(), outCount,
					projection, constraint);
			return fs;
		} else if(accessType.equalsIgnoreCase("COLUMNSCAN")) {
			int len = constraint.length-1;
			Set<Integer> colNumsList = new TreeSet<>();
			for(int j=0; j<len; j++) {
				CondExpr temp = constraint[j];
				while (temp!=null) {
					colNumsList.add(temp.operand1.symbol.offset-1);
					temp = temp.next;
				}
			}
			for(int j=0; j<len; j++) {
				CondExpr temp = constraint[j];
				while (temp!=null) {
					temp.operand1.symbol.offset = findFieldOffset(temp.operand1.symbol.offset-1, colNumsList);
					temp = temp.next;
				}
			}
			int[] colNums = new int[colNumsList.size()];
			i=0;
			for(int val: colNumsList) {
				colNums[i] = val;
				i++;
			}
			ColumnarColumnsScan cs = new ColumnarColumnsScan(columnarFile, colNums, outCount, out_indexes, projection, constraint);
			return cs;
		} else if(accessType.equalsIgnoreCase("BTREE") || accessType.equalsIgnoreCase("BITMAP")) {

			int len = constraint.length-1;
			List<IndexType> indexTypesList = new ArrayList<>();
			List<Integer> fldNumsList = new ArrayList<>();
			List<String> indNamesList = new ArrayList<>();
			for(int j=0; j<len; j++) {
				CondExpr temp = constraint[j];
				while (temp!=null) {
					if(accessType.equalsIgnoreCase("BTREE")) {
						indexTypesList.add(new IndexType(IndexType.B_Index));
						temp.indexType = new IndexType(IndexType.B_Index);
						indNamesList.add(" ");
					} else {
						indexTypesList.add(new IndexType(IndexType.Bitmap));
						temp.indexType = new IndexType(IndexType.Bitmap);
						indNamesList.add(" ");
					}
					fldNumsList.add(temp.operand1.symbol.offset);
					temp = temp.next;
				}
			}
			IndexType[] indexTypes = new IndexType[indexTypesList.size()];
			indexTypes = indexTypesList.toArray(indexTypes);
			int[] fldNums = new int[fldNumsList.size()];
			i=0;
			for(int val: fldNumsList) {
				fldNums[i] = val;
				i++;
			}
			String[] indNames = new String[indNamesList.size()];
			indNames = indNamesList.toArray(indNames);

			ColumnarIndexScan is = new ColumnarIndexScan(columnarFile, fldNums, indexTypes, indNames,
					columnarFile.getAttributeTypes(), columnarFile.getStringSizes(), columnarFile.getFieldCount(),
					outCount, out_indexes, projection, constraint, false);
			return is;
		}
		return null;
	}
	
	/**
     * Parses the command line CNF query string in order to build 
     * @param cnfExpr the conjuncts of the CNF query 
     * @throws Exception
     */
	private CondExpr[] buildCNFQueryCondExpr(String constraintStr, Columnarfile columnarFile, String accessType, List<CondExpr> leftOver, Set<Integer> targetColIndexes) throws Exception {
		List<CondExpr> list = new ArrayList<>();
		String[] conjuncts = constraintStr.split("\\^");
		for (int conjunctIndex = 0; conjunctIndex < conjuncts.length; conjunctIndex++) {
			//outer loop to iterate the conjuncts
			if (!(conjuncts[conjunctIndex].startsWith("{") && conjuncts[conjunctIndex].endsWith("}")))
				throw new Exception("Invalid query format");
			
			String[] disjuncts = conjuncts[conjunctIndex].substring(1, conjuncts[conjunctIndex].length() - 1).split("\\|");
			CondExpr temp;
			CondExpr innerExpr = null;
			for (int innerIndex = 0; innerIndex < disjuncts.length; innerIndex++) {
				//inner loop to iterate the disjuncts
				if (!(disjuncts[innerIndex].startsWith("(") && disjuncts[innerIndex].endsWith(")")))
					throw new Exception("Invalid query format");
				
				String[] consAttr = disjuncts[innerIndex].substring(1, disjuncts[innerIndex].length() - 1).trim().split("\\s*,\\s*");
				if (consAttr.length != 3)
					throw new Exception("Invalid VALUECONSTRAINT elements");
				
				temp=new CondExpr();
				int colIndex = columnarFile.colNameToIndex(consAttr[0]);
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
				innerExpr=appendExpr(innerExpr,temp);
			}
			list.add(innerExpr);
		}

		//Handling primary construct for other access types
		if(!accessType.equalsIgnoreCase("FILESCAN")) {
			CondExpr temp;
			for(int i=1; i<list.size(); i++) {
				temp = list.get(i);
				while (temp != null) {
					temp.operand1.symbol.offset = findFieldOffset(temp.operand1.symbol.offset-1, targetColIndexes);
					temp = temp.next;
				}
			}
			leftOver.addAll(list.subList(1, list.size()));
			leftOver.add(null);
			list = list.subList(0, 1);
		}
		list.add(null);
		CondExpr[] exps = new CondExpr[list.size()];
		return list.toArray(exps);
	}

	private CondExpr[] buildCNFJoinCondExpr(String constraintStr) throws Exception {
		List<CondExpr> list = new ArrayList<>();
		String[] conjuncts = constraintStr.split("\\^");
		for (int conjunctIndex = 0; conjunctIndex < conjuncts.length; conjunctIndex++) {
			//outer loop to iterate the conjuncts
			if (!(conjuncts[conjunctIndex].startsWith("{") && conjuncts[conjunctIndex].endsWith("}")))
				throw new Exception("Invalid query format");

			String[] disjuncts = conjuncts[conjunctIndex].substring(1, conjuncts[conjunctIndex].length() - 1).split("\\|");
			CondExpr temp;
			CondExpr innerExpr = null;
			for (int innerIndex = 0; innerIndex < disjuncts.length; innerIndex++) {
				//inner loop to iterate the disjuncts
				if (!(disjuncts[innerIndex].startsWith("(") && disjuncts[innerIndex].endsWith(")")))
					throw new Exception("Invalid query format");

				String[] consAttr = disjuncts[innerIndex].substring(1, disjuncts[innerIndex].length() - 1).trim().split("\\s*,\\s*");
				if (consAttr.length != 3)
					throw new Exception("Invalid VALUECONSTRAINT elements");

				temp=new CondExpr();
				int outerColIndex = outerColumnarFile.colNameToIndex(consAttr[0]);
				int innerColIndex = innerColumnarFile.colNameToIndex(consAttr[2]);
				if(outerColumnarFile.getAttributeTypes()[outerColIndex].attrType
						!= innerColumnarFile.getAttributeTypes()[innerColIndex].attrType){
					throw new Exception("Invalid JOIN COLUMN ATTR TYPE NOT MATCH.");
				}
				temp.op=AttrOperator.findOperator(consAttr[1]);
				temp.type1 = new AttrType(AttrType.attrSymbol);
				temp.type2 = new AttrType(AttrType.attrSymbol);
				temp.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), findFieldOffset(outerColIndex, outerTargetColIndexes));
				temp.operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), findFieldOffset(innerColIndex, innerTargetColIndexes));
				innerExpr=appendExpr(innerExpr,temp);
			}
			list.add(innerExpr);
		}
		list.add(null);
		CondExpr[] exps = new CondExpr[list.size()];
		return list.toArray(exps);
	}

	private void findJoinTargetCols(String constraintStr) throws Exception {
		String[] conjuncts = constraintStr.split("\\^");
		for (int conjunctIndex = 0; conjunctIndex < conjuncts.length; conjunctIndex++) {
			//outer loop to iterate the conjuncts
			if (!(conjuncts[conjunctIndex].startsWith("{") && conjuncts[conjunctIndex].endsWith("}")))
				throw new Exception("Invalid query format");

			String[] disjuncts = conjuncts[conjunctIndex].substring(1, conjuncts[conjunctIndex].length() - 1).split("\\|");
			for (int innerIndex = 0; innerIndex < disjuncts.length; innerIndex++) {
				//inner loop to iterate the disjuncts
				if (!(disjuncts[innerIndex].startsWith("(") && disjuncts[innerIndex].endsWith(")")))
					throw new Exception("Invalid query format");

				String[] consAttr = disjuncts[innerIndex].substring(1, disjuncts[innerIndex].length() - 1).trim().split("\\s*,\\s*");
				if (consAttr.length != 3)
					throw new Exception("Invalid VALUECONSTRAINT elements");

				int outerColIndex = outerColumnarFile.colNameToIndex(consAttr[0]);
				outerTargetColIndexes.add(outerColIndex);
				int innerColIndex = innerColumnarFile.colNameToIndex(consAttr[2]);
				innerTargetColIndexes.add(innerColIndex);
				if(outerColumnarFile.getAttributeTypes()[outerColIndex].attrType
						!= innerColumnarFile.getAttributeTypes()[innerColIndex].attrType){
					throw new Exception("Invalid JOIN COLUMN ATTR TYPE NOT MATCH.");
				}
			}
		}
	}

	private void findConsTargetCols(String constraintStr, String accessType, Columnarfile columnarfile, Set<Integer> targetCols) throws Exception {
		if(!accessType.equalsIgnoreCase("FILESCAN")) {
			String[] conjuncts = constraintStr.split("\\^");
			for (int conjunctIndex = 1; conjunctIndex < conjuncts.length; conjunctIndex++) {
				//outer loop to iterate the conjuncts
				if (!(conjuncts[conjunctIndex].startsWith("{") && conjuncts[conjunctIndex].endsWith("}")))
					throw new Exception("Invalid query format");

				String[] disjuncts = conjuncts[conjunctIndex].substring(1, conjuncts[conjunctIndex].length() - 1).split("\\|");
				for (int innerIndex = 0; innerIndex < disjuncts.length; innerIndex++) {
					//inner loop to iterate the disjuncts
					if (!(disjuncts[innerIndex].startsWith("(") && disjuncts[innerIndex].endsWith(")")))
						throw new Exception("Invalid query format");

					String[] consAttr = disjuncts[innerIndex].substring(1, disjuncts[innerIndex].length() - 1).trim().split("\\s*,\\s*");
					if (consAttr.length != 3)
						throw new Exception("Invalid VALUECONSTRAINT elements");

					int colIndex = columnarfile.colNameToIndex(consAttr[0]);
					targetCols.add(colIndex);
				}

			}
		}
	}

	private int findFieldOffset(int index, Set<Integer> indexes) {
        int offset = 1;
        for(int val: indexes) {
            if(val==index) {
                return offset;
            }
            offset++;
        }
        return -1;
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

package input;

import java.io.File;
import java.io.IOException;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import bitmap.BitMapFile;
import chainexception.ChainException;
import columnar.Columnarfile;
import diskmgr.PCounter;
import diskmgr.Page;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;

public class BitMapQuery {
	private String columnDBName;
	private String outerColumnarFileName;
	private String innerColumnarFileName;
	private Columnarfile outerColumnarFile;
	private Columnarfile innerColumnarFile;
	private String targetColsStr;
	private String outerConstraint;
	private String innerConstraint;
	private String equiConstraint;
	private String[] targetColNames;
	private int bufferSize;
	private BitSet innerConditionsBitset;
	private BitSet outerConditionsBitset;
	private Set<Integer> outerTargetColIndexes;
	private Set<Integer> innerTargetColIndexes;
	private FldSpec[] proj;
	private Tuple finalTuple;
	private List<AttrType> outAttrTypes;
	private Tuple ituple;
	private AttrType[] itypes;
	private short[] i_s_sizes;
	private Tuple otuple;
	private AttrType[] otypes;
	private short[] o_s_sizes;
	private int nOutFlds;


	public void execute(String[] args) throws Exception {
		try {
			if (args.length < 8) {
				throw new Exception("Invalid number of attributes.");
			} else {
				columnDBName = args[0];
				if (!new File(columnDBName).exists()) {
					throw new Exception("Database does not exist.");
				}

				bufferSize = Integer.parseInt(args[7]);

				SystemDefs coulmnDb = new SystemDefs(columnDBName, 0, bufferSize, null);

				outerColumnarFileName = args[1];
				outerColumnarFile = new Columnarfile(outerColumnarFileName);

				innerColumnarFileName = args[2];
				innerColumnarFile = new Columnarfile(innerColumnarFileName);

				PCounter.initialize();
				int startReadCount = PCounter.rcounter;
				int startWriteCount = PCounter.wcounter;

				// OUTER CONSTRAINT
				outerConstraint = args[3];
				outerConditionsBitset = getConstraintBitset(outerColumnarFile, outerConstraint);
				System.out.println("OuterConstraint Bitset After performing AND and ORs");
				System.out.println(outerConditionsBitset);

				// INNER CONSTRAINT
				innerConstraint = args[4];
				innerConditionsBitset = getConstraintBitset(innerColumnarFile, innerConstraint);
				System.out.println("InnerConstraint Bitset After performing AND and ORs");
				System.out.println(innerConditionsBitset);

				targetColsStr = args[6];
				createProjectionsAndTuples();

				// EQUIJOIN CONSTRAINT
				equiConstraint = args[5];
				executeJoin();

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
		} catch (

		Exception e) {
			throw e;
		}
	}// end of execute method

	private void createProjectionsAndTuples() throws Exception {
		outAttrTypes = new ArrayList<>();
		List<FldSpec> proj_list = new ArrayList<>();
		outerTargetColIndexes = new TreeSet<>();
		innerTargetColIndexes = new TreeSet<>();
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
		proj = new FldSpec[proj_list.size()];
		proj = proj_list.toArray(proj);

		FldSpec[] oproj = new FldSpec[outerTargetColIndexes.size()];
		int i=0;
		for(Integer colIndex: outerTargetColIndexes) {
			oproj[i] = new FldSpec(new RelSpec(RelSpec.outer), colIndex+1);
			i++;
		}
		otuple =  new Tuple();
		otypes = new AttrType[outerTargetColIndexes.size()];
		o_s_sizes = new short[0];
		if(outerTargetColIndexes.size()>0) {
			o_s_sizes = TupleUtils.setup_op_tuple(otuple, otypes, outerColumnarFile.getAttributeTypes(), outerColumnarFile.getFieldCount(), outerColumnarFile.getStringSizes(), oproj, outerTargetColIndexes.size());
		}

		FldSpec[] iproj = new FldSpec[innerTargetColIndexes.size()];
		i=0;
		for(Integer colIndex: innerTargetColIndexes) {
			iproj[i] = new FldSpec(new RelSpec(RelSpec.outer), colIndex+1);
			i++;
		}
		ituple =  new Tuple();
		itypes = new AttrType[innerTargetColIndexes.size()];
		i_s_sizes = new short[0];
		if(innerTargetColIndexes.size()>0) {
			i_s_sizes = TupleUtils.setup_op_tuple(ituple, itypes, innerColumnarFile.getAttributeTypes(), innerColumnarFile.getFieldCount(), innerColumnarFile.getStringSizes(), iproj, innerTargetColIndexes.size());
		}

		finalTuple = new Tuple();
		nOutFlds = proj_list.size();
		AttrType[] ftypes = new AttrType[proj_list.size()];
		TupleUtils.setup_op_tuple(finalTuple, ftypes, otypes, outerTargetColIndexes.size(), itypes, innerTargetColIndexes.size(), o_s_sizes,
				i_s_sizes, proj, proj_list.size());
	}

	private void executeJoin() throws Exception {
		List<IndexType> indexTypesList = new ArrayList<>();
		List<Integer> fldNumsList = new ArrayList<>();
		List<String> indNamesList = new ArrayList<>();
		Set<Integer> outerColsInJoin = new TreeSet<>();
		CondExpr[] constraint = buildCNFJoinCondExprForFilling(equiConstraint, outerColsInJoin, indexTypesList,
				fldNumsList, indNamesList);

		IndexType[] indexTypes = new IndexType[indexTypesList.size()];
		indexTypes = indexTypesList.toArray(indexTypes);
		int[] fldNums = new int[fldNumsList.size()];
		int i=0;
		for(int val: fldNumsList) {
			fldNums[i] = val;
			i++;
		}
		String[] indNames = new String[indNamesList.size()];
		indNames = indNamesList.toArray(indNames);

		FldSpec[] oproj = new FldSpec[outerColsInJoin.size()];
		i=0;
		for(Integer colIndex: outerColsInJoin) {
			oproj[i] = new FldSpec(new RelSpec(RelSpec.outer), colIndex+1);
			i++;
		}
		Tuple outer_tuple =  new Tuple();
		AttrType[] outer_types = new AttrType[outerColsInJoin.size()];
		short[] outer_s_sizes = TupleUtils.setup_op_tuple(outer_tuple, outer_types, outerColumnarFile.getAttributeTypes(),
				outerColumnarFile.getFieldCount(), outerColumnarFile.getStringSizes(), oproj, outerColsInJoin.size());

		int outCount = targetColNames.length;
		for(i=0; i<outCount; i++) {
			System.out.print(targetColNames[i]);
			if(i<outCount-1) System.out.print(", ");
		}
		System.out.println();
		int resultCount = 0;

		int outerPosition = 0;
		int o_pointer = 0;
		while((o_pointer = outerConditionsBitset.nextSetBit(outerPosition))!=-1) {
			Map<Integer,Object> map = new HashMap<>();
			int j = 1;
			for(int col : outerColsInJoin) {
				Heapfile file = outerColumnarFile.getHeapfiles()[col];
				RID id = file.findRID(o_pointer);
				Tuple tuple = file.getRecord(id);
				outer_tuple.setFld(j, tuple.returnTupleByteArray());
				if(outer_types[j-1].attrType==AttrType.attrString) {
					map.put(col, outer_tuple.getStrFld(j));
				} else {
					map.put(col, outer_tuple.getIntFld(j));
				}
				j++;
			}
			updateConstraint(constraint, map);

			ColumnarIndexScan is = new ColumnarIndexScan(innerColumnarFile, fldNums, indexTypes, indNames,
					innerColumnarFile.getAttributeTypes(), innerColumnarFile.getStringSizes(),
					innerColumnarFile.getFieldCount(), constraint);
			BitSet joinBitset = is.getOutputPositions();
			is.close();
			joinBitset.and(innerConditionsBitset);
			if(joinBitset.length()>0) {
				j=1;
				for(int col : outerTargetColIndexes) {
					if(map.containsKey(col)) {
						if(outerColumnarFile.getAttributeType(col).attrType==AttrType.attrString) {
							otuple.setStrFld(j, (String)map.get(col));
						} else {
							otuple.setIntFld(j, (Integer) map.get(col));
						}
					} else {
						Heapfile file = outerColumnarFile.getHeapfiles()[col];
						RID id = file.findRID(o_pointer);
						Tuple tuple = file.getRecord(id);
						otuple.setFld(j, tuple.returnTupleByteArray());
					}
					j++;
				}
				int innerPosition = 0;
				int i_pointer = 0;
				while((i_pointer = joinBitset.nextSetBit(innerPosition))!=-1) {
					j=1;
					for(int col : innerTargetColIndexes) {
						Heapfile file = innerColumnarFile.getHeapfiles()[col];
						RID id = file.findRID(i_pointer);
						Tuple tuple = file.getRecord(id);
						ituple.setFld(j, tuple.returnTupleByteArray());
						j++;
					}

					Projection.Join(otuple, otypes,
							ituple, itypes, finalTuple, proj, nOutFlds);

					for(i=0; i<outCount; i++) {
						if(outAttrTypes.get(i).attrType==AttrType.attrInteger) {
							System.out.print(finalTuple.getIntFld(i+1));
						} else {
							System.out.print(finalTuple.getStrFld(i+1));
						}
						if(i<outCount-1) System.out.print(", ");
					}
					System.out.println();
					resultCount++;

					innerPosition = i_pointer+1;
				}
			}

			outerPosition = o_pointer+1;
		}

		System.out.println();
		System.out.println("************************************************************************");
		System.out.println("Total Results Count By Query: "+resultCount);
		System.out.println("************************************************************************");
		System.out.println();
	}

	private void updateConstraint(CondExpr[] constraint, Map<Integer,Object> map) {
		int len = constraint.length-1;
		for(int i=0; i<len; i++) {
			CondExpr temp = constraint[i];
			while (temp!=null) {
				if(temp.type2.attrType==AttrType.attrString) {
					temp.operand2.string = (String) map.get(temp.operand2.symbol.offset-1);
				} else {
					temp.operand2.integer = (Integer) map.get(temp.operand2.symbol.offset-1);
				}
				temp = temp.next;
			}
		}
	}

	private BitSet getConstraintBitset(Columnarfile columnarFile, String constraintStr) throws Exception {
		List<IndexType> indexTypesList = new ArrayList<>();
		List<Integer> fldNumsList = new ArrayList<>();
		List<String> indNamesList = new ArrayList<>();
		CondExpr[] constraint = buildCNFQueryCondExpr(constraintStr, columnarFile, indexTypesList, fldNumsList, indNamesList);

		IndexType[] indexTypes = new IndexType[indexTypesList.size()];
		indexTypes = indexTypesList.toArray(indexTypes);
		int[] fldNums = new int[fldNumsList.size()];
		int i=0;
		for(int val: fldNumsList) {
			fldNums[i] = val;
			i++;
		}
		String[] indNames = new String[indNamesList.size()];
		indNames = indNamesList.toArray(indNames);

		ColumnarIndexScan is = new ColumnarIndexScan(columnarFile, fldNums, indexTypes, indNames,
				columnarFile.getAttributeTypes(), columnarFile.getStringSizes(),
				columnarFile.getFieldCount(), constraint);
		BitSet bitSet = is.getOutputPositions();
		is.close();
		return bitSet;
	}

	private CondExpr[] buildCNFQueryCondExpr(String constraintStr,
											 Columnarfile columnarFile,
											 List<IndexType> indexTypesList,
											 List<Integer> fldNumsList,
											 List<String> indNamesList) throws Exception {
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
				temp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), colIndex+1);
				if(temp.type2.attrType==AttrType.attrInteger){
					temp.operand2.integer=Integer.parseInt(consAttr[2]);
				} else {
					temp.operand2.string=consAttr[2];
				}

				indexTypesList.add(new IndexType(IndexType.Bitmap));
				temp.indexType = new IndexType(IndexType.Bitmap);
				indNamesList.add(" ");
				fldNumsList.add(temp.operand1.symbol.offset);

				innerExpr=appendExpr(innerExpr,temp);
			}
			list.add(innerExpr);
		}

		list.add(null);
		CondExpr[] exps = new CondExpr[list.size()];
		return list.toArray(exps);
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

	private CondExpr[] buildCNFJoinCondExprForFilling(String constraintStr, Set<Integer> outerTargets,
													  List<IndexType> indexTypesList,
													  List<Integer> fldNumsList,
													  List<String> indNamesList) throws Exception {
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
				outerTargets.add(outerColIndex);
				int innerColIndex = innerColumnarFile.colNameToIndex(consAttr[2]);
				if(outerColumnarFile.getAttributeTypes()[outerColIndex].attrType
						!= innerColumnarFile.getAttributeTypes()[innerColIndex].attrType){
					throw new Exception("Invalid JOIN COLUMN ATTR TYPE NOT MATCH.");
				}
				temp.op=AttrOperator.getOppositeOperator(consAttr[1]);
				temp.type1 = new AttrType(AttrType.attrSymbol);
				if(innerColumnarFile.getAttributeTypes()[innerColIndex].attrType == AttrType.attrInteger){
					temp.type2 = new AttrType(AttrType.attrInteger);
				} else {
					temp.type2 = new AttrType(AttrType.attrString);
				}
				temp.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), innerColIndex+1);
				//To remember outer col Index for update
				temp.operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer), outerColIndex+1);

				indexTypesList.add(new IndexType(IndexType.Bitmap));
				temp.indexType = new IndexType(IndexType.Bitmap);
				indNamesList.add(" ");
				fldNumsList.add(temp.operand1.symbol.offset);

				innerExpr=appendExpr(innerExpr,temp);
			}
			list.add(innerExpr);
		}
		list.add(null);
		CondExpr[] exps = new CondExpr[list.size()];
		return list.toArray(exps);
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

}

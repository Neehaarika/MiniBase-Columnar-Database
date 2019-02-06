package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import global.*;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 * nlj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST JOINCONST OUTERACCTYPE INNERACCTYPE [TARGETCOLUMNS] NUMBUF
 */
public class ColumnarNestedLoopJoins extends Iterator implements GlobalConst {
	private AttrType _in1[], _in2[];
	private int _in1_len, _in2_len;
	private short t1_str_sizescopy[];
	private short t2_str_sizescopy[];
	private CondExpr OuterFilter[];
	private CondExpr RightFilter[];
	private CondExpr JoinFilter[];
	private int n_buf_pgs; // # of buffer pages available.
	private Tuple Jtuple; // Joined tuple
	private short[] jTuple_size;
	private AttrType[] Jtypes;
	private FldSpec perm_mat[];
	private int nOutFlds;
	private Columnarfile outerColumnarFile;
	private Columnarfile innerColumnarFile;
	private Iterator outerItr;
	private Iterator innerItr;
	private int innerbuffer_pages;
	private int outerbuffer_pages;
	private List<Tuple> outerBuf;
	private List<Tuple> innerBuf;
	private int outer_ptr = 0;
	private int inner_ptr = 0;
	private PageId[] pids;
	private PageId[] i_pids;
	private int totalOuterCount = 0;
	private int itrTotalOuterCount = 0;
	private int pass = 0;


	public ColumnarNestedLoopJoins(Columnarfile outerColumnarFile,
                                   Columnarfile innerColumnarFile,
								   AttrType in1[],
								   int in1_len,
								   short[] t1_str_sizes,
								   AttrType in2[],
								   int in2_len,
								   short[] t2_str_sizes,
                                   Iterator outerItr,
                                   Iterator innerItr,
                                   CondExpr[] outFilter,
                                   CondExpr[] rightFilter,
                                   CondExpr[] joinFilter,
                                   FldSpec[] proj_list, int n_out_flds, int amt_of_mem)
			throws IOException,
			NestedLoopException,
			InvalidTypeException,
			InvalidTupleSizeException, JoinsException, HFException, HFBufMgrException, HFDiskMgrException {
//		System.out.println("-------Inside columnar nested loop join------");
		this.outerColumnarFile = outerColumnarFile;
		this.innerColumnarFile = innerColumnarFile;
		this.outerItr = outerItr;
		this.innerItr = innerItr;
		this.OuterFilter = outFilter;
		this.RightFilter = rightFilter;
		this.JoinFilter = joinFilter;
		_in1 = in1;
		_in2 = in2;
		_in1_len = in1_len;
		_in2_len = in2_len;
		t1_str_sizescopy = t1_str_sizes;
		t2_str_sizescopy = t2_str_sizes;

		n_buf_pgs = amt_of_mem;
		innerbuffer_pages = 1;
		outerbuffer_pages = n_buf_pgs-1;

		Jtuple = new Tuple();
		Jtypes = new AttrType[n_out_flds];
		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		try {
			jTuple_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, _in1, _in1_len, _in2, _in2_len, t1_str_sizescopy,
					t2_str_sizescopy, proj_list, nOutFlds);
		} catch (TupleUtilsException e) {
			throw new NestedLoopException(e, "TupleUtilsException is caught by ColumnarNestedLoopsJoins.java");
		}

		try {
			pids = new PageId[outerbuffer_pages];
			byte[][] bufs = new byte[outerbuffer_pages][MINIBASE_PAGESIZE];
			outerItr.get_buffer_pages(outerbuffer_pages, pids, bufs);
			i_pids = new PageId[innerbuffer_pages];
			byte[][] i_bufs = new byte[innerbuffer_pages][MINIBASE_PAGESIZE];
			innerItr.get_buffer_pages(innerbuffer_pages, i_pids, i_bufs);
			System.out.println();
			System.out.println("************************************************************************");
			System.out.println("Next Pass Over Inner Table: "+pass);
			System.out.println("************************************************************************");
			System.out.println();
			fillOuterBuffer();
			fillInnerBuffer();
		} catch (Exception e) {
			throw new NestedLoopException(e, "Exceeption is caught by ColumnarNestedLoopsJoins.java");
		}
	}

	private void fillOuterBuffer() throws Exception {
//		System.out.println("outer relation buff:" + outerbuffer_pages + "\ninner relation buff:" + innerbuffer_pages);
		Tuple outer_tuple;
		int outerTupleCount = 0;
		int noOfOuterTuplesToBeFetched = outerbuffer_pages * (MINIBASE_PAGESIZE / outerItr.getTupleSize());
		outerBuf = new ArrayList<>(noOfOuterTuplesToBeFetched);
		while (outerTupleCount < noOfOuterTuplesToBeFetched && (outer_tuple = outerItr.get_next()) != null) {
			itrTotalOuterCount++;
			if (PredEval.Eval(OuterFilter, outer_tuple, null, _in1, null) == true) {
//				System.out.println("Outer tuple fields: " + outer_tuple.getStrFld(1));
				outerBuf.add(new Tuple(outer_tuple));
				outerTupleCount++;
				totalOuterCount++;
			}
//			System.out.println("tuples evaluated: " + outerTupleCount);
		}
        /*for(Tuple t: outerBuf) {
            System.out.print("Outer: ");
            t.print(outerColumnarFile.getAttributeTypes());
        }*/
	}

	private void fillInnerBuffer() throws Exception {
//		System.out.println("inner relation buff:" + innerbuffer_pages);
		int innerTupleCount = 0;
		Tuple inner_tuple;
		int noOfInnerTuplesToBeFetched = innerbuffer_pages * (MINIBASE_PAGESIZE / innerItr.getTupleSize());
		innerBuf = new ArrayList<>(noOfInnerTuplesToBeFetched);
		while (innerTupleCount < noOfInnerTuplesToBeFetched && (inner_tuple = innerItr.get_next()) != null) {
			if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true) {
//				System.out.println("Inner tuple fields: " + inner_tuple.getStrFld(1));
				innerBuf.add(new Tuple(inner_tuple));
				innerTupleCount++;
			}
//			System.out.println("tuples evaluated: " + innerTupleCount);
		}
		/*for(Tuple t: innerBuf) {
            System.out.print("INNER: ");
            t.print(innerColumnarFile.getAttributeTypes());
        }*/
	}

	@Override
	public Tuple get_next() throws InvalidTupleSizeException, IOException, InvalidTypeException {
		while (true) {
			try {
				if(inner_ptr<innerBuf.size()) {
					if(outer_ptr<outerBuf.size()) {
						Tuple iTuple = innerBuf.get(inner_ptr);
						Tuple oTuple = outerBuf.get(outer_ptr);
						outer_ptr++;
						if (PredEval.Eval(JoinFilter, oTuple, iTuple, _in1, _in2) == true) {
							Projection.Join(oTuple, _in1, iTuple, _in2, Jtuple, perm_mat, nOutFlds);
							return Jtuple;
						}
					} else {
						outer_ptr = 0;
						inner_ptr++;
						continue;
					}
				} else {
					fillInnerBuffer();
					if(innerBuf.size()==0) {
						fillOuterBuffer();
						if(outerBuf.size()==0) {
							int noOfOuterTuplesToBeFetched = outerbuffer_pages * (MINIBASE_PAGESIZE / outerItr.getTupleSize());
							System.out.println();
							System.out.println("************************************************************************");
							System.out.println("Tuple Size: "+outerItr.getTupleSize());
							System.out.println("Number of Tuples Buffer Can Hold: "+noOfOuterTuplesToBeFetched);
							System.out.println("Total Outer Tuples By Full Constraint: "+totalOuterCount);
							System.out.println("Total Outer Tuples By Iterator: "+itrTotalOuterCount);
							System.out.println("************************************************************************");
							System.out.println();
							return null;
						} else {
							pass++;
							System.out.println();
							System.out.println("************************************************************************");
							System.out.println("Next Pass Over Inner Table: "+pass);
							System.out.println("************************************************************************");
							System.out.println();
							innerItr.restart();
							fillInnerBuffer();
						}
					}
					inner_ptr = 0;
					outer_ptr = 0;
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		try{
			for(PageId pid: pids) {
				SystemDefs.JavabaseBM.unpinPage(pid, false);
			}
			outerItr.free_buffer_pages(outerbuffer_pages, pids);
			for(PageId pid: i_pids) {
				SystemDefs.JavabaseBM.unpinPage(pid, false);
			}
			innerItr.free_buffer_pages(innerbuffer_pages, i_pids);
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
		innerItr.close();
		outerItr.close();
	}
}

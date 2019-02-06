package iterator;


import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.TupleScan;
import global.AttrType;
import global.Convert;
import global.RID;
import global.TID;
import heap.*;

import java.io.IOException;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class ColumnarColumnScan extends  Iterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes;
  private Columnarfile f;
  private int colNo;
  private Scan scan;
  private Tuple     tuple1;
  private Tuple    Jtuple;
  private int        t1_size;
  private int nOutFlds;
    private int[] outIndexes;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;
    private AttrType[] destType;
    private short dest_len;
    private short[] dest_s_sizes;


  public ColumnarColumnScan(Columnarfile  columnarfile,
                            int colNo,
                            int n_out_flds,
                            int[] out_indexes,
                            FldSpec[] proj_list,
                            CondExpr[]  outFilter
		    )
    throws IOException,
	   FileScanException,
	   TupleUtilsException, 
	   InvalidRelation
    {
       f = columnarfile;
        this.colNo = colNo;
      _in1 = columnarfile.getAttributeTypes();
      in1_len = columnarfile.getFieldCount();
      s_sizes = columnarfile.getStringSizes();
      
      Jtuple =  new Tuple();
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    ts_size;
      ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, _in1, in1_len, s_sizes, proj_list, n_out_flds);
      
      OutputFilter = outFilter;
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
        outIndexes = out_indexes;
      tuple1 =  new Tuple();

        dest_len = 1;
        destType = new AttrType[1];
        destType[0] = columnarfile.getAttributeTypes()[colNo];
        if(destType[0].attrType == AttrType.attrString) {
            dest_s_sizes = new short[] {columnarfile.getAttrSizes()[colNo]};
        }

      try {
	tuple1.setHdr(dest_len, destType, dest_s_sizes);
      }catch (Exception e){
	throw new FileScanException(e, "setHdr() failed");
      }
      t1_size = tuple1.size();
      
      try {
	scan = f.openColumnScan(colNo);
      }
      catch(Exception e){
	throw new FileScanException(e, "openTupleScan() failed");
      }
    }

    //Only use for delete query
  public ColumnarColumnScan(Columnarfile  columnarfile,
                            int colNo,
                            CondExpr[]  outFilter
  )
          throws IOException,
          FileScanException,
          TupleUtilsException,
          InvalidRelation
  {
    f = columnarfile;
    this.colNo = colNo;
    _in1 = columnarfile.getAttributeTypes();
    in1_len = columnarfile.getFieldCount();
    s_sizes = columnarfile.getStringSizes();

    OutputFilter = outFilter;
    tuple1 =  new Tuple();

    dest_len = 1;
    destType = new AttrType[1];
    destType[0] = columnarfile.getAttributeTypes()[colNo];
    if(destType[0].attrType == AttrType.attrString) {
      dest_s_sizes = new short[] {columnarfile.getAttrSizes()[colNo]};
    }

    try {
      tuple1.setHdr(dest_len, destType, dest_s_sizes);
    }catch (Exception e){
      throw new FileScanException(e, "setHdr() failed");
    }
    t1_size = tuple1.size();

    try {
      scan = f.openColumnScan(colNo);
    }
    catch(Exception e){
      throw new FileScanException(e, "openTupleScan() failed");
    }
  }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public Tuple get_next()
    throws Exception
    {     
      RID rid = new RID();
      Tuple tuple;
      while(true) {
        if((tuple =  scan.getNext(rid)) == null) {
          return null;
        }

        tuple1.setFld(1, tuple.returnTupleByteArray());

        if (PredEval.Eval(OutputFilter, tuple1, null, destType, null) == true){
            int position = f.getHeapfiles()[colNo].findPosition(rid);
            int j = 1;
            for(int i : outIndexes) {
                Heapfile file = f.getHeapfiles()[i];
                RID id = file.findRID(position);
                tuple = file.getRecord(id);
                Jtuple.setFld(j, tuple.returnTupleByteArray());
                j++;
            }
    	  return  Jtuple;
        }
      }
    }

  public TID get_next_tid()
          throws Exception
  {
    RID rid = new RID();
    Tuple tuple;
    while(true) {
      if((tuple =  scan.getNext(rid)) == null) {
        return null;
      }

      tuple1.setFld(1, tuple.returnTupleByteArray());

      if (PredEval.Eval(OutputFilter, tuple1, null, destType, null) == true){
        int position = f.getHeapfiles()[colNo].findPosition(rid);
        TID tid = new TID(f.getFieldCount(), position);
        return tid;
      }
    }
  }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}



package index;

import bitmap.BitMapFile;
import btree.*;
import columnar.Columnarfile;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import input.Index;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;


/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class ColumnIndexScan extends Iterator {


    public FldSpec[]      perm_mat;
    private IndexFile     indFile;
    private IndexFileScan indScan;
    private BitMapFile bitMapFile;
    private BitSet bitSet;
    private AttrType[]    _types;
    private short[]       _s_sizes;
    private CondExpr[]    _selects;
    private int           _noInFlds;
    private int           _noOutFlds;
    private int[] outIndexes;
    private Columnarfile      f;
    private Tuple         tuple1;
    private Tuple         Jtuple;
    private int           t1_size;
    private int           _fldNum;
    private boolean       index_only;
    private int colNo;
    private AttrType[] dest_types;
    private short dest_len;
    private short[] dest_s_sizes;
    private short dest_size;
    private IndexType index;
    private int currentBitMapPos;
    private BitMapFile markedDeleted;

    /**
   * class constructor. set up the index scan.
   * @param index type of the index (B_Index, Hash)
   * @param columnarfile name of the input relation
   * @param indName name of the input index
   * @param types array of types in this relation
   * @param str_sizes array of string sizes (for attributes that are string)
   * @param noInFlds number of fields in input tuple
   * @param noOutFlds number of fields in output tuple
   * @param outFlds fields to project
   * @param selects conditions to apply, first one is primary
   * @param fldNum field number of the indexed field
   * @param indexOnly whether the answer requires only the key or the tuple
   * @exception IndexException error from the lower layer
   * @exception InvalidTypeException tuple type not valid
   * @exception InvalidTupleSizeException tuple size not valid
   * @exception UnknownIndexTypeException index type unknown
   * @exception IOException from the lower layer
   */
  public ColumnIndexScan(
	   IndexType     index,
       Columnarfile columnarfile,
	   final String  indName,  
	   AttrType      types[],      
	   short         str_sizes[],     
	   int           noInFlds,          
	   int           noOutFlds,
       int[] out_indexes,
	   FldSpec       outFlds[],     
	   CondExpr      selects[],  
	   final int     fldNum,
	   final boolean indexOnly
	   ) 
    throws Exception
  {
    _fldNum = fldNum;
      colNo = fldNum-1;
    _noInFlds = noInFlds;
    _types = types;
    _s_sizes = str_sizes;
      outIndexes = out_indexes;
      this.index = index;
    
    AttrType[] Jtypes = new AttrType[noOutFlds];
    short[] ts_sizes;
    Jtuple = new Tuple();
    
    try {
    	ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
    }
    catch (TupleUtilsException e) {
      throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
    }
    catch (InvalidRelation e) {
      throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
    }
     
    _selects = selects;
    perm_mat = outFlds;
    _noOutFlds = noOutFlds;
    tuple1 = new Tuple();
      dest_len = 1;
      dest_types = new AttrType[] {types[colNo]};
      dest_s_sizes = new short[dest_len];
      dest_size = columnarfile.getAttrSizes()[colNo];
      if(types[colNo].attrType==AttrType.attrString) {
          dest_s_sizes = new short[] {columnarfile.getAttrSizes()[colNo]};
      }
    try {
      tuple1.setHdr(dest_len, dest_types, dest_s_sizes);
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile error");
    }
    
    t1_size = tuple1.size();
    index_only = indexOnly;  // added by bingjie miao
    
    try {
        f = columnarfile;
        markedDeleted = columnarfile.getMarkedDeleted();
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile not created");
    }
    
    switch(index.indexType) {
      // linear hashing is not yet implemented
    case IndexType.B_Index:
      // error check the select condition
      // must be of the type: value op symbol || symbol op value
      // but not symbol op symbol || value op value
      try {
	indFile = columnarfile.getBtreeIndex(colNo);
      }
      catch (Exception e) {
	throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
      }
      
      try {
	indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
      }
      catch (Exception e) {
	throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
      }

      break;
    case IndexType.Bitmap:
        // error check the select condition
        // must be of the type: value op symbol || symbol op value
        // but not symbol op symbol || value op value
        try {
            getBitSet();
            currentBitMapPos = 0;
        }
        catch (Exception e) {
            throw new IndexException(e, "ColumnIndexScan.java: BitMapFile exceptions.");
        }

        break;
    case IndexType.None:
    default:
      throw new UnknownIndexTypeException("Only BTree and Bitmap index is supported so far");
      
    }
    
  }

    public ColumnIndexScan(
            IndexType     index,
            Columnarfile columnarfile,
            final String  indName,
            AttrType      types[],
            short         str_sizes[],
            int           noInFlds,
            CondExpr      selects[],
            final int     fldNum
    )
            throws Exception
    {
        _fldNum = fldNum;
        colNo = fldNum-1;
        _noInFlds = noInFlds;
        _types = types;
        _s_sizes = str_sizes;
        this.index = index;


        _selects = selects;
        tuple1 = new Tuple();
        dest_len = 1;
        dest_types = new AttrType[] {types[colNo]};
        dest_s_sizes = new short[dest_len];
        dest_size = columnarfile.getAttrSizes()[colNo];
        if(types[colNo].attrType==AttrType.attrString) {
            dest_s_sizes = new short[] {columnarfile.getAttrSizes()[colNo]};
        }
        try {
            tuple1.setHdr(dest_len, dest_types, dest_s_sizes);
        }
        catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        t1_size = tuple1.size();

        try {
            f = columnarfile;
            markedDeleted = columnarfile.getMarkedDeleted();
        }
        catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch(index.indexType) {
            // linear hashing is not yet implemented
            case IndexType.B_Index:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = columnarfile.getBtreeIndex(colNo);
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.Bitmap:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value

                try {
                    getBitSet();
                    currentBitMapPos = 0;
                }
                catch (Exception e) {
                    throw new IndexException(e, "ColumnIndexScan.java: BitMapFile exceptions.");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree and Bitmap index is supported so far");

        }

    }
  
  /**
   * returns the next tuple.
   * if <code>index_only</code>, only returns the key value 
   * (as the first field in a tuple)
   * otherwise, retrive the tuple and returns the whole tuple
   * @return the tuple
   * @exception IndexException error from the lower layer
   * @exception UnknownKeyTypeException key type unknown
   * @exception IOException from the lower layer
   */
  public Tuple get_next() 
    throws Exception
  {
      if(index.indexType == IndexType.B_Index) {
          return get_btree_next();
      } else {
          return get_bm_next();
      }
  }

    public TID get_next_tid()
            throws Exception
    {
        if(index.indexType == IndexType.B_Index) {
            return get_btree_next_tid();
        } else {
            return get_bm_next_tid();
        }
    }

    /**
     * Returns the btree index - If indexonly is true (Returns the key only) else returns the entire tuple
     * @return
     * @throws Exception
     */
  private Tuple get_btree_next() throws Exception {
      RID rid;
      int unused;
      KeyDataEntry nextentry = null;

      try {
          nextentry = indScan.get_next();
      }
      catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: BTree error");
      }

      while(nextentry != null) {

          rid = ((LeafData)nextentry.data).getData();
          int position = f.getHeapfiles()[colNo].findPosition(rid);
          RID id;
          if(markedDeleted.isClear(position)) {
              if (index_only) {
                  // only need to return the key

                  AttrType[] attrType = new AttrType[1];
                  short[] s_sizes = new short[1];

                  if (_types[_fldNum -1].attrType == AttrType.attrInteger) {
                      attrType[0] = new AttrType(AttrType.attrInteger);
                      try {
                          Jtuple.setHdr((short) 1, attrType, s_sizes);
                      }
                      catch (Exception e) {
                          throw new IndexException(e, "IndexScan.java: Heapfile error");
                      }

                      try {
                          Jtuple.setIntFld(1, ((IntegerKey)nextentry.key).getKey().intValue());
                      }
                      catch (Exception e) {
                          throw new IndexException(e, "IndexScan.java: Heapfile error");
                      }
                  }
                  else if (_types[_fldNum -1].attrType == AttrType.attrString) {

                      attrType[0] = new AttrType(AttrType.attrString);
                      // calculate string size of _fldNum
                      int count = 0;
                      for (int i=0; i<_fldNum; i++) {
                          if (_types[i].attrType == AttrType.attrString)
                              count ++;
                      }
                      s_sizes[0] = _s_sizes[count-1];

                      try {
                          Jtuple.setHdr((short) 1, attrType, s_sizes);
                      }
                      catch (Exception e) {
                          throw new IndexException(e, "IndexScan.java: Heapfile error");
                      }

                      try {
                          Jtuple.setStrFld(1, ((StringKey)nextentry.key).getKey());
                      }
                      catch (Exception e) {
                          throw new IndexException(e, "IndexScan.java: Heapfile error");
                      }
                  }
                  else {
                      // attrReal not supported for now
                      throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                  }

                  return Jtuple;
              }

              // not index_only, need to return the whole tuple
//          rid = ((LeafData)nextentry.data).getData();

              byte tmp[] = new byte[dest_size];
              if(dest_types[0].attrType==AttrType.attrString) {
                  Convert.setStrValue(((StringKey)nextentry.key).getKey(), 0, tmp);
              } else {
                  Convert.setIntValue(((IntegerKey)nextentry.key).getKey().intValue(), 0, tmp);
              }
              tuple1.setFld(1, tmp);

              boolean eval;
              try {
                  eval = PredEval.Eval(_selects, tuple1, null, dest_types, null);
              }
              catch (Exception e) {
                  throw new IndexException(e, "IndexScan.java: Heapfile error");
              }

              if (eval) {
                  // need projection.java
                  try {
//                  int position = f.getHeapfiles()[colNo].findPosition(rid);
                      int j = 1;
                      for(int i : outIndexes) {
                          Heapfile file = f.getHeapfiles()[i];
                          id = file.findRID(position);
                          Tuple tuple = file.getRecord(id);
                          Jtuple.setFld(j, tuple.returnTupleByteArray());
                          j++;
                      }
                  }
                  catch (Exception e) {
                      throw new IndexException(e, "IndexScan.java: Heapfile error");
                  }

                  return Jtuple;
              }
          }

          try {
              nextentry = indScan.get_next();
          }
          catch (Exception e) {
              throw new IndexException(e, "IndexScan.java: BTree error");
          }
      }

      return null;
  }

  /**
   * Returns the next tuple id 
   * @return
   * @throws Exception
   */
    private TID get_btree_next_tid() throws Exception {
        RID rid;
        int unused;
        KeyDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        }
        catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTree error");
        }

        while(nextentry != null) {

            // not index_only, need to return the whole tuple
            rid = ((LeafData)nextentry.data).getData();
            int position = f.getHeapfiles()[colNo].findPosition(rid);
            RID id;
            if(markedDeleted.isClear(position)) {
                byte tmp[] = new byte[dest_size];
                if(dest_types[0].attrType==AttrType.attrString) {
                    Convert.setStrValue(((StringKey)nextentry.key).getKey(), 0, tmp);
                } else {
                    Convert.setIntValue(((IntegerKey)nextentry.key).getKey().intValue(), 0, tmp);
                }
                tuple1.setFld(1, tmp);

                boolean eval;
                try {
                    eval = PredEval.Eval(_selects, tuple1, null, dest_types, null);
                }
                catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: Heapfile error");
                }

                if (eval) {
                    // need projection.java
                    try {
                        TID tid = new TID(_noInFlds, position);
                        return tid;
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                }
            }

            try {
                nextentry = indScan.get_next();
            }
            catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: BTree error");
            }
        }

        return null;
    }

    /**
     * Returns the next tuple of bit map index
     * @return
     * @throws Exception
     */
    private Tuple get_bm_next() throws Exception {
        RID rid;
        int unused;
        int position = -1; //initial
        while(true) {
            position = bitSet.nextSetBit(currentBitMapPos);
            if(position!=-1) {
                if(markedDeleted.isClear(position)) {
                    break;
                }
                currentBitMapPos = position+1;
            } else {
                return null;
            }
        }

        if(position != -1) {
            if (index_only) {
                // only need to return the key

                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1];

                if (_types[_fldNum -1].attrType == AttrType.attrInteger) {
                    attrType[0] = new AttrType(AttrType.attrInteger);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setIntFld(1, _selects[0].operand2.integer);
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                }
                else if (_types[_fldNum -1].attrType == AttrType.attrString) {

                    attrType[0] = new AttrType(AttrType.attrString);
                    // calculate string size of _fldNum
                    int count = 0;
                    for (int i=0; i<_fldNum; i++) {
                        if (_types[i].attrType == AttrType.attrString)
                            count ++;
                    }
                    s_sizes[0] = _s_sizes[count-1];

                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setStrFld(1, _selects[0].operand2.string);
                    }
                    catch (Exception e) {
                        throw new IndexException(e, "IndexScan.java: Heapfile error");
                    }
                }
                else {
                    // attrReal not supported for now
                    throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                }
                currentBitMapPos = position+1;
                return Jtuple;
            }

            // not index_only, need to return the whole tuple

            try {
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
            }
            catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: Heapfile error");
            }
        }

        return null;
    }

    /**
     * Returns the next bit map tuple id 
     * @return
     * @throws Exception
     */
    private TID get_bm_next_tid() throws Exception {
        RID rid;
        int unused;
        int position = -1;
        while(true) {
            position = bitSet.nextSetBit(currentBitMapPos);

            if(position!=-1) {
                if(markedDeleted.isClear(position)) {
                    break;
                }
                currentBitMapPos = position+1;
            } else {
                return null;
            }
        }

        if(position != -1) {
            TID tid = new TID(_noInFlds, position);
            currentBitMapPos = position+1;
            return tid;
        }

        return null;
    }
  /**
   * Cleaning up the index scan, does not remove either the original
   * relation or the index from the database.
   * @exception IndexException error from the lower layer
   * @exception IOException from the lower layer
   */
  public void close() throws IOException, IndexException
  {
    if (!closeFlag) {
      if (indScan instanceof BTFileScan) {
	try {
	  ((BTFileScan)indScan).DestroyBTreeFileScan();
	}
	catch(Exception e) {
	  throw new IndexException(e, "BTree error in destroying index scan.");
	}
      }
      
      closeFlag = true; 
    }
  }

  public BitSet getPositionsOfIndexScan() throws Exception {
	  TID tid=null;
	  BitSet positions = new BitSet();
      while((tid=this.get_next_tid())!=null) {
    	  positions.set(tid.position);
      }
      return positions;
  }

  public void getBitSet() throws Exception {
      List<ValueClass> values = new ArrayList<>();
      AttrType type = f.getAttributeType(colNo);
      // symbol = value
      if (_selects[0].op.attrOperator == AttrOperator.aopEQ
              || _selects[0].op.attrOperator == AttrOperator.aopLE
              || _selects[0].op.attrOperator == AttrOperator.aopGE) {
          if(type.attrType==AttrType.attrString) {
              values.add(new StringValue(_selects[0].operand2.string));
          } else {
              values.add(new IntegerValue(_selects[0].operand2.integer));
          }
      }

      // symbol < value or symbol <= value
      if (_selects[0].op.attrOperator == AttrOperator.aopLT || _selects[0].op.attrOperator == AttrOperator.aopLE) {
          if(type.attrType==AttrType.attrString) {
              String value = _selects[0].operand2.string;
              Set<String> allValues = f.getBitmapValues(colNo);
              for(String str: allValues) {
                if(value.compareTo(str)>0) {
                    values.add(new StringValue(str));
                }
              }
          } else {
              int value = _selects[0].operand2.integer;
              Set<Integer> allValues = f.getBitmapValues(colNo);
              for(Integer other: allValues) {
                  if(value>other) {
                      values.add(new IntegerValue(other));
                  }
              }
          }
      }

      // symbol > value or symbol >= value
      if (_selects[0].op.attrOperator == AttrOperator.aopGT || _selects[0].op.attrOperator == AttrOperator.aopGE) {
          if (type.attrType == AttrType.attrString) {
              String value = _selects[0].operand2.string;
              Set<String> allValues = f.getBitmapValues(colNo);
              for (String str : allValues) {
                  if (value.compareTo(str) < 0) {
                      values.add(new StringValue(str));
                  }
              }
          } else {
              int value = _selects[0].operand2.integer;
              Set<Integer> allValues = f.getBitmapValues(colNo);
              for (Integer other : allValues) {
                  if (value < other) {
                      values.add(new IntegerValue(other));
                  }
              }
          }
      }

      if (_selects[0].op.attrOperator == AttrOperator.aopNE) {
          if (type.attrType == AttrType.attrString) {
              String value = _selects[0].operand2.string;
              Set<String> allValues = f.getBitmapValues(colNo);
              for (String str : allValues) {
                  if (value.compareTo(str) != 0) {
                      values.add(new StringValue(str));
                  }
              }
          } else {
              int value = _selects[0].operand2.integer;
              Set<Integer> allValues = f.getBitmapValues(colNo);
              for (Integer other : allValues) {
                  if (value != other) {
                      values.add(new IntegerValue(other));
                  }
              }
          }
      }

      if(values.size()==1) {
          bitSet = f.getBitmapIndex(colNo, values.get(0)).getBitSet();
      } else {
          bitSet = new BitSet();
          for(ValueClass value : values) {
              bitSet.or(f.getBitmapIndex(colNo, value).getBitSet());
          }
      }
  }
}

package tests2;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.*;
import heap.Tuple;
import index.ColumnIndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.File;

/**
 * Created by rubinder on 3/23/18.
 */
public class IndexTest {
    public static void main(String[] args) {
        try {
            String columndbname = "db1";
            String columnarfilename = "cf1";
//            String columnname = args[2];
//            String indextype = args[3];

            int startReadCount = PCounter.rcounter;
            int startWriteCount = PCounter.wcounter;

            if(!new File(columndbname).exists()) {
                throw new Exception("Database does not exist.");
            } else {
                SystemDefs columnDb = new SystemDefs(columndbname, 0, GlobalConst.MINIBASE_BUFFER_POOL_SIZE, null);
            }

            Columnarfile columnarfile = new Columnarfile(columnarfilename);


            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
            expr[0].next  = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            expr[0].operand2.string = "Business";

            expr[1] = null;

            FldSpec [] Sprojection = new FldSpec[4];
            Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
            Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
            Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
//            Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

            AttrType[] attrTypes = new AttrType[] {new AttrType(AttrType.attrString)};
            short[] s_sizes = new short[] {25};
            int out_indexes[] = new int[] {0,1,2,3};
            /*ColumnIndexScan indexScan = new ColumnIndexScan(new IndexType(IndexType.B_Index), columnarfile,
                    "cf1.btree.0",
                    columnarfile.getAttributeTypes(),
                    columnarfile.getStringSizes(),
                    4, 4, out_indexes,
                    Sprojection, expr, 1, false);*/
            ColumnIndexScan indexScan = new ColumnIndexScan(new IndexType(IndexType.Bitmap), columnarfile,
                    "cf1.bm.0.Business",
                    columnarfile.getAttributeTypes(),
                    columnarfile.getStringSizes(),
                    4, 4, out_indexes,
                    Sprojection, expr, 1, false);
            Tuple tuple = null;
            int count = 0;
            while((tuple=indexScan.get_next())!=null) {
                System.out.println(tuple.getStrFld(1));
                count++;
            }
            System.out.println(count);


            //Flushing data to disk
//            SystemDefs.JavabaseBM.flushAllPages();

            int endReadCount = PCounter.rcounter;
            int endWriteCount = PCounter.wcounter;
            System.out.println("Read Page Count: "+(endReadCount-startReadCount));
            System.out.println("Write Page Count: "+(endWriteCount-startWriteCount));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

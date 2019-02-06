package input;

import columnar.Columnarfile;
import columnar.TupleScan;
import diskmgr.PCounter;
import global.*;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.File;

/**
 * Created by rubinder on 3/23/18.
 */
public class ScanTest {
    public static void main(String[] args) {
        try {
            String columndbname = "CDB2";
            String columnarfilename = "CF1";
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


/*            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
            expr[0].next  = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),3);
            expr[0].operand2.integer = 0;

            expr[1] = null;

            FldSpec [] Sprojection = new FldSpec[2];
            Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

            ColumnarFileScan scan = new ColumnarFileScan(columnarfilename,
                    columnarfile.getAttributeTypes(),
                    columnarfile.getStringSizes(),
                    columnarfile.getFieldCount(),
                    2,
                    Sprojection,
                    expr);*/
            Tuple tuple = null;
            TID tid = new TID(columnarfile.getFieldCount());
            TupleScan scan = columnarfile.openTupleScan();
            while((tuple=scan.getNext(tid))!=null) {
                System.out.println(tuple.getStrFld(1)
                        +", "+tuple.getStrFld(2)
                        +", "+tuple.getIntFld(3)
                        +", "+tuple.getIntFld(4));
            }

            //Flushing data to disk
            SystemDefs.JavabaseBM.flushAllPages();

            int endReadCount = PCounter.rcounter;
            int endWriteCount = PCounter.wcounter;
            System.out.println("Read Page Count: "+(endReadCount-startReadCount));
            System.out.println("Write Page Count: "+(endWriteCount-startWriteCount));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

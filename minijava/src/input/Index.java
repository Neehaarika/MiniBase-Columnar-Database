package input;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.SystemDefs;

import java.io.*;

/**
 * Reprsents an index query where an index gets created 
 */
public class Index {
    public void createIndex(String[] args) {
        try {
            if (args.length < 4) {
                throw new Exception("Invalid number of attributes.");
            } else {
                PCounter.initialize();
                String columndbname = args[0];
                String columnarfilename = args[1];
                String columnname = args[2];
                String indextype = args[3];

                if(!(indextype.equalsIgnoreCase("BTREE") || indextype.equalsIgnoreCase("BITMAP"))) {
                    throw new Exception("index type invalid.");
                }

                if(!new File(columndbname).exists()) {
                    throw new Exception("Database does not exist.");
                } else {
                    SystemDefs columnDb = new SystemDefs(columndbname, 0, 1024, null);
                }

                Columnarfile columnarfile = new Columnarfile(columnarfilename);
                int colIndex = columnarfile.colNameToIndex(columnname);

                int startReadCount = PCounter.rcounter;
                int startWriteCount = PCounter.wcounter;
                if(indextype.equalsIgnoreCase("BTREE")) {
                    columnarfile.createBTreeIndex(colIndex);
                } else {
                    columnarfile.createBitMapIndex(colIndex);
                }

                //Flushing all written data to disk.
                try {
                    SystemDefs.JavabaseBM.flushAllPages();
                } catch (Exception e) {
//                    e.printStackTrace();
                }

                int endReadCount = PCounter.rcounter;
                int endWriteCount = PCounter.wcounter;
                System.out.println("Read Page Count: "+(endReadCount-startReadCount));
                System.out.println("Write Page Count: "+(endWriteCount-startWriteCount));
                System.out.println("Read Pages: "+PCounter.readPages);
                System.out.println("Wrote Pages: "+PCounter.writePages);
//                System.out.println("Pinned Pages: "+PCounter.currentlyPinnedPages);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package tests;

import java.io.IOException;

import btree.BT;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.PinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

public class IndexPrint {

	public static void main(String args[]) throws GetFileEntryException, PinPageException, ConstructPageException, IteratorException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException, IOException{
		String treefilename = "CF1.btree.3";
		BTreeFile bTreeFile = new BTreeFile(treefilename);
		BT.printBTree(bTreeFile.getHeaderPage());
		
	}
}

package bitmap;

/**
 * Created by sidharth on 3/15/18.
 */

import btree.*;
import chainexception.ChainException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BM implements GlobalConst {

	private static final Logger LOGGER = Logger.getLogger(BM.class.getName());
	public static final int DPFIXED = 20;

	/**
	 * It compares two keys.
	 * 
	 * @param key1
	 *            the first key to compare. Input parameter.
	 * @param key2
	 *            the second key to compare. Input parameter.
	 * @return return negative if key1 less than key2; positive if key1 bigger than
	 *         key2; 0 if key1=key2.
	 * @exception KeyNotMatchException
	 *                key is not IntegerKey or StringKey class
	 */
	public final static int keyCompare(KeyClass key1, KeyClass key2) throws KeyNotMatchException {
		if ((key1 instanceof IntegerKey) && (key2 instanceof IntegerKey)) {

			return (((IntegerKey) key1).getKey()).intValue() - (((IntegerKey) key2).getKey()).intValue();
		} else if ((key1 instanceof StringKey) && (key2 instanceof StringKey)) {
			return ((StringKey) key1).getKey().compareTo(((StringKey) key2).getKey());
		}

		else {
			throw new KeyNotMatchException(null, "key types do not match");
		}
	}

	/**
	 * This function takes the BitMapHaaderPage and the bitSet and saves it into the BMIndexPage page.
	 * 
	 * @param bitMapHeaderPage
	 * @param bitSet
	 * @return
	 * @throws IndexInsertRecException
	 * @throws IOException
	 * @throws InsertRecException 
	 * @throws UnpinPageException 
	 */
	public final static RID insertBitSet(BMIndexPage bitMapHeaderPage, BitSet bitSet, HashMap<Integer, Integer> offsetToPage)
			throws IndexInsertRecException, IOException, InsertRecException, UnpinPageException, Exception {
		RID rid = null;
		if(bitMapHeaderPage == null || bitSet == null || bitMapHeaderPage.getNextPage().pid != new PageId(INVALID_PAGE).pid) {
			return null;
		}
		// Convert incoming BitSet to Byte Array for saving it into the database.
		byte[] bitSetByteArray = bitSet.toByteArray();
		int sizeOfBitSetByteArray = bitSetByteArray.length;
		int pageIndex = 0;
		// Each page is of size 1024 in which 24 is fixed for page parameters.And
		// another 4 bytes we consider for the size of the slot.So each page we only use
		// 996 bytes.
		int availableSpaceInEachPage = MAX_SPACE - DPFIXED - 4;

		// The bitSet Byte array is divided into array of byte[] by the size of each
		// page such that it can be stored across multiple pages.
		byte[][] bitSetBitArrays = divideByteArraysByPageSize(bitSetByteArray, availableSpaceInEachPage);
		System.out.println(
				"Size while writing:" + sizeOfBitSetByteArray + "Pages to be written:" + bitSetBitArrays.length);
		try {
			// Two new pages are created for current page and next page, as we follow a
			// linked list implementation.
			PageId prevPageId = new PageId(INVALID_PAGE);
			BMIndexPage prevBitMapPage = null;
			BMIndexPage currentBitMapPage = bitMapHeaderPage;
			// Records are inserted, as well as the next and previous page is set.
			rid = currentBitMapPage.insertRecord(bitSetBitArrays[0], prevPageId, new PageId(INVALID_PAGE));
			offsetToPage.put(pageIndex, currentBitMapPage.getCurPage().pid);
			if (rid == null) {
				System.out.println("Insertion Record Failed");
				throw new InsertRecException();
			}
			prevPageId = currentBitMapPage.getCurPage();
			prevBitMapPage = currentBitMapPage;
			LOGGER.log(Level.FINE, "Inserting Data  0 " + "Pageno: " + rid.pageNo);
			RID previousRid = rid;
			RID ridTemp = null;
			for (int i = 1; i < bitSetBitArrays.length; i++) {
				currentBitMapPage = new BMIndexPage();
				LOGGER.log(Level.FINE, "Inserting Data" + i + "Pageno" + currentBitMapPage.getCurPage());
				pageIndex++;
				offsetToPage.put(pageIndex, currentBitMapPage.getCurPage().pid);
				// When populating value in the last page, we should make sure that the next
				// page points to an INVALID_PAGE.
				ridTemp = currentBitMapPage.insertRecord(bitSetBitArrays[i], prevPageId,
						new PageId(INVALID_PAGE));
				if (ridTemp == null) {
					System.out.println("Page Generation Failed");
					throw new ConstructPageException();
				}
				//Handling pointers for next page
				prevBitMapPage.setNextPage(currentBitMapPage.getCurPage());
				unpinPage(prevPageId, true /* = DIRTY */ );

				prevPageId = currentBitMapPage.getCurPage();
				prevBitMapPage = currentBitMapPage;
				previousRid = ridTemp;
			}
			unpinPage(prevPageId, true /* = DIRTY */ );
		} catch (ConstructPageException e1) {
			LOGGER.log(Level.SEVERE, e1.toString(), e1);
			e1.printStackTrace();
		}
		return rid;
	}

	private static void unpinPage(PageId pageno, boolean dirty) 
		    throws UnpinPageException
		    {
		      try{
		        SystemDefs.JavabaseBM.unpinPage(pageno, dirty);  
		      }
		      catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		      }  
		    }

	private static Page pinPage(PageId pageno)
			throws PinPageException
	{
		try {
			Page page=new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			return page;
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e,"");
		}
	}

	private static void freePage(PageId pageno)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"BM.java: freePage() failed");
		}

	} // end of freePage
	 

	/**
	 * This function reads the byteArray from the BitMapIndexPages as a byte array,
	 * merge it and return it as an bitSet Object.
	 * 
	 * @param bitMapHeaderPage
	 * @return
	 * @throws IOException
	 * @throws ChainException 
	 */
	public final static BitSet readBitSet(BMIndexPage bitMapHeaderPage, HashMap<Integer, Integer> offsetToPage)
			throws IOException, ChainException {
		BitSet bitSetBitArray = null;
		int availableSpaceInEachPage = MAX_SPACE - DPFIXED - 4;
		try {
			int totalSize = 0;
			int pageIndex = 0;
			List<byte[]> list = new ArrayList<>();
			BMIndexPage currentBitMapPage = bitMapHeaderPage;
			byte[] data = currentBitMapPage.getRecord(currentBitMapPage.firstRecord()).returnTupleByteArray();
			totalSize += data.length;
			list.add(data);
			offsetToPage.put(pageIndex, currentBitMapPage.getCurPage().pid);
			PageId prevPageId = currentBitMapPage.getCurPage();
			while(currentBitMapPage.getNextPage().pid!=INVALID_PAGE) {
				pageIndex++;
				currentBitMapPage = new BMIndexPage(currentBitMapPage.getNextPage());
				unpinPage(prevPageId, false);
				data = currentBitMapPage.getRecord(currentBitMapPage.firstRecord()).returnTupleByteArray();
				totalSize += data.length;
				list.add(data);
				offsetToPage.put(pageIndex, currentBitMapPage.getCurPage().pid);
				prevPageId = currentBitMapPage.getCurPage();
			}
			unpinPage(prevPageId, false);
			// Merge all the byte[] we got from all the pages into one single byte[].
			byte[] mergedByteArray = mergeArray(list, totalSize);
			// Convert the byte[] back into an bitSet object.
			
			bitSetBitArray = BitSet.valueOf(mergedByteArray);
		} catch (ConstructPageException | InvalidSlotNumberException e1) {
			e1.printStackTrace();
			throw e1;
		}
//		System.out.println(bitSetBitArray);
		return bitSetBitArray;
	}

	/**
	 * This function updates the corresponding bitSet across pages,deletes the
	 * records and later on inserts the new data on to the same set of pages, and
	 * adds new pages if the data is bigger than the existing data as well as it
	 * deletes if the new data is smaller than the existing ones.
	 * 
	 * 
	 * Three conditions are taken care: 
	 * 
	 * 1) If the number of pages needed for the
	 * existing bitSet is same as off that of the one for which is going to be
	 * updated.Then we just delete the old records and update them with the new ones.
	 * 
	 * 2) If the number of pages needed for the existing bitSet is more as off that
	 * of the one for which is going to be updated.In this case we first fill the
	 * same pages by first deleting and then putting in the new value, and later on
	 * create new pages and add records as per the requirement till all the byte
	 * array is inserted.
	 * 
	 * 3)If the number of pages needed for the existing bitSet is less as off that
	 * of the one for which is going to be updated.Then we fill the required pages,
	 * and delete the records from rest of the pages.
	 * 
	 * @param bitMapHeaderPage
	 * @param bitSet
	 * @throws IndexInsertRecException
	 * @throws IOException
	 * @throws ConstructPageException 
	 * @throws InvalidSlotNumberException 
	 * @throws InsertRecException 
	 * @throws UnpinPageException 
	 */
	public final static void updateBitSet(BMIndexPage bitMapHeaderPage, BitSet bitSet, HashMap<Integer, Integer> offsetToPage)
			throws IndexInsertRecException, IOException, InsertRecException, UnpinPageException, Exception {
		RID rid = null;
		if(bitMapHeaderPage == null || bitSet == null) {
			return;
		}
		// Convert incoming BitSet to Byte Array for saving it into the database.
		byte[] bitSetByteArray = bitSet.toByteArray();
		int sizeOfBitSetByteArray = bitSetByteArray.length;
		int pageIndex = 0;
		// Each page is of size 1024 in which 24 is fixed for page parameters.And
		// another 4 bytes we consider for the size of the slot.So each page we only use
		// 996 bytes.
		int availableSpaceInEachPage = MAX_SPACE - DPFIXED - 4;

		// The bitSet Byte array is divided into array of byte[] by the size of each
		// page such that it can be stored across multiple pages.
		byte[][] bitSetBitArrays = divideByteArraysByPageSize(bitSetByteArray, availableSpaceInEachPage);
		System.out.println(
				"Size while writing:" + sizeOfBitSetByteArray + "Pages to be written:" + bitSetBitArrays.length);
		try {
			// Two new pages are created for current page and next page, as we follow a
			// linked list implementation.
			PageId prevPageId = new PageId(INVALID_PAGE);
			BMIndexPage prevBitMapPage = null;
			BMIndexPage currentBitMapPage = bitMapHeaderPage;
			// Records are updated, as well as the next and previous page is set.
			rid = currentBitMapPage.firstRecord();
			Tuple atuple = currentBitMapPage.returnRecord(rid);
			Tuple newtuple = new Tuple(bitSetBitArrays[0], 0, bitSetBitArrays[0].length);
			atuple.tupleCopy(newtuple);

			prevPageId = currentBitMapPage.getCurPage();
			prevBitMapPage = currentBitMapPage;
			LOGGER.log(Level.FINE, "Updating Data  0 " + "Pageno: " + rid.pageNo);
			for (int i = 1; i < bitSetBitArrays.length; i++) {
				currentBitMapPage = new BMIndexPage(prevBitMapPage.getNextPage());
				LOGGER.log(Level.FINE, "Updating Data" + i + "Pageno" + currentBitMapPage.getCurPage());
				pageIndex++;
				// When populating value in the last page, we should make sure that the next
				// page points to an INVALID_PAGE.
				rid = currentBitMapPage.firstRecord();
				Tuple atuple1 = currentBitMapPage.returnRecord(rid);
				Tuple newtuple1 = new Tuple(bitSetBitArrays[i], 0, bitSetBitArrays[i].length);
				atuple1.tupleCopy(newtuple1);
				//Handling pointers for next page
				prevBitMapPage.setNextPage(currentBitMapPage.getCurPage());
				unpinPage(prevPageId, true /* = DIRTY */ );

				prevPageId = currentBitMapPage.getCurPage();
				prevBitMapPage = currentBitMapPage;
			}
			//Freeing all unused pages
			if(prevBitMapPage.getNextPage().pid!=INVALID_PAGE) {
				PageId nextPid = prevBitMapPage.getNextPage();;
				while (nextPid.pid!=INVALID_PAGE) {
					pageIndex++;
					offsetToPage.remove(pageIndex);
					currentBitMapPage = new BMIndexPage(nextPid);
					nextPid = currentBitMapPage.getNextPage();
					unpinPage(currentBitMapPage.getCurPage(), false/*undirty*/);
					freePage(currentBitMapPage.getCurPage());
				}
			}
			prevBitMapPage.setNextPage(new PageId(INVALID_PAGE));
			unpinPage(prevPageId, true /* = DIRTY */ );
		} catch (ConstructPageException e1) {
			LOGGER.log(Level.SEVERE, e1.toString(), e1);
			e1.printStackTrace();
		}
	}


	/**
	 * This function takes the given parameters, deletes the first record from each
	 * page and inserts data into the first record as well.
	 * 
	 * @param bitSetByteArrays
	 * @param sizeOfUpdatedBitSet
	 * @param nextBitMapPage
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws InsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException 
	 */
	public static void deleteAndInsertNewRecords(byte[][] bitSetByteArrays, int sizeOfUpdatedBitSet,
			BMIndexPage nextBitMapPage)
			throws IOException, InvalidSlotNumberException, InsertRecException, ConstructPageException, UnpinPageException {
		int i = 0;
		for (int j = 0; j < sizeOfUpdatedBitSet; j++) {
			nextBitMapPage.deleteRecord(nextBitMapPage.firstRecord());
			RID temp = nextBitMapPage.insertRecord(bitSetByteArrays[i++]);
			unpinPage(nextBitMapPage.getCurPage(), true /* = DIRTY */ );
			//System.out.println("Unpinned page:"+nextBitMapPage.getCurPage());
			if (temp == null) {
				System.out.println("Insertion Record Failed");
				throw new InsertRecException();
			}
			if (j != sizeOfUpdatedBitSet - 1) {
				nextBitMapPage = new BMIndexPage(nextBitMapPage.getNextPage());
			}
		}
	}

	/**
	 * This function divides a byte[] into an array of byte[] on the basis of the
	 * page size given as an input.
	 * 
	 * @param inputByteArray
	 * @param pageSize
	 * @return
	 */
	public static byte[][] divideByteArraysByPageSize(byte[] inputByteArray, int pageSize) {
		byte[][] resultingArray = new byte[(int) Math.ceil(inputByteArray.length / (double) pageSize)][pageSize];
		int start = 0;
		for (int i = 0; i < resultingArray.length; i++) {
			if (start + pageSize > inputByteArray.length) {
				System.arraycopy(inputByteArray, start, resultingArray[i], 0, inputByteArray.length - start);
			} else {
				System.arraycopy(inputByteArray, start, resultingArray[i], 0, pageSize);
			}
			start += pageSize;
		}
		return resultingArray;
	}

	/**
	 * This takes multiple byte arrays and merges in to a single one.
	 * 
	 * @param byteArrays
	 * @return
	 */
	public static byte[] mergeArray(List<byte[]> byteArrays, int lengthOfTheCombinedArray) {
		byte[] combinedByteArray = new byte[lengthOfTheCombinedArray];
		int index = 0;
		for (int i = 0; i < byteArrays.size(); i++) {
			System.arraycopy(byteArrays.get(i), 0, combinedByteArray, index, byteArrays.get(i).length);
			index += byteArrays.get(i).length;
		}
		return combinedByteArray;
	}

}

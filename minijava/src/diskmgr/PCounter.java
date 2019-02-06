package diskmgr;

import global.PageId;

import java.util.*;

/**
 * Created by rubinder on 3/16/18.
 */
public class PCounter {
	public static int rcounter = 0;
	public static int wcounter = 0;
	public static Map<Integer, Integer> currentlyPinnedPages = new TreeMap<>();
	public static Map<Integer, Integer> readPages = new TreeMap<>();
	public static Map<Integer, Integer> writePages = new TreeMap<>();

	public static void initialize() {
		rcounter = 0;
		wcounter = 0;
		currentlyPinnedPages = new HashMap<>();
		readPages = new TreeMap<>();
		writePages = new TreeMap<>();
	}

	public static void readIncrement(int pid) {
		rcounter++;
		if(readPages.containsKey(pid)) {
			readPages.put(pid, readPages.get(pid)+1);
		} else {
			readPages.put(pid, 1);
		}
	}

	public static void writeIncrement(int pid) {
		wcounter++;
		if(writePages.containsKey(pid)) {
			writePages.put(pid, writePages.get(pid)+1);
		} else {
			writePages.put(pid, 1);
		}
	}

	public static void addToPinpage(PageId pageId) {
		if(currentlyPinnedPages.containsKey(pageId.pid)) {
			currentlyPinnedPages.put(pageId.pid, currentlyPinnedPages.get(pageId.pid)+1);
		} else {
			currentlyPinnedPages.put(pageId.pid, 1);
		}
	}

	public static void removePinpage(PageId pageId) {
		if(currentlyPinnedPages.containsKey(pageId.pid)) {
			int pincount = currentlyPinnedPages.get(pageId.pid)-1;
			if(pincount<=0) {
				currentlyPinnedPages.remove(pageId.pid);
			} else {
				currentlyPinnedPages.put(pageId.pid, pincount);
			}
		}
	}

	public static int getReleventReadCount() {
		int count = 0;
		for(Integer pid: readPages.keySet()) {
			if(pid>128) {
				count+= readPages.get(pid);
			}
		}
		return count;
	}
}

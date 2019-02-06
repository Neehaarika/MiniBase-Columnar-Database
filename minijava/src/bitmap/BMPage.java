package bitmap;

/**
 * Created by sidharth on 3/15/18.
 */

import diskmgr.Page;
import heap.HFPage;

public class BMPage extends HFPage{
	
	BMPage(){
		super();
	}
	
	BMPage(Page page){
		super(page);
	}
	

    public void openBMpage(Page apage) {
    	super.openHFpage(apage);
    }


	public byte[] getBMpageArray() {
		return super.getHFpageArray();
	}
	
	public void writeBMPageArray(byte[] byteArray) {
		data = byteArray;
	}

}

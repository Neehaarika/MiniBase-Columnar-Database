package bitmap;

/**
 * Created by sidharth on 3/15/18.
 */

import btree.ConstructPageException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;

import java.io.IOException;

public class BMIndexPage extends BMPage {

	public BMIndexPage(PageId pageno) throws ConstructPageException {
		super();
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
		} catch (Exception e) {
			throw new ConstructPageException(e, "pinpage failed");
		}
	}

	/** associate the BMPage instance with the Page instance */
	public BMIndexPage(Page page) {

		super(page);
	}

	public BMIndexPage() throws ConstructPageException {
		super();
		try {
			Page apage = new Page();
			PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
			if (pageId == null)
				throw new ConstructPageException(null, "new page failed");
			this.init(pageId, apage);

		} catch (Exception e) {
			throw new ConstructPageException(e, "construct header page failed");
		}
	}

	public RID insertRecord(byte[] record, PageId previousPage, PageId nextPage) throws IOException {
		RID rid = super.insertRecord(record);
		super.setNextPage(nextPage);
		super.setPrevPage(previousPage);
		return rid;
	}
	
}

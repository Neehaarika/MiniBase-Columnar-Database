package Utilities;

import btree.*;
import global.*;
import heap.*;

public class KeyHelper {

	public static KeyClass getKeyClass(AttrType attributrType, Tuple record) {
		KeyClass key = null;

		try {
			switch (attributrType.attrType) {

			case AttrType.attrInteger:

				key = new IntegerKey(record.getIntFld(1)); // one column - hence only only one field. Field Number is 1.
				break;

			case AttrType.attrString:

				key = new StringKey(record.getStrFld(1));
				break;
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return key;
	}
}

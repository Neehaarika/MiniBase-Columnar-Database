package global;

import java.util.Arrays;

/**
 * Created by rubinder on 3/14/18.
 */
public class TID {

    public int numRIDs;
    public int position;
    public RID[] recordIDs;

    public TID(int numRIDs) {
        this.numRIDs = numRIDs;
        recordIDs = new RID[numRIDs];
        position = -1;
    }

    public TID(int numRIDs, int position) {
        this.numRIDs = numRIDs;
        this.position = position;
    }

    public TID(int numRIDs, int position, RID[] recordIDs) {
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = recordIDs;
    }

    public void copyTid (TID tid) {
        numRIDs = tid.numRIDs;
        position = tid.position;
        recordIDs = tid.recordIDs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TID tid = (TID) o;

        if (numRIDs != tid.numRIDs) return false;
        if (position != tid.position) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(recordIDs, tid.recordIDs);

    }

    public void writeToByteArray(byte[] array, int offset) throws java.io.IOException {
        Convert.setIntValue ( numRIDs, offset, array);
        offset = offset+4;
        Convert.setIntValue ( position, offset, array);
        offset = offset+4;
        for (RID rid: recordIDs) {
            rid.writeToByteArray(array, offset);
            offset += 8;
        }
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setRID(int column, RID recordID) {
        this.recordIDs[column] = recordID;
    }

    @Override
    public int hashCode() {
        int result = numRIDs;
        result = 31 * result + position;
        result = 31 * result + Arrays.hashCode(recordIDs);
        return result;
    }
    
    public RID[] getRecordIds() {
    	return this.recordIDs;
    }
}

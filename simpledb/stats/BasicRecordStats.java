package simpledb.stats;

/**
 * Created by Daniel on 12/05/16.
 */
public class BasicRecordStats {

    private int readRecord;

    private int writtenRecord;

    private int readFieldsRecord;

    private int writtenFieldsRecord;


    public BasicRecordStats() {
        this.readFieldsRecord = 0;
        this.readRecord = 0;
        this.writtenRecord = 0;
        this.writtenFieldsRecord = 0;
    }

    public int getReadRecord() {
        return readRecord;
    }

    public void updateReadRecord() {
        this.readRecord++;
    }

    public int getWrittenRecord() {
        return writtenRecord;
    }

    public void updateWrittenRecord() {
        this.writtenRecord++;
    }

    public int getWrittenFieldsRecord() {
        return writtenFieldsRecord;
    }

    public void updateWrittenFieldsRecord() {
        this.writtenFieldsRecord++;
    }

    public int getReadFieldsRecord() {
        return readFieldsRecord;
    }

    public void updateReadFieldsRecord() {
        this.readFieldsRecord++;
    }

    @Override
    public String toString() {
        return "Record read: "+ readRecord + ", Record written: "+ writtenRecord+
                ", Fields read: "+ readFieldsRecord + ", Fields written: "+ writtenFieldsRecord;
    }
}

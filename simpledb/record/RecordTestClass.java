package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.stats.BasicRecordStats;
import simpledb.tx.Transaction;

import java.util.Map;
import java.util.Random;

/**
 * Created by Daniel on 12/05/16.
 */
public class RecordTestClass {

    public static void main(String[] args) {

        SimpleDB.init("studentdb", "LRU");

        // analogous to the connection
        Transaction tx = new Transaction();

        //table schema
        Schema sch = new Schema();
        sch.addStringField("nome", 20);
        sch.addIntField("eta");

        String tblname = "people_test";
        TableInfo ti = new TableInfo(tblname, sch);
        RecordFile rf = new RecordFile(ti, tx);

        // Inserimento delle ennuple
        insertRecords(rf, 10000);
        printStats(rf);
        rf.resetStats();

        // Posiziono il cursone sul primo record
        rf.beforeFirst();
        rf.next();
        rf.resetStats();

        // Lettura di tutti i record
        readRecords(rf, 10000);
        printStats(rf);
        rf.resetStats();

        // Posiziono il cursone sul primo record
        rf.beforeFirst();
        rf.next();
        rf.resetStats();

        deleteRecords(rf, 10000);
        printStats(rf);
        rf.resetStats();

        // Posiziono il cursone sul primo record
        rf.beforeFirst();
        rf.next();
        rf.resetStats();

        scanRecords(rf, 10000);
        printStats(rf);
        rf.resetStats();

        // Posiziono il cursone sul primo record
        rf.beforeFirst();
        rf.next();
        rf.resetStats();

        // Inserimento delle ennuple
        insertRecords(rf, 7000);
        printStats(rf);
        rf.resetStats();

        // Posiziono il cursone sul primo record
        rf.beforeFirst();
        rf.next();
        rf.resetStats();

        // Lettura di record
        readRecords(rf, 12000);
        printStats(rf);
        rf.resetStats();

        rf.close();
    }

    private static void scanRecords(RecordFile rf, int length) {
        long startTime = System.currentTimeMillis();
        do  {
            int var = rf.getInt("eta");
            var *= 2;
        } while(rf.next());
        long stopTime = System.currentTimeMillis();
        System.out.println(stopTime - startTime + "ms");
    }

    private static void deleteRecords(RecordFile rf, int length) {
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < length; i++) {
            if (i % 2 == 0) {
                rf.delete();
            }
            rf.next();
        }

        long stopTime = System.currentTimeMillis();
        System.out.println(stopTime - startTime + "ms");
    }

    private static void readRecords(RecordFile rf, int length) {
        long startTime = System.currentTimeMillis();
        do {
            rf.getString("nome");
            rf.getInt("eta");
            rf.next();
        } while (rf.next());
        long stopTime = System.currentTimeMillis();
        System.out.println(stopTime - startTime + "ms");
    }

    private static void insertRecords(RecordFile rf, int length) {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < length; i++) {
            rf.insert();
            rf.setString("nome", RecordTestClass.randomStringGenerator());
            rf.setInt("eta", RecordTestClass.randomNumberGenerator(15, 60));
        }
        long stopTime = System.currentTimeMillis();
        System.out.println(stopTime - startTime + "ms");
    }

    private static void printStats(RecordFile rf) {
        Map<RID, BasicRecordStats> stats = rf.getStatsRecord();
        System.out.println("SIZE: " + stats.keySet().size());
        int readRecord = 0;
        int writtenRecord = 0;
        for (RID currentKey : stats.keySet()) {
            readRecord += stats.get(currentKey).getReadRecord();
            writtenRecord += stats.get(currentKey).getWrittenRecord();
        }
        System.out.println("Read Records: " + readRecord + ", Written Records: " + writtenRecord);
    }

    public static int randomNumberGenerator(int leftLimit, int rightLimit) {
        Random r = new Random();
        return r.nextInt(rightLimit - leftLimit) + leftLimit;
    }

    public static String randomStringGenerator() {
        String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();
        Random rnd = new Random();
        int varlength = rnd.nextInt(20) + 1;
        while (sb.length() < varlength) {
            int index = (int) (rnd.nextFloat() * alphabet.length());
            sb.append(alphabet.charAt(index));
        }
        String randString = sb.toString();
        return randString;
    }
}
package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>hashjoin</i> operator.
 *
 * @author Daniel Morales
 */
public class HashJoinPlan implements Plan {

    private Transaction tx;
    private int blocksAccessed;
    private String fldnameRHS;
    private String fldnameLHS;
    private Schema sch;
    private Plan lhs;
    private Plan rhs;

    /**
     * Creates an hashjoin plan for the two specified queries.
     * The smalest relation must be materialized,
     *
     * @param p1       the first query plan
     * @param p2       the second query plan
     * @param fldname1 the first join field
     * @param fldname2 the second join field
     * @param tx       the calling transaction
     */
    public HashJoinPlan(Plan p1, Plan p2, String fldname1, String fldname2, Transaction tx) {
        sch = new Schema();
        int p1BlocksAccessed = p1.blocksAccessed();
        int p2BlocksAccessed = p2.blocksAccessed();
        if (p1BlocksAccessed < p2BlocksAccessed) {
            rhs = p2;
            lhs = p1;
            fldnameLHS = fldname2;
            fldnameRHS = fldname1;
            blocksAccessed = p1BlocksAccessed;
        } else {
            rhs = p1;
            lhs = p2;
            fldnameLHS = fldname2;
            fldnameRHS = fldname1;
            blocksAccessed = p2BlocksAccessed;
        }
        sch.addAll(rhs.schema());
        sch.addAll(lhs.schema());
        this.tx = tx;
    }


    @Override
    public Scan open() {
        int buckets = BufferNeeds.bestRoot(blocksAccessed);
        TempTable[] tempLHS = splitIntoBuckets(buckets, lhs, fldnameLHS, sch.type(fldnameLHS));
        TempTable[] tempRHS = splitIntoBuckets(buckets, rhs, fldnameRHS, sch.type(fldnameRHS));
        return new HashJoinScan(tempLHS, tempRHS, fldnameLHS, fldnameRHS, tx);
    }

    private TempTable[] splitIntoBuckets(int buckets, Plan p, String fld, int type) {
        TempTable[] temp = new TempTable[buckets];
        Schema schema = p.schema();
        Scan src = p.open();
        src.beforeFirst();
        while (src.next()) {
            int i;
            int val;
            val = (type == 4) ? src.getInt(fld): src.getString(fld).hashCode();
            i = hash(val, buckets);
            TempTable t = temp[i];
            if(t == null)
                t = new TempTable(schema, tx);
            UpdateScan sc = t.open();
            copy(src, sc, schema);
            temp[i] = t;
            sc.close();
        }
        return temp;

    }

    private int hash(int val, int buckets) {
        return val % buckets;
    }

    private void copy(Scan src, UpdateScan dest, Schema schema) {
        dest.insert();
        for (String fldname : schema.fields()) {
            dest.setVal(fldname, src.getVal(fldname));
        }
    }


    @Override
    public int blocksAccessed() {
        return lhs.blocksAccessed() + rhs.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        int maxvals = Math.max(lhs.distinctValues(fldnameLHS),
                rhs.distinctValues(fldnameRHS));
        return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
    }

    @Override
    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname))
            return lhs.distinctValues(fldname);
        else
            return rhs.distinctValues(fldname);

    }

    @Override
    public Schema schema() {
        return sch;
    }
}

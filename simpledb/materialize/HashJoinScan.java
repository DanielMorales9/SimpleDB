package simpledb.materialize;

import simpledb.multibuffer.MultiBufferProductScan;
import simpledb.query.*;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class HashJoinScan implements Scan {

    private TempTable[] lhs;
    private TempTable[] rhs;
    private int currentBucket;
    private Transaction tx;
    private Predicate pred;
    private SelectScan sc;

    public HashJoinScan(TempTable[] tempLHS, TempTable[] tempRHS,
                        String lhsFldname, String rhsFldname, Transaction tx) {
        currentBucket = 0;
        lhs = tempLHS;
        rhs = tempRHS;
        this.tx = tx;
        this.pred = new Predicate(
                        new Term(
                            new FieldNameExpression(lhsFldname),
                            new FieldNameExpression(rhsFldname)));
        UpdateScan lhsScan = lhs[currentBucket].open();
        TableInfo rhsTableInfo = rhs[currentBucket].getTableInfo();
        MultiBufferProductScan mb = new MultiBufferProductScan(lhsScan, rhsTableInfo, tx);
        sc = new SelectScan(mb, pred);
    }

    @Override
    public void beforeFirst() {
        currentBucket = 0;
    }

    @Override
    public boolean next() {
        boolean hasNext = sc.next();
        if (!hasNext) {
            sc.close();
            currentBucket++;
            if(currentBucket < lhs.length) {
                UpdateScan lhsScan = lhs[currentBucket].open();
                TableInfo rhsTableInfo = rhs[currentBucket].getTableInfo();
                MultiBufferProductScan mb = new MultiBufferProductScan(lhsScan, rhsTableInfo, tx);
                sc = new SelectScan(mb, pred);
                return sc.next();
            } else return false;
        }
        return hasNext;
    }

    @Override
    public void close() {
        sc.close();
    }

    @Override
    public Constant getVal(String fldname) {
        return sc.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return sc.getInt(fldname);
    }

    @Override
    public String getString(String fldname){
        return sc.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return sc.hasField(fldname);
    }
}

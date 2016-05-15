package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Daniel on 14/05/16.
 */
public class KMergeSortPlan extends SortPlan {

    public KMergeSortPlan(Plan p, Collection<String> sortfields, Transaction tx) {
        super(p, sortfields, tx);
    }

    @Override
    public Scan open() {
        Scan src = getP().open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        int k = BufferNeeds.bestRoot(getP().blocksAccessed());
        while (runs.size() > 2)
            runs = doAMergeIteration(runs, k);
        return new SortScan(runs, getComp());
    }

    protected List<TempTable> doAMergeIteration(List<TempTable> runs, int k) {
        List<TempTable> result = new ArrayList<TempTable>();
        while (runs.size() > 1) {
            result.add(mergeKRuns(runs, k));
        }
        if (runs.size() == 1)
            result.add(runs.get(0));
        return result;
    }

    private TempTable mergeKRuns(List<TempTable> runs, int k) {
        TempTable result = new TempTable(getSch(), getTx());
        UpdateScan dest = result.open();
        List<Scan> merge = new ArrayList<>();
        if (k > runs.size())
            k = runs.size();
        for(int i = 0; i < k; i++) {
            Scan s = runs.remove(0).open();
            s.next();
            merge.add(s);
        }

        List<Scan> toClose = new ArrayList<>();
        while(merge.size() > 1) {
            int p = 0;
            for(int i = 1; i < k ; i++) {
                if(getComp().compare(merge.get(p), merge.get(i)) > 0) {
                    p = i;
                }
            }
            if(!copy(merge.get(p), dest)) {
                toClose.add(merge.remove(p));
                k--;
            }
        }

        if(merge.size() == 1)
            while (copy(merge.get(0), dest));

        toClose.add(merge.get(0));
        toClose.forEach(Scan::close);
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : getSch().fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }
}
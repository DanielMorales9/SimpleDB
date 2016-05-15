package simpledb.materialize;

import simpledb.parse.QueryData;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Daniel on 15/05/16.
 */
public class HashJoinTest {

    public static void main(String[] args) {


        SimpleDB.init("studentdb", "LRU");

        // analogous to the connection
        Transaction tx = new Transaction();

        Collection<String> fields = new ArrayList<>();
        fields.add("sid");
        fields.add("sname");
        fields.add("majorid");
        fields.add("gradyear");
        fields.add("did");
        fields.add("dname");
        Collection<String> tables = new ArrayList<>();

        tables.add("student");
        tables.add("dept");

        QueryData data = new QueryData(fields, tables, new Predicate(), new ArrayList<>());

        //Step 1: Create a plan for each mentioned table or view
        List<Plan> plans = new ArrayList<Plan>();
        for (Iterator<String> iterator = data.tables().iterator(); iterator.hasNext(); ) {
            String tblname = iterator.next();
            plans.add(new TablePlan(tblname, tx));
        }

        //Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans)
            p = new HashJoinPlan(p, nextplan, "majorid", "did", tx);

        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        //Step 4: Project on the fields names
        p = new ProjectPlan(p, data.fields());

        Scan s = p.open();

        for (String str : fields) {
            System.out.print(str +"\t");
        }
        System.out.println();

        while(s.next()) {
            for (String st : fields) {
                System.out.print(s.getVal(st) + "\t\t");
            }
            System.out.println();
        }

    }
}

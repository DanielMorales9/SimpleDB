import java.sql.*;
import java.util.Random;

import simpledb.remote.SimpleDriver;

/**
 * Created by Daniel on 14/05/16.
 */
public class CreatePeopleDB {

    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            /*String s = "create table PEOPLE(PId int, PName varchar(10), Eta int)";
            stmt.executeUpdate(s);
            System.out.println("Table PEOPLE created.");*/

            String[] peoplevals = values(10001, 0, 100);

            String s = "insert into PEOPLE(PId, PName, Eta) values ";

            for (int i=0; i<peoplevals.length; i++) {
                System.out.println(s+ peoplevals[i]);
                stmt.executeUpdate(s + peoplevals[i]);
            }
            System.out.println("PEOPLE records inserted.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String[] values(int k, int minAge, int maxAge) {
        String[] peoplevals = new String[k];

        for (int i = 0; i < k; i++) {
            Integer id = (i+1);
            String sid = id.toString();
            String v = "("+ sid + ", \'"+randomStringGenerator()+"\', "+randomNumberGenerator(minAge, maxAge)+")";
            peoplevals[i] = v;
        }
        return peoplevals;
    }

    public static int randomNumberGenerator(int leftLimit, int rightLimit) {
        Random r = new Random();
        return r.nextInt(rightLimit - leftLimit) + leftLimit;
    }

    public static String randomStringGenerator() {
        String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();
        Random rnd = new Random();
        int varlength = rnd.nextInt(10) + 1;
        while (sb.length() < varlength) {
            int index = (int) (rnd.nextFloat() * alphabet.length());
            sb.append(alphabet.charAt(index));
        }
        String randString = sb.toString();
        return randString;
    }

}

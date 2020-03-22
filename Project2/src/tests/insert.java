package tests;

import BigT.Map;
import BigT.bigt;
import btree.BTFileScan;
import btree.BTreeFile;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

class Sailor {
    public String    sid;
    public String sname;
    public int    rating;
    public String age;

    public Sailor (String _sid, String _sname, int _rating,String _age) {
        sid    = _sid;
        sname  = _sname;
        rating = _rating;
        age    = _age;
    }
}

public class insert{
    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;

    private bigt bigtable;
    private BTreeFile bTreeFile;
    private BTFileScan btScan,btScan1;
    private Map t;

    private String fileName;
    private String tableName;
    private int tableType;
    private int NUMBUF;

    public insert(){
        //System.out.println("Please enter the fileName: ");
        // fileName = getCommand();

        System.out.println("Please enter the table name: ");
        tableName = getCommand();

        System.out.println("Please enter the table type(1,2 or 4): ");
        tableType = Integer.parseInt(getCommand());

        System.out.println("Please enter the NUMBUF: ");
        NUMBUF = Integer.parseInt(getCommand());
    }

    public bigt runInsertTest(){
        sailors  = new Vector();

        sailors.addElement(new Sailor("Dominica", "Bob Holloway",       9, "53.6"));
        sailors.addElement(new Sailor("Serbia", "Susan Horowitz",     1, "34.2"));
        sailors.addElement(new Sailor("Carcharhinus", "Yannis Ioannidis",   8, "40.2"));
        sailors.addElement(new Sailor("Virginia", "Deborah Joseph",    10, "39.8"));
        sailors.addElement(new Sailor("Monaco", "Landwebber",         8, "56.7"));
        sailors.addElement(new Sailor("Turkey", "James Larus",        9, "30.3"));
        sailors.addElement(new Sailor("Zimbabwe", "Barton Miller",      5, "43.7"));
        sailors.addElement(new Sailor("California", "David Parter",       1, "99.9"));
        sailors.addElement(new Sailor("Tunisia", "Raghu Ramakrishnan", 9, "37.1"));
        sailors.addElement(new Sailor("Greece", "Guri Sohi",         10, "42.1"));
        sailors.addElement(new Sailor("Hawk", "Prasoon Tiwari",     8, "39.2"));
        sailors.addElement(new Sailor("Isogomphodon", "Anne Condon",        3, "30.3"));
        sailors.addElement(new Sailor("Tuvalu", "Charles Fischer",    6, "46.3"));
        sailors.addElement(new Sailor("Dominica", "James Goodman",      4, "50.3"));
        sailors.addElement(new Sailor("Zebra", "Mark Hill",          5, "35.2"));
        sailors.addElement(new Sailor("Singapore", "Mary Vernon",        7, "43.1"));
        sailors.addElement(new Sailor("Camel", "David Wood",         3, "39.2"));


        boolean status = OK;
        int numsailors = 17;
        int numsailors_attrs = 4;

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.testdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".testlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock" );

        t = new Map();
        try {
            t.setHdr((short) 4,t.getAttrType(), t.getMapSizes());
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        MID mid;
        bigtable = null;
        try {
            bigtable = new bigt(tableName,tableType);
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Map(size);
        try {
            t.setHdr((short) 4, t.getAttrType(), t.getMapSizes());
        }
        catch (Exception e) {
            System.err.println("*** error in Map.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<numsailors; i++) {
            try {
                t.setStrFld(1, ((Sailor)sailors.elementAt(i)).sid);
                t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
                t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
                t.setStrFld(4, ((Sailor)sailors.elementAt(i)).age);
            }
            catch (Exception e) {
                System.err.println("*** bigt error in Map.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                mid = bigtable.insertMap(t.returnMapByteArray());
//                String columLable = t.getColumnLabel();
//                System.out.println("Column " + i + " Label: " + columLable);
            }
            catch (Exception e) {
                System.err.println("*** error in map.insertMap() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }


        try {
            int mapcount = bigtable.getMapCnt();
            System.out.println ("There are " + mapcount + " maps");
        }
        catch (Exception e) {
            System.err.println("*** error in Map.getMapCnt() ***");
            status = FAIL;
            e.printStackTrace();
        }

        return bigtable;
    }

    public String getCommand(){
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String s = null;

        try {
            s = in.readLine();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return s;
    }
}

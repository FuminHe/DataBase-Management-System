package tests;
//originally from : joins.C

import BigT.*;
import BigT.Map;
import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

//Define the Sailor schema
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


class JoinsDriver implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;
    private AttrType [] Stypes;
    private short [] Ssizes;

    private bigt bigtable;
    private BTreeFile bTreeFile;
    private BTFileScan btScan,btScan1;
    private Map t;
    private MID mid;

    private short maxRowLen = 30;
    private short maxColLen = 30;
    private short maxValLen = 30;


    /** Constructor
     */
    public JoinsDriver() {

        //build Sailor, Boats, Reserves table
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

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

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

        SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

//        // creating the sailors relation
//        Stypes = new AttrType[4];
//        Stypes[0] = new AttrType (AttrType.attrString);
//        Stypes[1] = new AttrType (AttrType.attrString);
//        Stypes[2] = new AttrType (AttrType.attrInteger);
//        Stypes[3] = new AttrType (AttrType.attrString);
//
//        //SOS
//        Ssizes = new short [3];
//        Ssizes[0] = maxRowLen; //first elt. is 30
//        Ssizes[1] = maxColLen;
//        Ssizes[2] = maxValLen;


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
        //MID mid;
        bigtable = null;
        try {
            bigtable = new bigt("sailors.in",2);
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
            System.out.println ("--------------map count is:----------" + mapcount);
        }
        catch (Exception e) {
            System.err.println("*** error in Map.getMapCnt() ***");
            status = FAIL;
            e.printStackTrace();
        }



    }

    public boolean runTests() {
        try{
            Stream s = new Stream(bigtable,3, "[A,E]",
               "*", "[30,50]");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

//        Query1(bigtable,1, "[A,D]",
//                "*", "[1,3]");

        //Query2();
        System.out.print ("Finished testing"+"\n");


        return true;
    }

    public void Query1(bigt bigtable, int orderType, String rowFilter,
                       String columnFilter, String valueFilter){
        System.out.println("**********************Query1 strating *********************\n");

    }



    public void Query2(){
        System.out.print("**********************Query2 strating *********************\n");

        Stream scan = null;
        try {
            scan = new Stream(bigtable);
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        try {
            bTreeFile = new BTreeFile("BTreeIndex_row", AttrType.attrString, maxRowLen, 1);
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        MID m_id = new MID();
        Map temp = null;

        try {
            temp = scan.getNext(m_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        int sizett = t.size();
        Map tt = new Map(sizett);
        try {
            tt.setHdr((short) 4, Stypes, Ssizes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        String key = "";
        while (temp != null){
            tt.mapCopy(temp);
            //System.out.println("temp is not null");
            try{
                key = tt.getStrFld(1);
                bTreeFile.insert(new StringKey(key), m_id);
                temp = scan.getNext(mid);
                //System.out.println("the key " + i + "is: "+ key);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        // get the initial scan
        try{
            btScan = bTreeFile.new_scan(new StringKey("A"),
                    new StringKey("D"));
            System.out.println("succeed");
        }
        catch (Exception e) {
            e.printStackTrace();
        }


    }
}

public class QueryTest {
    public static void main(String argv[]){
        boolean sortstatus;

        JoinsDriver jjoin = new JoinsDriver();

        sortstatus = jjoin.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during tests");
        }
        else {
            System.out.println(" tests completed successfully");
        }
    }
}


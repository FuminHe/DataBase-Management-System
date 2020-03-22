
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

    private Heapfile bigtable;
    private BTreeFile bTreeFile;
    private BTFileScan btScan,btScan1;
    private Map t;

    private short maxRowLen = 30;
    private short maxColLen = 30;
    private short maxValLen = 30;


    /** Constructor
     */
    public JoinsDriver() throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException, ScanIteratorException {

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

        // creating the sailors relation
        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrString);
        Stypes[1] = new AttrType (AttrType.attrString);
        Stypes[2] = new AttrType (AttrType.attrInteger);
        Stypes[3] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short [3];
        Ssizes[0] = maxRowLen; //first elt. is 30
        Ssizes[1] = maxColLen;
        Ssizes[2] = maxValLen;


        t = new Map();
        try {
            t.setHdr((short) 4,Stypes, Ssizes);
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
            bigtable = new Heapfile("sailors.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Map(size);
        try {
            t.setHdr((short) 4, Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Map.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

//        for (int i=0; i<numsailors; i++) {
//            try {
//                t.setStrFld(1, ((Sailor)sailors.elementAt(i)).sid);
//                t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
//                t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
//                t.setStrFld(4, ((Sailor)sailors.elementAt(i)).age);
//            }
//            catch (Exception e) {
//                System.err.println("*** bigt error in Map.setStrFld() ***");
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            try {
//                mid = bigtable.insertMap(t.returnMapByteArray());
////                String columLable = t.getColumnLabel();
////                System.out.println("Column " + i + " Label: " + columLable);
//            }
//            catch (Exception e) {
//                System.err.println("*** error in map.insertMap() ***");
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }
//        if (status != OK) {
//            //bail out
//            System.err.println ("*** Error creating relation for sailors");
//            Runtime.getRuntime().exit(1);
//        }
//
//
//        try {
//            int mapcount = bigtable.getMapCnt();
//            System.out.println ("--------------map count is:----------" + mapcount);
//        }
//        catch (Exception e) {
//            System.err.println("*** error in Map.getMapCnt() ***");
//            status = FAIL;
//            e.printStackTrace();
//        }

        Scan scan = null;

        try {
            scan = new Scan(bigtable);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // create the index file --> row
        bTreeFile = null;
        try {
            bTreeFile = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


        MID rid = new MID();
        int key = 0;
        Map temp = null;

        try {
            temp = scan.getNext(rid);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        while ( temp != null) {
            System.out.println("temp is not null");
            t.mapCopy(temp);

            try {
                key = t.getIntFld(3);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                bTreeFile.insert(new IntegerKey(key), rid);
                System.out.println("Key: " + key + "rid:");
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                temp = scan.getNext(rid);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // close the file scan
        scan.closescan();

        btScan = bTreeFile.new_scan(new IntegerKey(1),
                new IntegerKey(4));

        KeyDataEntry entry= btScan.get_next();
        if(entry!=null)
            System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);

        else{
            System.out.println("the entry is empty");
        }

    }

    public boolean runTests() throws IteratorException, ConstructPageException, UnpinPageException, PinPageException, AddFileEntryException, GetFileEntryException, ScanIteratorException, IOException, KeyNotMatchException, NodeNotMatchException, LeafDeleteException, InvalidMapSizeException, IndexSearchException, InsertException, ConvertException, DeleteRecException, FieldNumberOutOfBoundException, KeyTooLongException, IndexInsertRecException, LeafInsertRecException {

//        Query1(bigtable,1, "[A,D]",
//                "*", "[1,3]");

        Query2();
        System.out.print ("Finished testing"+"\n");


        return true;
    }

    public void Query1(bigt bigtable, int orderType, String rowFilter,
                       String columnFilter, String valueFilter)
            throws ScanIteratorException,
            PinPageException,
            KeyNotMatchException,
            IteratorException,
            IOException,
            ConstructPageException,
            UnpinPageException,
            GetFileEntryException,
            AddFileEntryException, NodeNotMatchException, KeyTooLongException, InvalidMapSizeException, IndexSearchException, IndexInsertRecException, ConvertException, DeleteRecException, LeafInsertRecException, InsertException, LeafDeleteException, FieldNumberOutOfBoundException {
        System.out.println("**********************Query1 strating *********************\n");

        // try the query here
        //query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF


//        // get the key filter
//        String[] rFilter = getKeyFilter(rowFilter);
//        String[] cFilter = getKeyFilter(columnFilter);
//        String[] vFilter = getKeyFilter(valueFilter);
//
//
//        // based on the big table type
//        // initialize a scan
//        int tabletype = bigtable.getBigtType();
//        System.out.println("table type: "+tabletype);
//        System.out.println("Row filter[0]: " + rFilter[0]);
//        System.out.println("Row filter[1]: " + rFilter[1]);
//        switch (tabletype){
//            case 1: // no index
//                break;
//            case 2: // one btree to index row labels
//                if(rFilter[0].equals("*")){
//                    break;
//                }
//                else{
//                    // create a row index file
//                    //bTreeFile = new BTreeFile("BTreeIndex_row", AttrType.attrString, maxRowLen, 1);
//                    // e.g. 24
//                    if(rFilter.length == 1){
//                        btScan = bTreeFile.new_scan(new StringKey(rFilter[0]),
//                                    new StringKey(rFilter[0]));
//                    }
//                    // [23,25]
//                    else{
//                        System.out.println("should be here");
//                        btScan = bTreeFile.new_scan(new StringKey(rFilter[0]),
//                                    new StringKey(rFilter[1]));
//
//                        KeyDataEntry entry= btScan.get_next();
//                        if(entry!=null)
//                            System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);
//
//                        else{
//                            System.out.println("the entry is empty");
//                        }
//                    }
//                }
//
//
//                break;
//            case 4: //one btree to index column label and row label (combined key) and
//                    //one btree to index timestamps
//                if(rFilter[0].equals("*"))
//                break;
//        }


    }



    /**
     * get the lo_key and hi_key from the filter
     * The filter could be : * or [12,34] or 24
     */
    public String[] getKeyFilter(String filter){
        String[] flt;

        if(filter.contains("[")){
            String temp = filter.substring(filter.lastIndexOf("[")+1).replaceAll("]", "");
            flt = temp.split(",");
        }
        else{
            flt = new String[1];
            flt[0] = filter;
        }

        return flt;
    }


    public void Query2() {
        System.out.print("**********************Query2 strating *********************\n");
    }

//    private void Disclaimer() {
//        System.out.print ("--------------start-----------------");
//    }
}

public class query {
    public static void main(String argv[]) throws IOException, ConstructPageException, UnpinPageException, IteratorException, GetFileEntryException, ScanIteratorException, AddFileEntryException, KeyNotMatchException, PinPageException, NodeNotMatchException, KeyTooLongException, InvalidMapSizeException, IndexSearchException, IndexInsertRecException, ConvertException, DeleteRecException, LeafInsertRecException, InsertException, LeafDeleteException, FieldNumberOutOfBoundException {
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


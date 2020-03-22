package tests;

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

class QueryTest{
    //private bigt bigTable;
    private String dbpath;

    public QueryTest(){
        dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.testdb";
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
        catch(IOException e) {
            System.err.println (""+e);
        }
    }

    public void runQueryTest(bigt bigTable){
        System.out.println("bigt name: " + bigTable.getBigtName() +
                ", bigt type: " + bigTable.getBigtType());

//        try{
//            System.out.println("bigt map count: " + bigTable.getMapCnt());
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }

        String[] query = getQuery();
//        SystemDefs sysdef = new SystemDefs(dbpath, 1000,
//                Integer.parseInt(query[6]), "Clock" );

        // check table name and type
        if(!bigTable.getBigtName().equals(query[0]) &&
                bigTable.getBigtType() != Integer.parseInt(query[1])){
            System.out.println("Please enter the correct table name/type");
        }
        else{
            try {
                //bigTable = new bigt(query[0],Integer.parseInt(query[1]));
//                Stream stream = new Stream(bigTable,Integer.parseInt(query[2]),
//                        query[3],query[4],query[5]);
                Stream stream = bigTable.openStream(Integer.parseInt(query[2]),
                        query[3],query[4],query[5]);
                //stream.closestream();
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public String[] getQuery(){
        String[] query = new String[7];
        // BIGTABLENAME
        System.out.println("Please enter your big table name:");
        query[0] = getCommand();

        //TYPE
        System.out.println("Please choose big table type: 1, 2 or 4");
        query[1] = getCommand();

        // ORDERTYPE
        System.out.println("Please choose order type: 3 or 4");
        query[2] = getCommand();

        System.out.println("Please enter row filter:");
        query[3] = getCommand();

        System.out.println("Please enter column filter:");
        query[4] = getCommand();

        System.out.println("Please enter value filter:");
        query[5] = getCommand();

        System.out.println("Please enter NUMBUF(50)");
        query[6] = getCommand();

        return query;
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



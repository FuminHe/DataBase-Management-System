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

class queryDriver implements GlobalConst {
    private bigt bigTable;

    public queryDriver(){
        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.querytestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".querylog";

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

        SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

    }

    public void runTest(){
        int choice = Menu();
        while (choice != 2){
            pcounter.initialize();
            String[] query = getQuery();
            batchInsert(query[0],Integer.parseInt(query[1]));
            try{
                Stream stream = new Stream(bigTable,Integer.parseInt(query[2]),
                        query[3],query[4],query[5]);
                int readCount = pcounter.rcounter;
                int writeCount = pcounter.wcounter;

                System.out.println("The read count is: " + readCount);
                System.out.println("The write count is: " + writeCount);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            choice = Menu();
        }
    }

    private void batchInsert(String bigtName, int bigtType) {
        // data file
        String fileName = "project2_testdata.csv";
        String line = "";
        Map map = null;
        MID mid;

        try{
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            // need further correction
            map.setHdr((short)4,map.getAttrType(),map.getMapSizes());
            bigTable = new bigt(bigtName,2);

            while ((line = br.readLine()) != null) {
                String[] mapString = line.split(",");
                map.setStrFld(1, mapString[0]);
                map.setStrFld(2, mapString[1]);
                map.setIntFld(3, Integer.parseInt(mapString[2]));
                map.setStrFld(4, mapString[3]);

                mid = bigTable.insertMap(map.getMapByteArray());
            }

            System.out.println("Complete File Inserting");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    public int Menu(){
        System.out.println("--------------Query Test-----------");
        System.out.println("[1] start test");
        System.out.println("[2] Exit");

        return Integer.parseInt(getCommand());
    }
    public String[] getQuery(){
        String[] query = new String[6];
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

public class QueryTest {
    public static void main(String argv[]){
        queryDriver jjoin = new queryDriver();
        jjoin.runTest();
    }
}


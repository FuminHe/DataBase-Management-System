package diskmgr;

public class pcounter {
    public static int rcounter;
    public static int wcounter;
    public static void initialize() {
        if(rcounter != 0 & wcounter != 0){
        }
        else{
            rcounter =0;
            wcounter =0;
        }

    }
    public static void readIncrement() {
        rcounter++;
    }
    public static void writeIncrement() {
        wcounter++;
    }
}

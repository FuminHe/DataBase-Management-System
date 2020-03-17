import java.lang.*;

public class project2_main_func {
    public static void main(String[] args)
    {
        System.out.println("hello");
        String[] re = getKeyFilter("24");
        for (String s : re) {
            System.out.println(s);
        }

    }

    public static String[] getKeyFilter(String filter){
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

}

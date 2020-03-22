package BigT;


import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.*;

import java.io.IOException;

/**
 * Based on Tuple.java
 * with 4 fixed fields:
 * RowLabel:attrString
 * ColumnLabel:attrString
 * TimeStamp:attrInteger
 * Value:attrString
 */

public class Map implements GlobalConst {
    /**
     * Maximum size of any tuple
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte [] data;

    /**
     * start position of this map in data[]
     */
    private int map_offset;

    /**
     * length of this map
     */
    private int map_length;

    /**
     * private field
     * Number of fields in this map
     */
    private short fldCnt;

    /**
     * private field
     * Array of offsets of the fields
     */
    private short [] fldOffset;

    /**
     * Class constructor
     * Creat a new map with length = max_size,map offset = 0.
     */
    public  Map(){
        // Creat a new map
        data = new byte[max_size];
        map_offset = 0;
        map_length = max_size;
    }

    /** Constructor
     * @param amap a byte string which contains the map
     * @param offset the offset of the map in the byte array
     * @param length the length of the map
     */
    public Map(byte [] amap, int offset, int length){
        data = amap;
        map_offset = offset;
        map_length = length;
    }

    /** Constructor(used as tuple copy)
     * @param fromMap   a byte array which contains the map
     *
     */
    public Map(Map fromMap){
        data = fromMap.getMapByteArray();
        map_length = fromMap.getLength();
        map_offset = 0;
        fldCnt = fromMap.noOfFlds();
        fldOffset = fromMap.copyFldOffset();
    }

    /**
     * Class constructor
     * Creat a new map with length = size,map offset = 0.
     */
    public Map(int size) {
        // Creat a new tuple
        data = new byte[size];
        map_offset = 0;
        map_length = size;
    }

    /**
     * Returns the row label
     * <row,column,timestamp,value> -> fldOffset[0]
     */
    public String getRowLabel() throws IOException {
        return Convert.getStrValue(fldOffset[0],data,
                fldOffset[1]-fldOffset[0]);
    }

    /**
     * Returns the column label
     * <row,column,timestamp,value> -> fldOffset[1]
     */
    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(fldOffset[1],data,
                fldOffset[2]-fldOffset[1]);
    }

    /**
     * Returns the timestamp
     * <row,column,timestamp,value> -> fldOffset[2]
     */
    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(fldOffset[2],data);
    }

    /**
     * Returns the value
     * <row,column,timestamp,value> -> fldOffset[3]
     */
    public String getValue() throws IOException {
        return Convert.getStrValue(fldOffset[3],data,
                fldOffset[4] - fldOffset[3]);
    }

    /**
     * Set the row label
     */
    public Map setRowLabel(String val) throws IOException {
        Convert.setStrValue(val,fldOffset[0],data);
        return this;
    }

    /**
     * Set the column label
     */
    public Map setColumnLabel(String val) throws IOException {
        Convert.setStrValue(val,fldOffset[1],data);
        return this;
    }

    /**
     * Set the timestamp
     */
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val,fldOffset[2],data);
        return this;
    }

    /**
     * Set the value
     */
    public Map setValue(String val) throws IOException {
        Convert.setStrValue(val,fldOffset[3],data);
        return this;
    }

    /**
     * Copy the map to byte array out
     * @return  byte[], a byte array contains the map
     *  the length of byte[] = length of the map
     */
    public byte [] getMapByteArray() {
        byte [] mapcopy = new byte [map_length];
        System.arraycopy(data, map_offset, mapcopy, 0, map_length);
        return mapcopy;
    }

    /**
     * Print out the map
     * [0,0,1,0]
     */
    public void print(AttrType type[])
            throws IOException {
        int i, val;
        float fval;
        String sval;

        System.out.print("[");
        for (i=0; i< fldCnt-1; i++) {
            switch(type[i].attrType) {
                case AttrType.attrInteger:
                    val = Convert.getIntValue(fldOffset[i], data);
                    System.out.print(val);
                    break;

                case AttrType.attrReal:
                    fval = Convert.getFloValue(fldOffset[i], data);
                    System.out.print(fval);
                    break;

                case AttrType.attrString:
                    sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
                    System.out.print(sval);
                    break;

                case AttrType.attrNull:
                case AttrType.attrSymbol:
                    break;
            }
            System.out.print(", ");
        }

        switch(type[fldCnt-1].attrType) {
            case AttrType.attrInteger:
                val = Convert.getIntValue(fldOffset[i], data);
                System.out.print(val);
                break;

            case AttrType.attrReal:
                fval = Convert.getFloValue(fldOffset[i], data);
                System.out.print(fval);
                break;

            case AttrType.attrString:
                sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
                System.out.print(sval);
                break;

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
        }
        System.out.println("]");

    }

    /**
     * Get the length of the map
     * Call setHdr () before
     * Return size of this map in bytes
     */
    public short size()
    {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    /** Copy a map to the current map position
     *  you must make sure the tuple lengths must be equal
     * @param fromMap the tuple being copied
     */
    public void mapCopy(Map fromMap) {
        byte [] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, data, map_offset, map_length);
//        fldCnt = fromMap.noOfFlds();
//        fldOffset = fromMap.copyFldOffset();
    }

    /** This is used when you don't want to use the constructor
     * @param amap  a byte array which contains the map
     * @param offset the offset of the map in the byte array
     * @param length the length of the map
     */
    public void mapInit(byte [] amap, int offset, int length) {
        data = amap;
        map_offset = offset;
        map_length = length;
    }

    /**
     * Set a tuple with the given tuple length and offset
     * @param	record	a byte array contains the tuple
     * @param	offset  the offset of the tuple ( =0 by default)
     * @param	length	the length of the tuple
     */
    public void mapSet(byte [] record, int offset, int length) {
        System.arraycopy(record, offset, data, 0, length);
        map_offset = 0;
        map_length = length;
    }






    /** get the length of a map, call this method if you did not
     *  call setHdr () before
     * @return 	length of this tuple in bytes
     */
    public int getLength()
    {
        return map_length;
    }

    /** get the offset of a map
     *  @return offset of the map in byte array
     */
    public int getOffset() {
        return map_offset;
    }

    /** return the data byte array
     *  @return  data byte array
     */
    public byte [] returnMapByteArray() {
        return data;
    }

    /**
     * Convert this field into integer
     *
     * @param	fldNo	the field number
     * @return		the converted integer if success
     *
     * @exception IOException I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */

    public int getIntFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        int val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getIntValue(fldOffset[fldNo -1], data);
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field in to float
     *
     * @param    fldNo   the field number
     * @return           the converted float number  if success
     *
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Map field number out of bound
     */

    public float getFloFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        float val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getFloValue(fldOffset[fldNo -1], data);
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
    }


    /**
     * Convert this field into String
     *
     * @param    fldNo   the field number
     * @return           the converted string if success
     *
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Map field number out of bound
     */

    public String getStrFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        String val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getStrValue(fldOffset[fldNo -1], data,
                    fldOffset[fldNo] - fldOffset[fldNo -1]); //strlen+2
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field into a character
     *
     * @param    fldNo   the field number
     * @return           the character if success
     *
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Tuple field number out of bound
     */

    public char getCharFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException
    {
        char val;
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            val = Convert.getCharValue(fldOffset[fldNo -1], data);
            return val;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to integer value
     *
     * @param	fldNo	the field number
     * @param	val	the integer value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            Convert.setIntValue (val, fldOffset[fldNo -1], data);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Set this field to float value
     *
     * @param     fldNo   the field number
     * @param     val     the float value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            Convert.setFloValue (val, fldOffset[fldNo -1], data);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to String value
     *
     * @param     fldNo   the field number
     * @param     val     the string value
     * @exception   IOException I/O errors
     * @exception   FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo <= fldCnt))
        {
            Convert.setStrValue (val, fldOffset[fldNo -1], data);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "MAP:MAP_FLDNO_OUT_OF_BOUND");
    }


    /**
     * setHdr will set the header of this map.
     *
     * @param	numFlds	  number of fields
     * @param	types     contains the types that will be in this map
     * @param	strSizes  contains the sizes of the string
     *
     * @exception IOException I/O errors
     * @exception InvalidTypeException Invalid tupe type
     * @exception InvalidTupleSizeException Tuple size too big
     *
     */

    public void setHdr (short numFlds, AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidMapSizeException
    {
        if((numFlds +2)*2 > max_size)
            throw new InvalidMapSizeException (null, "MAP: MAP_TOOBIG_ERROR");

        fldCnt = numFlds;
        Convert.setShortValue(numFlds, map_offset, data);
        fldOffset = new short[numFlds+1];
        int pos = map_offset+2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        fldOffset[0] = (short) ((numFlds +2) * 2 + map_offset);

        Convert.setShortValue(fldOffset[0], pos, data);
        pos +=2;
        short strCount =0;
        short incr;
        int i;

        for (i=1; i<numFlds; i++)
        {
            switch(types[i-1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrReal:
                    incr =4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidTypeException (null, "MAP: MAP_TYPE_ERROR");
            }
            fldOffset[i]  = (short) (fldOffset[i-1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos +=2;

        }
        switch(types[numFlds -1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr =4;
                break;

            case AttrType.attrString:
                incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidTypeException (null, "MAP: MAP_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        map_length = fldOffset[numFlds] - map_offset;

        if(map_length > max_size)
            throw new InvalidMapSizeException (null, "MAP: MAP_TOOBIG_ERROR");
    }


    /**
     * Returns number of fields in this map
     *
     * @return the number of fields in this map
     *
     */
    public short noOfFlds() {
        return fldCnt;
    }

    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     *
     */
    public short[] copyFldOffset()
    {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i=0; i<=fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }



    /**
     * private method
     * Padding must be used when storing different types.
     *
     * @param	offset
     * @param type   the type of tuple
     * @return short typle
     */

    private short pad(short offset, AttrType type)
    {
        return 0;
    }

    // attr types for map
    public AttrType[] getAttrType(){
        AttrType[] mapAttrtypes = new AttrType[4];
        mapAttrtypes[0] = new AttrType (AttrType.attrString);
        mapAttrtypes[1] = new AttrType (AttrType.attrString);
        mapAttrtypes[2] = new AttrType (AttrType.attrInteger);
        mapAttrtypes[3] = new AttrType (AttrType.attrString);

        return mapAttrtypes;
    }

    // string sizes for map
    public short[] getMapSizes(){
        //bigt temp = new bigt();
        short[] mapSizes = new short[3];
        mapSizes[0] = 30;
        mapSizes[1] = 30;
        mapSizes[2] = 30;

        return mapSizes;
    }

//    private short[] mapSizes;
//    public void setMapSizes(short maxRow, short maxCol, short maxTS){
//        //bigt temp = new bigt();
////        maxRow = 30;
////        maxCol = 30;
////        maxTS = 30;
//
//        this.mapSizes = new short[3];
//        this.mapSizes[0] = maxRow;
//        this.mapSizes[1] = maxCol;
//        this.mapSizes[2] = maxTS;
//    }
}

package BigT;

import btree.*;
import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;

public class Stream implements GlobalConst {
    /** The heapfile we are using. */
    private bigt _hf;

    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private MID datapageMid = new MID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private HFPage datapage = new HFPage();

    /** record ID of the current record (from the current data page) */
    private MID usermid = new MID();

    /** Status of next user status */
    private boolean nextUserStatus;


//    /** The constructor pins the first directory page in the file
//     * and initializes its private data members from the private
//     * data member from hf
//     *
//     * @exception InvalidMapSizeException Invalid map size
//     * @exception IOException I/O errors
//     *
//     * @param hf A HeapFile object
//     */
//    public Stream(bigt hf)
//            throws InvalidMapSizeException,
//            IOException {
//        init(hf);
//    }

    /** Retrieve the next record in a sequential scan
     *
     * @exception InvalidMapSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param mid Record ID of the record
     * @return the Tuple of the retrieved record.
     */
    public Map getNext(MID mid)
            throws InvalidMapSizeException,
            IOException {
        Map recptrmap = null;

        if (nextUserStatus != true) {
            nextDataPage();
        }

        if (datapage == null)
            return null;

        mid.pageNo.pid = usermid.pageNo.pid;
        mid.slotNo = usermid.slotNo;

        try {
            recptrmap = datapage.getRecord(mid);
        }

        catch (Exception e) {
            //    System.err.println("SCAN: Error in Scan" + e);
            e.printStackTrace();
        }

        usermid = datapage.nextRecord(mid);
        if(usermid == null) nextUserStatus = false;
        else nextUserStatus = true;

        return recptrmap;
    }


    /** Position the scan cursor to the record with the given rid.
     *
     * @exception InvalidMapSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param mid Record ID of the given record
     * @return 	true if successful,
     *			false otherwise.
     */
    public boolean position(MID mid)
            throws InvalidMapSizeException,
            IOException {
        MID nxtmid = new MID();
        boolean bst;

        bst = peekNext(nxtmid);

        if (nxtmid.equals(mid)==true)
            return true;

        // This is kind lame, but otherwise it will take all day.
        PageId pgid = new PageId();
        pgid.pid = mid.pageNo.pid;

        if (!datapageId.equals(pgid)) {

            // reset everything and start over from the beginning
            reset();

            bst =  firstDataPage();

            if (bst != true)
                return bst;

            while (!datapageId.equals(pgid)) {
                bst = nextDataPage();
                if (bst != true)
                    return bst;
            }
        }

        // Now we are on the correct page.

        try{
            usermid = datapage.firstRecord();
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        if (usermid == null)
        {
            bst = false;
            return bst;
        }

        bst = peekNext(nxtmid);

        while ((bst == true) && (nxtmid != mid))
            bst = mvNext(nxtmid);

        return bst;
    }


    /** Do all the constructor work
     *
     * @exception InvalidMapSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
    private void init(bigt hf)
            throws InvalidMapSizeException,
            IOException
    {
        _hf = hf;
        firstDataPage();
    }


    /** Closes the Scan object */
    public void closestream()
    {
        reset();
    }


    /** Reset everything and unpin all pages. */
    private void reset()
    {

        if (datapage != null) {

            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {

            try{
                unpinPage(dirpageId, false);
            }
            catch (Exception e){
                //     System.err.println("SCAN: Error in Scan: " + e);
                e.printStackTrace();
            }
        }
        dirpage = null;

        nextUserStatus = true;

    }


    /** Move to the first data page in the file.
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @return true if successful
     *         false otherwise
     */
    private boolean firstDataPage()
            throws InvalidMapSizeException,
            IOException
    {
        DataPageInfo dpinfo;
        Map recmap = null;
        Boolean bst;

        /** copy data about first directory page */

        dirpageId.pid = _hf._firstDirPageId.pid;
        nextUserStatus = true;

        /** get first directory page and pin it */
        try {
            dirpage  = new HFPage();
            pinPage(dirpageId, (Page) dirpage, false);
        }

        catch (Exception e) {
            //    System.err.println("SCAN Error, try pinpage: " + e);
            e.printStackTrace();
        }

        /** now try to get a pointer to the first datapage */
        datapageMid = dirpage.firstRecord();

        if (datapageMid != null) {
            /** there is a datapage record on the first directory page: */

            try {
                recmap = dirpage.getRecord(datapageMid);
            }

            catch (Exception e) {
                //	System.err.println("SCAN: Chain Error in Scan: " + e);
                e.printStackTrace();
            }

            dpinfo = new DataPageInfo(recmap);
            datapageId.pid = dpinfo.pageId.pid;

        } else {

            /** the first directory page is the only one which can possibly remain
             * empty: therefore try to get the next directory page and
             * check it. The next one has to contain a datapage record, unless
             * the heapfile is empty:
             */
            PageId nextDirPageId = new PageId();

            nextDirPageId = dirpage.getNextPage();

            if (nextDirPageId.pid != INVALID_PAGE) {

                try {
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }

                catch (Exception e) {
                    //	System.err.println("SCAN: Error in 1stdatapage 1 " + e);
                    e.printStackTrace();
                }

                try {

                    dirpage = new HFPage();
                    pinPage(nextDirPageId, (Page )dirpage, false);

                }

                catch (Exception e) {
                    //  System.err.println("SCAN: Error in 1stdatapage 2 " + e);
                    e.printStackTrace();
                }

                /** now try again to read a data record: */

                try {
                    datapageMid = dirpage.firstRecord();
                }

                catch (Exception e) {
                    //  System.err.println("SCAN: Error in 1stdatapg 3 " + e);
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if(datapageMid != null) {

                    try {

                        recmap = dirpage.getRecord(datapageMid);
                    }

                    catch (Exception e) {
                        //    System.err.println("SCAN: Error getRecord 4: " + e);
                        e.printStackTrace();
                    }

                    if (recmap.getLength() != DataPageInfo.size)
                        return false;

                    dpinfo = new DataPageInfo(recmap);
                    datapageId.pid = dpinfo.pageId.pid;

                } else {
                    // heapfile empty
                    datapageId.pid = INVALID_PAGE;
                }
            }//end if01
            else {// heapfile empty
                datapageId.pid = INVALID_PAGE;
            }
        }

        datapage = null;

        try{
            nextDataPage();
        }

        catch (Exception e) {
            //  System.err.println("SCAN Error: 1st_next 0: " + e);
            e.printStackTrace();
        }

        return true;

        /** ASSERTIONS:
         * - first directory page pinned
         * - this->dirpageId has Id of first directory page
         * - this->dirpage valid
         * - if heapfile empty:
         *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
         * - if heapfile nonempty:
         *    - this->datapage == NULL, this->datapageId, this->datapageRid valid
         *    - first datapage is not yet pinned
         */

    }


    /** Move to the next data page in the file and
     * retrieve the next data page.
     *
     * @return 		true if successful
     *			false if unsuccessful
     */
    private boolean nextDataPage()
            throws InvalidMapSizeException,
            IOException
    {
        DataPageInfo dpinfo;

        boolean nextDataPageStatus;
        PageId nextDirPageId = new PageId();
        Map recmap = null;

        // ASSERTIONS:
        // - this->dirpageId has Id of current directory page
        // - this->dirpage is valid and pinned
        // (1) if heapfile empty:
        //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
        // (2) if overall first record in heapfile:
        //    - this->datapage==NULL, but this->datapageId valid
        //    - this->datapageRid valid
        //    - current data page unpinned !!!
        // (3) if somewhere in heapfile
        //    - this->datapageId, this->datapage, this->datapageRid valid
        //    - current data page pinned
        // (4)- if the scan had already been done,
        //        dirpage = NULL;  datapageId = INVALID_PAGE

        if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;

        if (datapage == null) {
            if (datapageId.pid == INVALID_PAGE) {
                // heapfile is empty to begin with

                try{
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e){
                    //  System.err.println("Scan: Chain Error: " + e);
                    e.printStackTrace();
                }

            } else {

                // pin first data page
                try {
                    datapage  = new HFPage();
                    pinPage(datapageId, (Page) datapage, false);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    usermid = datapage.firstRecord();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return true;
            }
        }

        // ASSERTIONS:
        // - this->datapage, this->datapageId, this->datapageRid valid
        // - current datapage pinned

        // unpin the current datapage
        try{
            unpinPage(datapageId, false /* no dirty */);
            datapage = null;
        }
        catch (Exception e){

        }

        // read next datapagerecord from current directory page
        // dirpage is set to NULL at the end of scan. Hence

        if (dirpage == null) {
            return false;
        }

        datapageMid = dirpage.nextRecord(datapageMid);

        if (datapageMid == null) {
            nextDataPageStatus = false;
            // we have read all datapage records on the current directory page

            // get next directory page
            nextDirPageId = dirpage.getNextPage();

            // unpin the current directory page
            try {
                unpinPage(dirpageId, false /* not dirty */);
                dirpage = null;

                datapageId.pid = INVALID_PAGE;
            }

            catch (Exception e) {

            }

            if (nextDirPageId.pid == INVALID_PAGE)
                return false;
            else {
                // ASSERTION:
                // - nextDirPageId has correct id of the page which is to get

                dirpageId = nextDirPageId;

                try {
                    dirpage  = new HFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }

                catch (Exception e){

                }

                if (dirpage == null)
                    return false;

                try {
                    datapageMid = dirpage.firstRecord();
                    nextDataPageStatus = true;
                }
                catch (Exception e){
                    nextDataPageStatus = false;
                    return false;
                }
            }
        }

        // ASSERTION:
        // - this->dirpageId, this->dirpage valid
        // - this->dirpage pinned
        // - the new datapage to be read is on dirpage
        // - this->datapageRid has the Rid of the next datapage to be read
        // - this->datapage, this->datapageId invalid

        // data page is not yet loaded: read its record from the directory page
        try {
            recmap = dirpage.getRecord(datapageMid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        if (recmap.getLength() != DataPageInfo.size)
            return false;

        dpinfo = new DataPageInfo(recmap);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new HFPage();
            pinPage(dpinfo.pageId, (Page) datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }


        // - directory page is pinned
        // - datapage is pinned
        // - this->dirpageId, this->dirpage correct
        // - this->datapageId, this->datapage, this->datapageRid correct

        usermid = datapage.firstRecord();

        if(usermid == null)
        {
            nextUserStatus = false;
            return false;
        }

        return true;
    }


    private boolean peekNext(MID mid) {

        mid.pageNo.pid = usermid.pageNo.pid;
        mid.slotNo = usermid.slotNo;
        return true;

    }


    /** Move to the next record in a sequential scan.
     * Also returns the RID of the (new) current record.
     */
    private boolean mvNext(MID mid)
            throws InvalidMapSizeException,
            IOException
    {
        MID nextmid;
        boolean status;

        if (datapage == null)
            return false;

        nextmid = datapage.nextRecord(mid);

        if( nextmid != null ){
            usermid.pageNo.pid = nextmid.pageNo.pid;
            usermid.slotNo = nextmid.slotNo;
            return true;
        } else {

            status = nextDataPage();

            if (status==true){
                mid.pageNo.pid = usermid.pageNo.pid;
                mid.slotNo = usermid.slotNo;
            }

        }
        return true;
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
        }

    } // end of unpinPage





    /**
     * Initialize a stream of maps on bigtable.
     */
    private BTFileScan BTreeFileScan;
    public Stream(bigt bigtable, int orderType, String rowFilter,
                   String columnFilter, String valueFilter)
            throws IOException,
            InvalidMapSizeException {

        // get the key filter
        String[] rFilter = getKeyFilter(rowFilter);
        String[] cFilter = getKeyFilter(columnFilter);
        String[] vFilter = getKeyFilter(valueFilter);

//        // get the type of big table
//        int bigtType = bigtable.getBigtType();
//        BTreeFile btfile = null;
//        switch (bigtType){
//            case 1: // no index
//                break;
//            case 2: // one btree to index row labels
//
//                BTreeFileScan = btfile.new_scan(new StringKey(rFilter[0]),cFilter[0]);
//        }

        // order type:
        // type 3: first ordered in row label, then time stamp
        // type 4:  first ordered in column label, then time stamp

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
}

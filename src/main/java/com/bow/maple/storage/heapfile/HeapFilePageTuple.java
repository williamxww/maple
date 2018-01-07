package com.bow.maple.storage.heapfile;


import com.bow.lab.storage.IPageStructure;
import com.bow.lab.storage.heap.PageTupleUtil;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.relations.ColumnInfo;

import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.util.ExtensionLoader;

import java.util.List;


/**
 */
public class HeapFilePageTuple extends PageTuple {

    private IPageStructure pageStructure = ExtensionLoader.getExtensionLoader(IPageStructure.class).getExtension();


    /**
     * The slot that this tuple corresponds to.  The tuple doesn't actually
     * manipulate the slot table directly; that is for the
     * {@link HeapFileTableManager} to deal with.
     */
    private int slot;


    /**
     * Construct a new tuple object that is backed by the data in the database
     * page.  This tuple is able to be read from or written to.
     *
     * @param tblFileInfo details of the table that this tuple is stored in
     *
     * @param dbPage the specific database page that holds the tuple
     *
     * @param slot the slot number of the tuple
     *
     * @param pageOffset the offset of the tuple's actual data in the page
     */
    public HeapFilePageTuple(TableFileInfo tblFileInfo, DBPage dbPage, int slot,
                             int pageOffset) {
        super(dbPage, pageOffset, tblFileInfo.getSchema().getColumnInfos());

        if (slot < 0) {
            throw new IllegalArgumentException(
                "slot must be nonnegative; got " + slot);
        }

        this.slot = slot;
    }


    /**
     * This method returns an external reference to the tuple, which references
     * the page number and slot-offset of the tuple.
     *
     * @return a file-pointer that can be used to look up this tuple
     */
    @Override
    public FilePointer getExternalReference() {
        return new FilePointer(getDBPage().getPageNo(),
                pageStructure.getSlotOffset(slot));
    }



    protected void insertTupleDataRange(int off, int len) {
        pageStructure.insertTupleDataRange(this.getDBPage(), off, len);
    }


    protected void deleteTupleDataRange(int off, int len) {
        pageStructure.deleteTupleDataRange(this.getDBPage(), off, len);
    }


    public int getSlot() {
        return slot;
    }


    public static HeapFilePageTuple storeNewTuple(TableFileInfo tblInfo,
        DBPage dbPage, int slot, int pageOffset, Tuple tuple) {

        List<ColumnInfo> colInfos = tblInfo.getSchema().getColumnInfos();
        PageTupleUtil.storeTuple(dbPage, pageOffset, colInfos, tuple);

        return new HeapFilePageTuple(tblInfo, dbPage, slot, pageOffset);
    }
}

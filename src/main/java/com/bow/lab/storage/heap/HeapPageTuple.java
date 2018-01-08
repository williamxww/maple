package com.bow.lab.storage.heap;

import com.bow.lab.storage.IPageStructure;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.util.ExtensionLoader;

/**
 * 在PageTuple的基础上扩展了 slot字段
 */
public class HeapPageTuple extends PageTuple {

    /**
     * heap page的存储结构
     */
    private IPageStructure pageStructure = ExtensionLoader.getExtensionLoader(IPageStructure.class).getExtension();

    /**
     * HeapFilePageTuple对应的槽位号，slot里放置的是tuple的偏移量
     */
    private int slot;

    /**
     * Construct a new tuple object that is backed by the data in the database
     * page. This tuple is able to be read from or written to.
     *
     * @param tblFileInfo details of the table that this tuple is stored in
     * @param dbPage the specific database page that holds the tuple
     * @param slot the slot number of the tuple
     * @param pageOffset the offset of the tuple's actual data in the page
     */
    public HeapPageTuple(TableFileInfo tblFileInfo, DBPage dbPage, int slot, int pageOffset) {
        super(dbPage, pageOffset, tblFileInfo.getSchema().getColumnInfos());
        if (slot < 0) {
            throw new IllegalArgumentException("slot must be nonnegative; got " + slot);
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
        return new FilePointer(getDBPage().getPageNo(), pageStructure.getSlotOffset(slot));
    }

    @Override
    protected void insertTupleDataRange(int off, int len) {
        pageStructure.insertTupleDataRange(this.getDBPage(), off, len);
    }

    @Override
    protected void deleteTupleDataRange(int off, int len) {
        pageStructure.deleteTupleDataRange(this.getDBPage(), off, len);
    }

    public int getSlot() {
        return slot;
    }
}

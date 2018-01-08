package com.bow.lab.storage;

import com.bow.lab.storage.heap.HeapPageTuple;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.TableFileInfo;

/**
 * 此类用于数据页的管理，主要体现在数据在DBPage上的结构
 * @author vv
 * @since 2018/1/7.
 */
public interface IPageStructure {

    /**
     * 用户特定的数据结构init new page
     * 
     * @param dbPage
     */
    void initNewPage(DBPage dbPage);

    /**
     * 剩余空间
     *
     * @return
     */
    int getFreeSpaceInPage(DBPage dbPage);

    /**
     * 合法性检查
     */
    void sanityCheck(DBPage dbPage);

    /**
     * 插入一个数据段
     *
     * @param dbPage
     * @param off
     * @param len
     */
    void insertTupleDataRange(DBPage dbPage, int off, int len);

    /**
     * 删除数据段
     *
     * @param dbPage
     * @param off
     * @param len
     */
    void deleteTupleDataRange(DBPage dbPage, int off, int len);

    int allocNewTuple(DBPage dbPage, int len);



    /**
     * 删除指定的tuple
     *
     * @param dbPage
     * @param slot
     */
    void deleteTuple(DBPage dbPage, int slot);

    /**
     * 获取指定tuple的真实长度
     *
     * @param dbPage
     * @param slot
     * @return
     */
    int getTupleLength(DBPage dbPage, int slot);

    //-------------------------------上面的为公有，下面的为heapPageStructure特有---------

    /**
     * 将指定tuple放置到page的指定位置上
     * @param tblInfo
     * @param dbPage
     * @param slot
     * @param pageOffset
     * @param tuple
     * @return
     */
    Tuple storeNewTuple(TableFileInfo tblInfo, DBPage dbPage, int slot, int pageOffset,
                        Tuple tuple);
    /**
     * 获取总slot 数量
     *
     * @return 总slot数量
     */
    int getNumSlots(DBPage dbPage);

    void setNumSlots(DBPage dbPage, int numSlots);

    /**
     * 获取指定slot的值，即tuple的起始偏移位置或是
     * {@link com.bow.lab.storage.heap.HeapPageStructure#EMPTY_SLOT}
     *
     * @param slot slot
     * @return 对应tuple的偏移量
     */
    int getSlotValue(DBPage dbPage, int slot);

    /**
     * 获取slot的偏移量
     * @param slot
     * @return
     */
    int getSlotOffset(int slot);

    /**
     * 将offset转换为slot
     *
     * @param offset 在整个page中的offset
     * @return slot
     * @throws IllegalArgumentException e
     */
    int getSlotIndexFromOffset(DBPage dbPage, int offset) throws IllegalArgumentException;
}

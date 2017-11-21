package com.bow.lab.storage;

/**
 * @author vv
 * @since 2017/11/21.
 */
public interface IPage {

    int getFreeSpaceInPage();

    void sanityCheck();

    void insertTupleDataRange(int off, int len);

    void deleteTupleDataRange(int off, int len);

    int allocNewTuple(int len);

    void deleteTuple(int slot);

    int getTupleLength(int slot);
}

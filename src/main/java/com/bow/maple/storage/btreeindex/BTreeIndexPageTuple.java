package com.bow.maple.storage.btreeindex;


import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.PageTuple;

import java.util.List;


/**
 * This class uses the <tt>PageTuple</tt> class functionality to access and
 * manipulate keys stored in a B<sup>+</sup> tree index file.
 */
public class BTreeIndexPageTuple extends PageTuple {
    public BTreeIndexPageTuple(DBPage dbPage, int pageOffset,
                               List<ColumnInfo> colInfos) {
        super(dbPage, pageOffset, colInfos);
    }


    @Override
    protected void insertTupleDataRange(int off, int len) {
        throw new UnsupportedOperationException(
            "B+ Tree index tuples don't support updating or resizing.");
    }


    @Override
    protected void deleteTupleDataRange(int off, int len) {
        throw new UnsupportedOperationException(
            "B+ Tree index tuples don't support updating or resizing.");
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("BTPT[");

        boolean first = true;
        for (int i = 0; i < getColumnCount(); i++) {
            if (first)
                first = false;
            else
                buf.append(',');

            Object obj = getColumnValue(i);
            if (obj == null)
                buf.append("NULL");
            else
                buf.append(obj);
        }

        buf.append(']');

        return buf.toString();
    }
}

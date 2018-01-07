package com.bow.lab.storage;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FilePointer;

import java.util.List;

/**
 * @author vv
 * @since 2018/1/7.
 */
public interface IPageTuple extends Tuple {

    /**
     * 此tuple的外部引用，外界能根据FilePointer定位到此tuple
     * @return
     */
    FilePointer getExternalReference();

    /**
     * tuple所在数据页
     * @return
     */
    DBPage getDBPage();

    /**
     * tuple的偏移量
     * @return
     */
    int getOffset();

    /**
     * tuple的结束位置
     * @return
     */
    int getEndOffset();

    /**
     * tuple的长度
     * @return
     */
    int getSize();

    /**
     * tuple的列信息
     * @return
     */
    List<ColumnInfo> getColumnInfos();
}

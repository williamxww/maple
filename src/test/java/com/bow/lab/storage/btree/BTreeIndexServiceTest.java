package com.bow.lab.storage.btree;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.bow.lab.indexes.IndexFileInfo;
import com.bow.lab.indexes.IndexInfo;
import com.bow.lab.storage.IIndexService;
import com.bow.lab.transaction.AbstractTest;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.storage.btreeindex.BTreeIndexPageTuple;

/**
 * @author vv
 * @since 2018/2/9.
 */
public class BTreeIndexServiceTest extends AbstractTest {

    private static final String TABLE_NAME = "person";

    private static final String IDX_FILE_NAME = "idx_test";

    private IIndexService indexService;

    private void clean() {
        File file = new File("test/" + IDX_FILE_NAME);
        if (file.exists()) {
            file.delete();
        }
    }

    @Before
    public void setup() throws IOException {
        clean();
        super.setup();
        indexService = new BTreeIndexService();
    }

    @Test
    public void initIndexInfo() throws Exception {

        IndexFileInfo idxFileInfo = getIndexFileInfo();
        indexService.initIndexInfo(idxFileInfo);

        // 将索引刷到磁盘
        storageService.flushDBFile(idxFileInfo.getDBFile());
    }

    private IndexFileInfo getIndexFileInfo() throws Exception {
        TableFileInfo tblFileInfo = new TableFileInfo(TABLE_NAME);
        tblFileInfo.setFileType(DBFileType.BTREE_INDEX_FILE);

        // 创建索引的实体文件
        DBFile dbFile = storageService.createDBFile(IDX_FILE_NAME, DBFileType.BTREE_INDEX_FILE,
                DBFile.DEFAULT_PAGESIZE);
        IndexFileInfo idxFileInfo = new IndexFileInfo("idx_v", tblFileInfo, (IndexInfo) null);
        idxFileInfo.setDBFile(dbFile);
        return idxFileInfo;
    }

    @Test
    public void addTuple() throws Exception {
        PageTuple tuple = new BTreeIndexPageTuple(null, 0, null);
        IndexFileInfo idxFileInfo = getIndexFileInfo();
        indexService.addTuple(idxFileInfo, tuple);
    }

    @Test
    public void deleteTuple() throws Exception {
    }

}
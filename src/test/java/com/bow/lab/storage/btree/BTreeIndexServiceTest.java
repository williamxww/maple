package com.bow.lab.storage.btree;

import static com.bow.lab.storage.TestConstant.IDX_AGE;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.bow.lab.indexes.IndexFileInfo;
import com.bow.lab.indexes.IndexInfo;
import com.bow.lab.storage.IIndexService;
import com.bow.lab.storage.ITableService;
import com.bow.lab.storage.SimpleTableService;
import com.bow.lab.storage.SimpleTableServiceTest;
import com.bow.lab.storage.TestConstant;
import com.bow.lab.transaction.AbstractTest;
import com.bow.lab.transaction.ITransactionService;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.util.ExtensionLoader;

/**
 * @author vv
 * @since 2018/2/9.
 */
public class BTreeIndexServiceTest extends AbstractTest {

    private ITableService tableService;

    private ITransactionService txnService;

    private IIndexService indexService;

    private void clean() {
        File file = new File("test/" + IDX_AGE);
        if (file.exists()) {
            file.delete();
        }
    }

    @Before
    public void setup() throws IOException {
        clean();
        super.setup();
        txnService = ExtensionLoader.getExtensionLoader(ITransactionService.class).getExtension();
        txnService.initialize();
        tableService = new SimpleTableService(storageService, txnService);
        indexService = new BTreeIndexService();
    }

    private IndexFileInfo getIndexFileInfo(TableFileInfo tblFileInfo) throws Exception {
        // 创建索引的实体文件
        DBFile dbFile = storageService.createDBFile(IDX_AGE, DBFileType.BTREE_INDEX_FILE, DBFile.MIN_PAGESIZE);
        IndexFileInfo idxFileInfo = new IndexFileInfo(IDX_AGE, tblFileInfo, (IndexInfo) null);
        idxFileInfo.setDBFile(dbFile);
        return idxFileInfo;
    }

    /**
     * 先要运行{@link SimpleTableServiceTest#createTable()} ,
     * {@link SimpleTableServiceTest#addTuple()}构造数据
     * 
     * @throws Exception
     */
    @Test
    public void addTuple() throws Exception {
        // 从表中取出第一个tuple
        TableFileInfo tblFileInfo = tableService.openTable(TestConstant.TABLE_NAME);
        PageTuple tuple = (PageTuple) tableService.getFirstTuple(tblFileInfo);
        // 生成一个IndexFileInfo
        IndexFileInfo idxFileInfo = getIndexFileInfo(tblFileInfo);
        // 将tuple放入到index中
        indexService.addTuple(idxFileInfo, tuple);
        // 关闭文件
        storageService.flushDBFile(idxFileInfo.getDBFile());
    }

    @Test
    public void deleteTuple() throws Exception {
    }

}
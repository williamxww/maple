package com.bow.lab.storage;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.bow.lab.transaction.AbstractTest;
import com.bow.lab.transaction.ITransactionService;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.util.ExtensionLoader;

/**
 * @author vv
 * @since 2017/11/11.
 */
public class SimpleTableServiceTest extends AbstractTest {

    private SimpleTableService tableService;
    private ITransactionService txnService;

    @Before
    public void setup() throws IOException {
        super.setup();
        txnService = ExtensionLoader.getExtensionLoader(ITransactionService.class).getExtension();
        txnService.initialize();
        tableService = new SimpleTableService(storageService, txnService);
    }

    @Test
    public void createTable() throws Exception {
        String tableName = "table1";
        TableFileInfo tblFileInfo = new TableFileInfo(tableName);
        tblFileInfo.setFileType(DBFileType.FRM_FILE);
        TableSchema schema = tblFileInfo.getSchema();
        ColumnType intType = new ColumnType(SQLDataType.INTEGER);
        ColumnInfo c1 = new ColumnInfo("id",tableName,intType);
        ColumnInfo c2 = new ColumnInfo("age",tableName,intType);
        schema.addColumnInfo(c1);
        schema.addColumnInfo(c2);
        txnService.startTransaction(true);
        tableService.createTable(tblFileInfo);
        // 刷到磁盘
        tableService.closeTable(tblFileInfo);
        txnService.commitTransaction();
    }

}
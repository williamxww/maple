package com.bow.lab.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bow.lab.storage.heap.HeapPageStructure;
import com.bow.maple.expressions.TupleLiteral;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.heapfile.HeapFilePageTuple;
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
 * heapFile tableService's operation
 * 
 * @author vv
 * @since 2017/11/11.
 */
public class SimpleTableServiceTest extends AbstractTest {

    private static final String TABLE_NAME = "person";

    private ITableService tableService;

    private ITransactionService txnService;

    @Before
    public void setup() throws IOException {
        // 组件交由容器管理
        super.setup();
        txnService = ExtensionLoader.getExtensionLoader(ITransactionService.class).getExtension();
        txnService.initialize();
        // 注入heap结构
        IPageStructure heapStructure = new HeapPageStructure();
        ExtensionLoader.getExtensionLoader(IPageStructure.class).putExtension(heapStructure);
        tableService = new SimpleTableService(storageService, txnService);
    }

    @Test
    public void createTable() throws Exception {
        TableFileInfo tblFileInfo = new TableFileInfo(TABLE_NAME);
        tblFileInfo.setFileType(DBFileType.FRM_FILE);
        TableSchema schema = tblFileInfo.getSchema();
        ColumnType intType = new ColumnType(SQLDataType.INTEGER);
        ColumnInfo c1 = new ColumnInfo("id", TABLE_NAME, intType);
        ColumnInfo c2 = new ColumnInfo("age", TABLE_NAME, intType);
        schema.addColumnInfo(c1);
        schema.addColumnInfo(c2);
        // 开启事务
        txnService.startTransaction(true);
        // 建表
        tableService.createTable(tblFileInfo);
        txnService.commitTransaction();
        // 刷到磁盘
        tableService.closeTable(tblFileInfo);

    }

    /**
     * 新增数据
     * 
     * @throws Exception
     */
    @Test
    public void addTuple() throws Exception {
        // 获取此表的TableFileInfo
        TableFileInfo tblFileInfo = tableService.openTable(TABLE_NAME);
        TupleLiteral tuple = new TupleLiteral();
        tuple.addValue(1);// id
        tuple.addValue(28);// age
        txnService.startTransaction(true);
        tableService.addTuple(tblFileInfo, tuple);
        txnService.commitTransaction();
        // 刷到磁盘
        tableService.closeTable(tblFileInfo);
    }

    @Test
    public void getTuple() throws Exception {
        TableFileInfo tblFileInfo = tableService.openTable(TABLE_NAME);
        FilePointer fp = new FilePointer(1, 2);
        Tuple tuple = tableService.getTuple(tblFileInfo, fp);
        // 获取age的值
        System.out.println(tuple.getColumnValue(1));
    }

    @Test
    public void getFirstTuple() throws Exception {
        TableFileInfo tblFileInfo = tableService.openTable(TABLE_NAME);
        Tuple tuple = tableService.getFirstTuple(tblFileInfo);
        // 获取age的值
        System.out.println(tuple.getColumnValue(1));
    }

    @Test
    public void getNextTuple() throws Exception {
        TableFileInfo tblFileInfo = tableService.openTable(TABLE_NAME);
        Tuple tuple = tableService.getFirstTuple(tblFileInfo);
        Tuple nextTuple = tableService.getNextTuple(tblFileInfo, tuple);
        // 获取age的值
        System.out.println(nextTuple.getColumnValue(1));

    }

    @Test
    public void updateTuple() throws Exception {
        TableFileInfo tblFileInfo = tableService.openTable(TABLE_NAME);
        Tuple tuple = tableService.getFirstTuple(tblFileInfo);
        Map<String, Object> newValues = new HashMap<>();
        newValues.put("id", 1);
        newValues.put("age", 20);

        txnService.startTransaction(true);
        // 更新tuple
        tableService.updateTuple(tblFileInfo, tuple, newValues);
        txnService.commitTransaction();
        // 刷到磁盘
        tableService.closeTable(tblFileInfo);
    }

    @Test
    public void deleteTuple() throws Exception {
        TableFileInfo tblFileInfo = tableService.openTable(TABLE_NAME);
        Tuple tuple = tableService.getFirstTuple(tblFileInfo);
        txnService.startTransaction(true);
        // 删除tuple
        tableService.deleteTuple(tblFileInfo, tuple);
        txnService.commitTransaction();
        // 刷到磁盘
        tableService.closeTable(tblFileInfo);

    }

}
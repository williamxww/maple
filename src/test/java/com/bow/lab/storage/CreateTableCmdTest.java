package com.bow.lab.storage;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.server.EventDispatcher;
import com.bow.maple.storage.StorageManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/8.
 */
public class CreateTableCmdTest {

    private String tableName = "TEST";

    @Before
    public void setup() throws IOException {
        StorageManager.init();
    }

    @After
    public void destroy() throws IOException {
        StorageManager.shutdown();
    }

    @Test
    public void create() throws Exception {

        ColumnType strType = new ColumnType(SQLDataType.VARCHAR);
        ColumnType intType = new ColumnType(SQLDataType.INTEGER);
        strType.setLength(10);
        ColumnInfo id = new ColumnInfo("ID", tableName, intType);
        ColumnInfo name = new ColumnInfo("NAME", tableName, strType);

        CreateTableCmd command = new CreateTableCmd();
        command.setTableName(tableName);
        command.addColumn(id);
        command.addColumn(name);

        // 开启事务并执行命令
        EventDispatcher eventDispatch = EventDispatcher.getInstance();
        eventDispatch.fireBeforeCommandExecuted(command);
        command.execute();
        eventDispatch.fireAfterCommandExecuted(command);
    }
}
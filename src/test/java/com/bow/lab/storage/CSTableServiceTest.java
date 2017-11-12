package com.bow.lab.storage;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.FileManager;
import com.bow.maple.storage.TableFileInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author vv
 * @since 2017/11/11.
 */
public class CSTableServiceTest {

    private CSTableService service;

    @Before
    public void setup(){
        File dir = new File("test");
        FileManager fileManager = new FileManager(dir);
        BufferService bufferManager = new BufferService(fileManager);
        StorageService storageService = new StorageService(fileManager,bufferManager);
        service = new CSTableService(storageService);
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
        service.createTable(tblFileInfo);
        // 刷到磁盘
        service.closeTable(tblFileInfo);
    }

}
package com.bow.lab.storage;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.InvalidFilePointerException;
import com.bow.maple.storage.PageWriter;
import com.bow.maple.storage.StorageManager;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.storage.TableManager;
import com.bow.maple.storage.heapfile.HeaderPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author vv
 * @since 2017/11/10.
 */
public class CSTableService implements ITableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSTableService.class);
    private IStorageService storageService;

    private FileManager fileManager;

    private Map<String, TableFileInfo> openTables = new HashMap<String, TableFileInfo>();

    @Override
    public void createTable(TableFileInfo tblFileInfo) throws IOException {

        int pageSize = StorageManager.getCurrentPageSize();

        String tableName = tblFileInfo.getTableName();
        String tblFileName = getTableFileName(tableName);

        // Get the file type from the table file info object.
        DBFileType type = tblFileInfo.getFileType();

        // 列式数据库页大一些
        if (type == DBFileType.CS_DATA_FILE) {
            pageSize = DBFile.MAX_PAGESIZE;
        }

        // 创建存储文件，此处只会写入 dbFileType和pageSize
        DBFile dbFile = fileManager.createDBFile(tblFileName, type, pageSize);
        LOGGER.debug("Created new DBFile for table " + tableName + " at path " + dbFile.getDataFile());

        tblFileInfo.setDBFile(dbFile);
        // 列式数据库还需要为每列创建一个文件
        if (type == DBFileType.CS_DATA_FILE) {
            // 列式存储，每个列形成一个文件
            TableSchema schema = tblFileInfo.getSchema();
            for (int i = 0; i < schema.numColumns(); i++) {
                dbFile = fileManager.createDBFileinDir(tableName,
                        schema.getColumnInfo(i).getColumnName().toString() + ".tbl", type, pageSize);
                LOGGER.debug("Created new DBFile for table " + tableName + " column "
                        + schema.getColumnInfo(i).getColumnName() + " at path " + dbFile.getDataFile());
                tblFileInfo.addDBFile(dbFile);
            }
        }

        // Cache this table since it's now considered "open".
        openTables.put(tblFileInfo.getTableName(), tblFileInfo);

        // 用tableManager初始化数据文件
        initFile(tblFileInfo);
    }

    private void initFile(TableFileInfo tblFileInfo) throws IOException{
        String tableName = tblFileInfo.getTableName();
        DBFile dbFile = tblFileInfo.getDBFile();
        TableSchema schema = tblFileInfo.getSchema();

        DBPage headerPage = storageService.loadDBPage(dbFile, 0);
        PageWriter hpWriter = new PageWriter(headerPage);
        hpWriter.setPosition(HeaderPage.OFFSET_NCOLS);
        hpWriter.writeByte(schema.numColumns());
        for (ColumnInfo colInfo : schema.getColumnInfos()) {
            ColumnType colType = colInfo.getType();
            //写类型
            hpWriter.writeByte(colType.getBaseType().getTypeID());

            //写列宽如VARCHAR(30)中的30
            if (colType.hasLength()) {
                hpWriter.writeShort(colType.getLength());
            }
            //写列名称
            hpWriter.writeVarString255(colInfo.getName());
        }
        int schemaSize = hpWriter.getPosition() - HeaderPage.OFFSET_NCOLS;
        headerPage.writeShort(HeaderPage.OFFSET_SCHEMA_SIZE, schemaSize);

        //写WAL
//        storageManager.logDBPageWrite(headerPage);
//        storageManager.unpinDBPage(headerPage);
    }

    @Override
    public void closeTable(TableFileInfo tblFileInfo) throws IOException {

    }

    @Override
    public Tuple getFirstTuple(TableFileInfo tblFileInfo) throws IOException {
        return null;
    }

    @Override
    public Tuple getNextTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {
        return null;
    }

    @Override
    public Tuple getTuple(TableFileInfo tblFileInfo, FilePointer fptr) throws InvalidFilePointerException, IOException {
        return null;
    }

    @Override
    public Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {
        return null;
    }

    @Override
    public void updateTuple(TableFileInfo tblFileInfo, Tuple tup, Map<String, Object> newValues) throws IOException {

    }

    @Override
    public void deleteTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {

    }


    private String getTableFileName(String tableName) {
        return tableName + ".tbl";
    }
}

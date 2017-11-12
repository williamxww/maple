package com.bow.lab.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bow.maple.transactions.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.InvalidFilePointerException;
import com.bow.maple.storage.PageWriter;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.storage.heapfile.HeaderPage;

/**
 * @author vv
 * @since 2017/11/10.
 */
public class CSTableService implements ITableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSTableService.class);

    private static final int PAGE_SIZE = DBFile.DEFAULT_PAGESIZE;

    private IStorageService storageService;

    private TransactionManager transactionManager;

    private Map<String, TableFileInfo> openTables = new HashMap<String, TableFileInfo>();

    public CSTableService(IStorageService storageService) {
        this.storageService = storageService;
    }

    /**
     * <pre>
     * |    1B  |       1B     |   4B     |
     * |FileType|encodePageSize|schemaSize|
     *
     * |  1B      |  1B  |   1B     |  XB   |  1B  |   1B     |  XB   |...
     * |numColumns|TypeID|colNameLen|colName|TypeID|colNameLen|colName|...
     * </pre>
     * @param tblFileInfo 表信息
     * @throws IOException 文件处理异常
     */
    @Override
    public void createTable(TableFileInfo tblFileInfo) throws IOException {

        String tableName = tblFileInfo.getTableName();
        String tblFileName = getTableFileName(tableName);
        DBFileType type = tblFileInfo.getFileType();

        // 创建存储文件，此处只会写入 dbFileType和pageSize
        DBFile dbFile = storageService.createDBFile(tblFileName, type, PAGE_SIZE);
        LOGGER.debug("Created new DBFile for table " + tableName + " at path " + dbFile.getDataFile());
        tblFileInfo.setDBFile(dbFile);

        // Cache this table since it's now considered "open".
        openTables.put(tblFileInfo.getTableName(), tblFileInfo);

        // 初始化frm文件
        TableSchema schema = tblFileInfo.getSchema();
        DBPage headerPage = storageService.loadDBPage(dbFile, 0);
        PageWriter hpWriter = new PageWriter(headerPage);
        hpWriter.setPosition(HeaderPage.OFFSET_NCOLS);
        hpWriter.writeByte(schema.numColumns());
        for (ColumnInfo colInfo : schema.getColumnInfos()) {
            ColumnType colType = colInfo.getType();
            // 写类型
            hpWriter.writeByte(colType.getBaseType().getTypeID());
            // 写列宽如VARCHAR(30)中的30
            if (colType.hasLength()) {
                hpWriter.writeShort(colType.getLength());
            }
            // 写列名称
            hpWriter.writeVarString255(colInfo.getName());
        }
        int schemaSize = hpWriter.getPosition() - HeaderPage.OFFSET_NCOLS;
        headerPage.writeShort(HeaderPage.OFFSET_SCHEMA_SIZE, schemaSize);

        // 写WAL
        transactionManager.recordPageUpdate(headerPage);
    }

    @Override
    public void closeTable(TableFileInfo tblFileInfo) throws IOException {

        // Flush all open pages for the table.
        for (DBFile dbf : tblFileInfo.dbFiles()) {
            storageService.flushDBFile(dbf);
        }
        // Remove this table from the cache since it's about to be closed.
        openTables.remove(tblFileInfo.getTableName());

        for (DBFile dbf : tblFileInfo.dbFiles()) {
            storageService.closeDBFile(dbf);
        }
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
        return tableName + ".frm";
    }
}

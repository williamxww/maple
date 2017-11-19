package com.bow.lab.storage;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bow.lab.transaction.ITransactionService;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.heapfile.DataPage;
import com.bow.maple.storage.heapfile.HeapFilePageTuple;
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
public class SimpleTableService implements ITableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTableService.class);

    private static final int PAGE_SIZE = DBFile.DEFAULT_PAGESIZE;

    private IStorageService storageService;

    private ITransactionService txnService;

    private Map<String, TableFileInfo> openTables = new HashMap<String, TableFileInfo>();

    public SimpleTableService(IStorageService storageService, ITransactionService txnService) {
        this.storageService = storageService;
        this.txnService = txnService;
    }

    /**
     * <pre>
     * |    1B  |       1B     |   4B     |
     * |FileType|encodePageSize|schemaSize|
     *
     * |  1B      |  1B  |   1B     |  XB   |  1B  |   1B     |  XB   |...
     * |numColumns|TypeID|colNameLen|colName|TypeID|colNameLen|colName|...
     * </pre>
     * 
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
        txnService.recordPageUpdate(headerPage);
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

    /**
     * Find out how large the new tuple will be, so we can find a page to store
     * it.<br/>
     * Find a page with space for the new tuple.<br/>
     * Generate the data necessary for storing the tuple into the file.
     */
    @Override
    public Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {

        DBFile dbFile = tblFileInfo.getDBFile();
        int tupSize = PageTuple.getTupleStorageSize(tblFileInfo.getSchema().getColumnInfos(), tup);
        LOGGER.debug("Adding new tuple of size " + tupSize + " bytes.");

        // 确保一页能放下一个tuple，每个tuple还需要对应一个slot(2 Byte)
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize + " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // Search for a page to put the tuple in. If we hit the end of the
        // data file, create a new page.
        int pageNo = 1;
        DBPage dbPage = null;
        while (true) {
            try {
                dbPage = storageService.loadDBPage(dbFile, pageNo);
            } catch (EOFException eofe) {
                // 到文件尾部了，就跳出循环
                LOGGER.debug("Reached end of data file without finding space for new tuple.");
                break;
            }
            int freeSpace = DataPage.getFreeSpaceInPage(dbPage);
            LOGGER.trace("Page {} has {} bytes of free space.", pageNo, freeSpace);
            // 每个tuple还需要对应一个slot(2 Byte)
            if (freeSpace >= tupSize + 2) {
                LOGGER.trace("Found space for new tuple in page " + pageNo + ".");
                break;
            }

            // 到此处说明本页的空间不够，调到下一页。
            storageService.unpinDBPage(dbPage);
            dbPage = null;
            pageNo++;
        }

        if (dbPage == null) {
            // Try to create a new page at the end of the file. In this
            // circumstance, pageNo is *just past* the last page in the data
            // file.
            LOGGER.debug("Creating new page " + pageNo + " to store new tuple.");
            dbPage = storageService.loadDBPage(dbFile, pageNo, true);
            DataPage.initNewPage(dbPage);
        }

        int slot = DataPage.allocNewTuple(dbPage, tupSize);
        int tupOffset = DataPage.getSlotValue(dbPage, slot);

        LOGGER.debug("New tuple will reside on page {}, slot {}.", pageNo, slot);

        HeapFilePageTuple pageTup = HeapFilePageTuple.storeNewTuple(tblFileInfo, dbPage, slot, tupOffset, tup);

        DataPage.sanityCheck(dbPage);
        txnService.recordPageUpdate(dbPage);
        // TODO: Really shouldn't unpin the page; the caller will want it.
        // TODO: Maybe need to change how we do this to make unpinning easier.
        return pageTup;
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

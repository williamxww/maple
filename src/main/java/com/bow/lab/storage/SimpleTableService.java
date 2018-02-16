package com.bow.lab.storage;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bow.lab.storage.heap.HeapPageStructure;
import com.bow.lab.storage.heap.HeapPageTuple;
import com.bow.lab.storage.heap.PageTupleUtil;
import com.bow.lab.transaction.ITransactionService;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.ForeignKeyColumnIndexes;
import com.bow.maple.relations.KeyColumnIndexes;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.Schema;
import com.bow.maple.relations.TableConstraintType;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.InvalidFilePointerException;
import com.bow.maple.storage.PageReader;
import com.bow.maple.storage.PageWriter;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.storage.heapfile.HeaderPage;
import com.bow.maple.util.ExtensionLoader;

/**
 * @author vv
 * @since 2017/11/10.
 */
public class SimpleTableService implements ITableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTableService.class);

    private static final int PAGE_SIZE = DBFile.DEFAULT_PAGESIZE;

    private IStorageService storageService = ExtensionLoader.getExtensionLoader(IStorageService.class).getExtension();

    private ITransactionService txnService = ExtensionLoader.getExtensionLoader(ITransactionService.class)
            .getExtension();

    /**
     * 页的数据结构
     */
    private IPageStructure pageStructure = new HeapPageStructure();

    /**
     * 已打开的表
     */
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
     *
     * |       1B     |       1B     |   1B  | 1B  | 1B  |...|   1B    |
     * |constraintType|constraintName|colSize|conNo|conNo|...|indexName|
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

        // 写主键，候选键，外键，其他非唯一索引
        int numConstraints = schema.numCandidateKeys() + schema.numForeignKeys();
        KeyColumnIndexes pk = schema.getPrimaryKey();
        if (pk != null) {
            numConstraints++;
        }
        LOGGER.debug("Writing " + numConstraints + " constraints");
        int constraintStartIndex = hpWriter.getPosition();
        hpWriter.writeByte(numConstraints);
        // 写主键
        if (pk != null) {
            writeKey(hpWriter, TableConstraintType.PRIMARY_KEY, pk);
        }
        // 写候选键(唯一索引)
        for (KeyColumnIndexes ck : schema.getCandidateKeys()) {
            writeKey(hpWriter, TableConstraintType.UNIQUE, ck);
        }
        // 写外键
        for (ForeignKeyColumnIndexes fk : schema.getForeignKeys()) {
            writeForeignKey(hpWriter, fk);
        }
        LOGGER.debug("Constraints occupy " + (hpWriter.getPosition() - constraintStartIndex) + " bytes in the schema");

        // 写出schema大小
        int schemaSize = hpWriter.getPosition() - HeaderPage.OFFSET_NCOLS;
        headerPage.writeShort(HeaderPage.OFFSET_SCHEMA_SIZE, schemaSize);

        // 写WAL
        txnService.recordPageUpdate(headerPage);
    }

    /**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     *
     * @param hpWriter the writer being used to write the table's schema to its
     *        header page
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @param key a specification of what columns appear in the key
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    private void writeKey(PageWriter hpWriter, TableConstraintType type, KeyColumnIndexes key) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            LOGGER.debug(" * Primary key {}, enforced with index {}", key, key.getIndexName());
        } else if (type == TableConstraintType.UNIQUE) {
            LOGGER.debug(" * Candidate key {}, enforced with index {}", key, key.getIndexName());
        } else {
            throw new IllegalArgumentException("Invalid TableConstraintType value " + type);
        }
        int typeVal = type.getTypeID();
        String cName = key.getConstraintName();
        if (cName != null) {
            typeVal |= 0x80;
        }
        hpWriter.writeByte(typeVal);
        if (cName != null) {
            hpWriter.writeVarString255(cName);
        }
        hpWriter.writeByte(key.size());
        for (int i = 0; i < key.size(); i++) {
            hpWriter.writeByte(key.getCol(i));
        }
        // This should always be specified.
        hpWriter.writeVarString255(key.getIndexName());
    }

    private void writeForeignKey(PageWriter hpWriter, ForeignKeyColumnIndexes key) {
        LOGGER.debug(" * Foreign key " + key);
        int type = TableConstraintType.FOREIGN_KEY.getTypeID();
        if (key.getConstraintName() != null) {
            type |= 0x80;
        }

        hpWriter.writeByte(type);
        if (key.getConstraintName() != null) {
            hpWriter.writeVarString255(key.getConstraintName());
        }

        hpWriter.writeVarString255(key.getRefTable());
        hpWriter.writeByte(key.size());
        for (int i = 0; i < key.size(); i++) {
            hpWriter.writeByte(key.getCol(i));
            hpWriter.writeByte(key.getRefCol(i));
        }
    }

    @Override
    public TableFileInfo openTable(String tableName) throws IOException {

        TableFileInfo tblFileInfo;
        tblFileInfo = openTables.get(tableName);
        if (tblFileInfo != null) {
            // 之前已经打开就直接返回
            return tblFileInfo;
        }

        // 从表结构文件中读取table file info
        String tblFileName = getTableFileName(tableName);
        DBFile dbFile = storageService.openDBFile(tblFileName);
        DBFileType type = dbFile.getType();
        LOGGER.debug("Opened DBFile for table {} at path {}. type:{}, page size:{}", tableName, dbFile.getDataFile(),
                type, dbFile.getPageSize());
        tblFileInfo = new TableFileInfo(tableName, dbFile);
        openTables.put(tableName, tblFileInfo);

        // 根据文件中的信息初始化tblFileInfo
        loadTableInfo(tblFileInfo);
        return tblFileInfo;
    }

    private void loadTableInfo(TableFileInfo tblFileInfo) throws IOException {

        String tableName = tblFileInfo.getTableName();
        DBFile dbFile = tblFileInfo.getDBFile();
        DBPage headerPage = storageService.loadDBPage(dbFile, 0);
        PageReader hpReader = new PageReader(headerPage);

        // 从指定位置开始读取
        hpReader.setPosition(HeaderPage.OFFSET_NCOLS);

        // 获取总列数
        int numCols = hpReader.readUnsignedByte();
        LOGGER.debug("Table has " + numCols + " columns.");
        if (numCols == 0)
            throw new IOException("Table must have at least one column.");

        // 设置table info中的schema
        TableSchema schema = tblFileInfo.getSchema();
        for (int iCol = 0; iCol < numCols; iCol++) {

            // 获取列类型
            byte sqlTypeID = hpReader.readByte();
            SQLDataType baseType = SQLDataType.findType(sqlTypeID);
            if (baseType == null) {
                throw new IOException("Unrecognized SQL type " + sqlTypeID + " for column " + iCol);
            }
            ColumnType colType = new ColumnType(baseType);
            if (colType.hasLength()) {
                // CHAR and VARCHAR 需要存字段长度
                colType.setLength(hpReader.readUnsignedShort());
            }

            // 获取列名
            String colName = hpReader.readVarString255();
            if (colName.length() == 0) {
                throw new IOException("Name of column " + iCol + " is unspecified.");
            }
            // 校验名字是否合法
            for (int iCh = 0; iCh < colName.length(); iCh++) {
                char ch = colName.charAt(iCh);
                if (iCh == 0 && !(Character.isLetter(ch) || ch == '_')
                        || iCh > 0 && !(Character.isLetterOrDigit(ch) || ch == '_')) {
                    throw new IOException(String.format(
                            "Name of column " + "%d \"%s\" has an invalid character at index %d.", iCol, colName, iCh));
                }
            }

            // 构造列信息
            ColumnInfo colInfo = new ColumnInfo(colName, tableName, colType);
            schema.addColumnInfo(colInfo);
        }

        // Read all details of key constraints, foreign keys, and indexes:
        int numConstraints = hpReader.readUnsignedByte();
        LOGGER.debug("Reading " + numConstraints + " constraints");
        for (int i = 0; i < numConstraints; i++) {
            int cTypeID = hpReader.readUnsignedByte();
            TableConstraintType cType = TableConstraintType.findType((byte) (cTypeID & 0x7F));
            if (cType == null) {
                throw new IOException("Unrecognized constraint-type value " + cTypeID);
            }
            switch (cType) {
                case PRIMARY_KEY:
                    schema.setPrimaryKey(readKey(hpReader, cTypeID, TableConstraintType.PRIMARY_KEY));
                    break;

                case UNIQUE:
                    schema.addCandidateKey(readKey(hpReader, cTypeID, TableConstraintType.UNIQUE));
                    break;

                case FOREIGN_KEY:
                    schema.addForeignKey(readForeignKey(hpReader, cTypeID));
                    break;

                default:
                    throw new IOException("Encountered unhandled constraint type " + cType);
            }
        }

        // 设置表的统计信息
        tblFileInfo.setStats(HeaderPage.getTableStats(headerPage, tblFileInfo));
        // unpin page
        storageService.unpinDBPage(headerPage);
    }

    /**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     *
     * @param hpReader the writer being used to write the table's schema to its
     *        header page
     *
     * @param typeID the unsigned-byte value read from the table's header page,
     *        corresponding to this key's type. Although this value is already
     *        parsed before calling this method, it also contains flags that
     *        this method handles, so it must be passed in as well.
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @return a specification of the key, including its name, what columns
     *         appear in the key, and what index is used to enforce the key
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    private KeyColumnIndexes readKey(PageReader hpReader, int typeID, TableConstraintType type) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            LOGGER.debug(" * Reading primary key");
        } else if (type == TableConstraintType.UNIQUE) {
            LOGGER.debug(" * Reading candidate key");
        } else {
            throw new IllegalArgumentException("Invalid TableConstraintType value " + type);
        }

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = hpReader.readVarString255();

        int keySize = hpReader.readUnsignedByte();
        int[] keyCols = new int[keySize];
        for (int i = 0; i < keySize; i++)
            keyCols[i] = hpReader.readUnsignedByte();

        // This should always be specified.
        String indexName = hpReader.readVarString255();

        KeyColumnIndexes key = new KeyColumnIndexes(indexName, keyCols);
        key.setConstraintName(constraintName);

        return key;
    }

    private ForeignKeyColumnIndexes readForeignKey(PageReader hpReader, int typeID) {
        LOGGER.debug(" * Reading foreign key");

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = hpReader.readVarString255();

        String refTableName = hpReader.readVarString255();
        int keySize = hpReader.readUnsignedByte();

        int[] keyCols = new int[keySize];
        int[] refCols = new int[keySize];
        for (int i = 0; i < keySize; i++) {
            keyCols[i] = hpReader.readUnsignedByte();
            refCols[i] = hpReader.readUnsignedByte();
        }

        ForeignKeyColumnIndexes fk = new ForeignKeyColumnIndexes(keyCols, refTableName, refCols);
        fk.setConstraintName(constraintName);

        return fk;
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
        if (tblFileInfo == null) {
            throw new IllegalArgumentException("tblFileInfo cannot be null");
        }

        DBFile dbFile = tblFileInfo.getDBFile();
        try {
            // 循环数据页，直至找到第一条记录为止
            for (int iPage = 1; /* nothing */ ; iPage++) {
                // 加载数据页
                DBPage dbPage = storageService.loadDBPage(dbFile, iPage);
                int numSlots = pageStructure.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // 如果是空slot就继续找
                    int offset = pageStructure.getSlotValue(dbPage, iSlot);
                    if (offset == HeapPageStructure.EMPTY_SLOT) {
                        continue;
                    }
                    // 找到后，返回
                    return new HeapPageTuple(tblFileInfo, dbPage, iSlot, offset);
                }

                // If we got here, the page has no tuples. Unpin the page.
                storageService.unpinDBPage(dbPage);
            }
        } catch (EOFException e) {
            // We ran out of pages. No tuples in the file!
            LOGGER.debug("No tuples in table-file " + dbFile + ".  Returning null.");
        }
        return null;
    }

    /**
     * <pre>
     * Procedure:
     * 1) Get slot index of current tuple.
     * 2) If there are more slots in the current page, find the next non-empty slot.
     * 3) If we get to the end of this page, go to the next page and try again.
     * 4) If we get to the end of the file, we return null.
     * </pre>
     * 
     * @param tblFileInfo 数据文件
     * @param tup 当前tuple
     * @return 下一个tuple
     */
    @Override
    public Tuple getNextTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {

        if (!(tup instanceof HeapPageTuple)) {
            throw new IllegalArgumentException("Tuple must be of type HeapPageTuple; got " + tup.getClass());
        }
        // 当前tuple
        HeapPageTuple heapTuple = (HeapPageTuple) tup;
        DBPage dbPage = heapTuple.getDBPage();
        DBFile dbFile = dbPage.getDBFile();

        // 找下一个tuple
        int nextSlot = heapTuple.getSlot() + 1;
        while (true) {
            int numSlots = pageStructure.getNumSlots(dbPage);
            // 只要不是EMPTY_SLOT，找到就返回
            while (nextSlot < numSlots) {
                int nextOffset = pageStructure.getSlotValue(dbPage, nextSlot);
                if (nextOffset != HeapPageStructure.EMPTY_SLOT) {
                    return new HeapPageTuple(tblFileInfo, dbPage, nextSlot, nextOffset);
                }
                nextSlot++;
            }

            // 到这里，说明当前页没有，接着从下一页的第一个tuple
            try {
                DBPage nextDBPage = storageService.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                storageService.unpinDBPage(dbPage);
                // 从下一页的第一个继续
                dbPage = nextDBPage;
                nextSlot = 0;
            } catch (EOFException e) {
                // 到了文件末尾，没有更多tuple了
                return null;
            }
        }
    }

    @Override
    public Tuple getTuple(TableFileInfo tblFileInfo, FilePointer fptr) throws InvalidFilePointerException, IOException {
        DBFile dbFile = tblFileInfo.getDBFile();
        DBPage dbPage;

        // 加载指定page
        try {
            dbPage = storageService.loadDBPage(dbFile, fptr.getPageNo());
        } catch (EOFException eofe) {
            // 将EOFException包装后抛出
            throw new InvalidFilePointerException(
                    "Specified page " + fptr.getPageNo() + " doesn't exist in file " + dbFile.getDataFile().getName(),
                    eofe);
        }

        // file-pointer指出了tuple对应slot的位置，slot里面放置了tuple的偏移量
        int slot;
        try {
            slot = pageStructure.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        } catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // 从slot中取出tuple的偏移量
        int offset = pageStructure.getSlotValue(dbPage, slot);
        if (offset == HeapPageStructure.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot + " on page " + fptr.getPageNo() + " is empty.");
        }

        return new HeapPageTuple(tblFileInfo, dbPage, slot, offset);
    }

    /**
     * <pre>
     * |  2 B   |  2 B  |  2 B  |...
     * |numSlots|slotVal|slotVal|...
     * .............
     * |nullFlag|tuple col1|tuple col2|
     * </pre>
     * 
     * numSlots:总槽位数 <br/>
     * slotVal: 其中存放的是tuple的Offset <br/>
     * tuple的值是从page的末尾开始存放的，在存放tuple前会先放置nullFlag，每个bit代表此tuple在对应列是否为Null
     * <br/>
     *
     * @param tblFileInfo the opened table to add the tuple to
     * @param tup a tuple object containing the values to add to the table
     * @return
     * @throws IOException e
     */
    @Override
    public Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {

        DBFile dbFile = tblFileInfo.getDBFile();
        int tupSize = PageTupleUtil.getTupleStorageSize(tblFileInfo.getSchema().getColumnInfos(), tup);
        LOGGER.debug("Adding new tuple of size " + tupSize + " bytes.");

        // 确保一页能放下一个tuple，每个tuple还需要对应一个slot(2 Byte)
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize + " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // 找到放置此tuple的数据页，不够就新建一个page
        int pageNo = 1;
        DBPage dbPage = null;
        while (true) {
            try {
                dbPage = storageService.loadDBPage(dbFile, pageNo);
            } catch (EOFException eofe) {
                // 到文件尾部了，就跳出循环，后面统一新建页
                LOGGER.debug("Reached end of data file without finding space for new tuple.");
                break;
            }
            int freeSpace = pageStructure.getFreeSpaceInPage(dbPage);
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

        // 现有的页都放不下，因此新增加一页
        if (dbPage == null) {
            LOGGER.debug("Creating new page " + pageNo + " to store new tuple.");
            dbPage = storageService.loadDBPage(dbFile, pageNo, true);
            pageStructure.initNewPage(dbPage);
        }

        // 分配空间，将tuple放入
        int slot = pageStructure.allocNewTuple(dbPage, tupSize);
        int tupOffset = pageStructure.getSlotValue(dbPage, slot);
        LOGGER.debug("New tuple will reside on page {}, slot {}.", pageNo, slot);
        Tuple pageTup = pageStructure.storeNewTuple(tblFileInfo, dbPage, slot, tupOffset, tup);

        pageStructure.sanityCheck(dbPage);
        txnService.recordPageUpdate(dbPage);
        // TODO: Really shouldn't unpin the page; the caller will want it.
        // TODO: Maybe need to change how we do this to make unpinning easier.
        return pageTup;
    }

    @Override
    public void updateTuple(TableFileInfo tblFileInfo, Tuple tup, Map<String, Object> newValues) throws IOException {
        if (!(tup instanceof HeapPageTuple)) {
            throw new IllegalArgumentException("Tuple must be of type HeapPageTuple; got " + tup.getClass());
        }
        HeapPageTuple heapTuple = (HeapPageTuple) tup;

        // 根据schema将对应列的值换成新的
        Schema schema = tblFileInfo.getSchema();
        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();
            int colIndex = schema.getColumnIndex(colName);
            heapTuple.setColumnValue(colIndex, value);
        }

        DBPage dbPage = heapTuple.getDBPage();
        pageStructure.sanityCheck(dbPage);
        txnService.recordPageUpdate(dbPage);
        storageService.unpinDBPage(dbPage);
    }

    @Override
    public void deleteTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException {

        if (!(tup instanceof HeapPageTuple)) {
            throw new IllegalArgumentException("Tuple must be of type HeapPageTuple; got " + tup.getClass());
        }
        HeapPageTuple heapTuple = (HeapPageTuple) tup;

        DBPage dbPage = heapTuple.getDBPage();
        pageStructure.deleteTuple(dbPage, heapTuple.getSlot());

        pageStructure.sanityCheck(dbPage);

        txnService.recordPageUpdate(dbPage);
        storageService.unpinDBPage(dbPage);
    }

    private String getTableFileName(String tableName) {
        return tableName + ".frm";
    }
}

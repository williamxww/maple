package com.bow.lab.transaction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.bow.lab.storage.IStorageService;
import com.bow.maple.storage.writeahead.WALManager;

import com.bow.maple.client.SessionState;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileReader;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBFileWriter;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.writeahead.LogSequenceNumber;
import com.bow.maple.storage.writeahead.RecoveryInfo;
import com.bow.maple.storage.writeahead.WALFileException;
import com.bow.maple.storage.writeahead.WALRecordType;
import com.bow.maple.transactions.TransactionState;
import com.bow.maple.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 此类用于管理write-ahead log，一些方法是用于记录各种类型的日志，一些方法则用于根据日志进行恢复。 the WAL, and so
 * forth.
 */
public class WALService implements IWALService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WALService.class);

    private static final String WAL_FILENAME_PATTERN = "wal-%05d.log";

    /**
     * log file number的最大值.
     */
    private static final int MAX_WAL_FILE_NUMBER = 65535;

    /**
     * write-ahead log file 最大 10MB，达到此值时创建新的WAL文件
     */
    private static final int MAX_WAL_FILE_SIZE = 10 * 1024 * 1024;

    /**
     * 接下来4byte记录了在前一个WAL file中最后一个字节的偏移量，第一个WAL文件中此值为0
     */
    private static final int OFFSET_PREV_FILE_END = 2;

    /**
     * 日志记录的起始位置
     */
    private static final int OFFSET_FIRST_RECORD = 6;

    private IStorageService storageService;

    public WALService(IStorageService storageService) {
        this.storageService = storageService;
    }

    /**
     * 根据文件号创建write ahead log file(WALFile)
     * 
     * @param fileNo 文件号
     * @return DBFile
     * @throws IOException e
     */
    @Override
    public DBFile createWALFile(int fileNo) throws IOException {
        String filename = getWALFileName(fileNo);
        LOGGER.debug("Creating WAL file " + filename);
        return storageService.createDBFile(filename, DBFileType.WRITE_AHEAD_LOG_FILE, DBFile.DEFAULT_PAGESIZE);
    }

    /**
     * 根据文件号，打开指定WAL日志文件
     * 
     * @param fileNo 文件号
     * @return 打开的WAL日志文件
     * @throws IOException 文件不存在或是文件类型不是WAL
     */
    @Override
    public DBFile openWALFile(int fileNo) throws IOException {
        String filename = getWALFileName(fileNo);
        LOGGER.debug("Opening WAL file " + filename);

        DBFile dbFile = storageService.openDBFile(filename);
        DBFileType type = dbFile.getType();

        if (type != DBFileType.WRITE_AHEAD_LOG_FILE) {
            throw new IOException(String.format("File %s is not of WAL-file type.", filename));
        }

        return dbFile;
    }

    /**
     * 生成WAL文件名
     * 
     * @param fileNo 文件号
     * @return WAL文件名
     */
    private String getWALFileName(int fileNo) {
        return String.format(WAL_FILENAME_PATTERN, fileNo);
    }

    /**
     * 根据recoveryInfo中WAL的起止位置，执行恢复操作。
     *
     * @param next 恢复时，写日志的位置
     * @param recoveryInfo 恢复的起止位置
     * @return 下次写日志的位置
     * @throws IOException 文件操作异常
     */
    @Override
    public LogSequenceNumber doRecovery(LogSequenceNumber next, RecoveryInfo recoveryInfo) throws IOException {

        LogSequenceNumber storedFirstLSN = recoveryInfo.firstLSN;
        LogSequenceNumber storedNextLSN = recoveryInfo.nextLSN;

        if (storedFirstLSN.equals(storedNextLSN)) {
            return next;
        }

        performRedo(recoveryInfo);
        performUndo(next, recoveryInfo);

        // data pages, and sync all of the affected files.
        storageService.writeAll(true);

        // FIXME 此处应该返回写完undo后的日志
        return next;
    }

    /**
     * 重做日志
     * 
     * @param recoveryInfo 从起始位置重做日志直到终止位置
     * @throws IOException e
     */
    private void performRedo(RecoveryInfo recoveryInfo) throws IOException {
        LogSequenceNumber current = recoveryInfo.firstLSN;
        LogSequenceNumber end = recoveryInfo.nextLSN;
        LOGGER.debug("Starting redo processing at LSN " + current);

        LogSequenceNumber oldLSN = null;
        DBFileReader walReader = null;
        while (current.compareTo(end) < 0) {
            if (oldLSN == null || oldLSN.getLogFileNo() != current.getLogFileNo()) {
                walReader = getWALFileReader(current);
            }

            // 获取type和transactionID
            byte typeID = walReader.readByte();
            int transactionID = walReader.readInt();
            WALRecordType type = WALRecordType.valueOf(typeID);
            LOGGER.debug("Redoing WAL record at {}.  Type = {}, TxnID = {}", current, type, transactionID);

            if (type != WALRecordType.START_TXN) {
                // 除了起始事务，其他事务类型都有preLSN (1个LSN：fileNo+offset)
                walReader.movePosition(6);
            }

            // 将此事务transactionID放到recoveryInfo的未完成事务map中
            recoveryInfo.updateInfo(transactionID, current);

            // Redo specific operations.
            switch (type) {
                case START_TXN:
                    // 前面updateInfo已将此事务放到未完成事务map中了，此处不再做处理
                    LOGGER.debug("Transaction " + transactionID + " is starting");
                    // 跳过record末尾的record-type
                    walReader.movePosition(1);
                    break;

                case COMMIT_TXN:
                case ABORT_TXN:
                    LOGGER.debug("Transaction " + transactionID + " is completed (" + type + ")");
                    // 将此事务从未完成map中移除
                    recoveryInfo.recordTxnCompleted(transactionID);
                    // 跳过record末尾的record-type
                    walReader.movePosition(1);

                    break;

                case UPDATE_PAGE:
                case UPDATE_PAGE_REDO_ONLY:
                    // 从WAL中获取要操作的真实数据页
                    String redoFilename = walReader.readVarString255();
                    int redoPageNo = walReader.readUnsignedShort();
                    int numSegments = walReader.readUnsignedShort();

                    // 打开对应的数据页
                    DBFile redoFile = storageService.openDBFile(redoFilename);
                    DBPage redoPage = storageService.loadDBPage(redoFile, redoPageNo);

                    LOGGER.debug(String.format("Redoing changes to file %s, page %d (%d segments)", redoFile,
                            redoPageNo, numSegments));

                    // 对数据页执行重做
                    applyRedo(type, walReader, redoPage, numSegments);

                    // 跳过此record末尾的recordSize(int)和recordType(byte)
                    walReader.movePosition(5);
                    break;

                default:
                    throw new WALFileException("Encountered unrecognized WAL record type " + type + " at LSN " + current
                            + " during redo processing!");
            }

            oldLSN = current;
            current = computeNextLSN(current.getLogFileNo(), walReader.getPosition());
        }

        if (current.compareTo(end) != 0) {
            throw new WALFileException(
                    "Traversing WAL file didn't yield " + " the same ending LSN as in the transaction-state file.  WAL "
                            + " result:  " + current + "  TxnState:  " + recoveryInfo.nextLSN);
        }

        LOGGER.debug("Redo processing is complete.  There are " + recoveryInfo.incompleteTxns.size()
                + " incomplete transactions.");
    }

    /**
     * 执行回滚操作，并记录新的日志。将RecoveryInfo中未完成的事务都回退
     *
     * @param recoveryInfo 指定了要回滚的范围和未完成的事务
     * @throws IOException e
     */
    private void performUndo(LogSequenceNumber next, RecoveryInfo recoveryInfo) throws IOException {
        LogSequenceNumber current = recoveryInfo.nextLSN;
        LogSequenceNumber begin = recoveryInfo.firstLSN;
        LOGGER.debug("Starting undo processing at LSN " + current);

        LogSequenceNumber oldLSN = null;
        DBFileReader walReader = null;
        while (recoveryInfo.hasIncompleteTxns()) {
            // 如果处于日志文件的开始位置，则移动到前一个文件的最后
            if (ensureRightPosition(current)) {
                break;
            }
            // 已回退到最开始位置了
            if (current.compareTo(begin) <= 0) {
                break;
            }
            // 前次循环的日志和本次不一致，则重新取reader
            int logFileNo = current.getLogFileNo();
            if (oldLSN == null || oldLSN.getLogFileNo() != logFileNo) {
                walReader = getWALFileReader(current);
            }
            // reader移动到txnId上
            WALRecordType type = moveToTxnId(current, walReader);
            if (current.compareTo(begin) < 0) {
                break;
            }
            // 读取transactionID，若此事务已完成，则不处理
            // FIXME UPDATE_PAGE读取不到transactionID
            int transactionID = walReader.readInt();
            if (recoveryInfo.isTxnComplete(transactionID)) {
                oldLSN = current;
                continue;
            }

            // 开始执行回退
            LOGGER.debug("Undoing WAL record at {}.  Type = {}, TxnID = {}", current, type, transactionID);
            performUndo(next, recoveryInfo, type, transactionID, walReader);

            oldLSN = current;
        }
        LOGGER.debug("Undo processing is complete.");
    }

    private boolean ensureRightPosition(LogSequenceNumber current) throws IOException {
        int logFileNo = current.getLogFileNo();
        int fileOffset = current.getFileOffset();

        if (fileOffset < OFFSET_FIRST_RECORD) {
            // 不可能到这里
            throw new WALFileException(
                    String.format("Overshot the start of WAL file %d's records; ended up at file-position %d",
                            logFileNo, fileOffset));
        }

        // fileOffset处于某日志文件的起始位置，则将current指向前一页的最后
        if (fileOffset == OFFSET_FIRST_RECORD) {
            // 获取前一个WAL文件的offset(prevFileEndOffset)
            DBFileReader walReader = getWALFileReader(current);
            walReader.setPosition(OFFSET_PREV_FILE_END);
            int prevFileEndOffset = walReader.readInt();
            if (prevFileEndOffset == 0) {
                LOGGER.debug("Reached the very start of the write-ahead log!");
                return false;
            }
            // 获取前一个WAL文件的fileNo
            logFileNo--;
            if (logFileNo < 0) {
                logFileNo = MAX_WAL_FILE_NUMBER;
            }
            // 获取前一个WAL的LSN
            current.setLogFileNo(logFileNo);
            current.setFileOffset(prevFileEndOffset);
        }
        return true;
    }

    private WALRecordType moveToTxnId(LogSequenceNumber current, DBFileReader walReader) throws IOException {
        int fileOffset = current.getFileOffset();
        // WAL记录的最后一个字节都是type,向前移动1byte便于读取typeId
        walReader.movePosition(-1);
        byte typeID = walReader.readByte();
        WALRecordType type = WALRecordType.valueOf(typeID);

        // 计算此record的start,注意fileOffset指向了nextLSN的第一个字节
        int startOffset;
        switch (type) {
            case START_TXN:
                // Type (1B) + TransactionID (4B) + Type (1B)
                // startOffset指向了TransactionID的第一个字节
                startOffset = fileOffset - 6 + 1;
                break;

            case COMMIT_TXN:
            case ABORT_TXN:
                // Type(1B)+TransactionID(4B)+PrevLSN(6B)+Type(1B)
                startOffset = fileOffset - 12 + 1;
                break;

            case UPDATE_PAGE:
            case UPDATE_PAGE_REDO_ONLY:
                // startOffset(4B)+Type(1B)
                walReader.movePosition(-5);
                startOffset = walReader.readInt();
                break;

            default:
                throw new WALFileException("Encountered unrecognized WAL record type " + type + " at LSN " + current
                        + " during redo processing!");
        }
        current.setFileOffset(startOffset);
        return type;
    }

    private void performUndo(LogSequenceNumber next, RecoveryInfo recoveryInfo, WALRecordType type, int transactionID,
            DBFileReader walReader) throws IOException {
        // 开始执行回滚操作
        switch (type) {
            case START_TXN:
                // Record that the transaction is aborted.
                writeTxnRecord(next, WALRecordType.ABORT_TXN, transactionID, recoveryInfo.getLastLSN(transactionID));

                LOGGER.debug(String.format("Undo phase:  aborted transaction %d", transactionID));
                recoveryInfo.recordTxnCompleted(transactionID);
                break;

            case COMMIT_TXN:
            case ABORT_TXN:
                // 因为现在是执行未处理玩的事务，所以不可能是处理COMMIT ABORT
                throw new IllegalStateException(
                        "Saw a " + type + "WAL-record for supposedly incomplete transaction " + transactionID + "!");

            case UPDATE_PAGE:
                // 读取数据页
                String undoFilename = walReader.readVarString255();
                int undoPageNo = walReader.readUnsignedShort();
                // 打开数据文件
                DBFile undoFile = storageService.openDBFile(undoFilename);
                DBPage undoPage = storageService.loadDBPage(undoFile, undoPageNo);

                int numSegments = walReader.readUnsignedShort();
                LOGGER.debug("Undoing changes to file {}, page {} ({} segments)", undoFile, undoPageNo, numSegments);

                // 执行回退操作
                byte[] redoOnlyData = applyUndo(walReader, undoPage, numSegments);
                // 将回退操作的内容也记录到新的日志中
                writeRedoRecord(next, transactionID, recoveryInfo.getLastLSN(transactionID), undoPage, numSegments,
                        redoOnlyData);

                recoveryInfo.updateInfo(transactionID, next);
                break;

            case UPDATE_PAGE_REDO_ONLY:
                // We ignore redo-only updates during the undo phase.
                break;

            default:
                throw new WALFileException(
                        "Encountered unrecognized WAL record type " + type + " during undo processing!");
        }
    }



    private LogSequenceNumber computeNextLSN(int fileNo, int fileOffset) {
        if (fileOffset >= MAX_WAL_FILE_SIZE) {
            // WAL文件超过大小限制后，fileNo+1,offset重置
            fileNo += 1;
            if (fileNo > MAX_WAL_FILE_NUMBER) {
                fileNo = 0;
            }
            fileOffset = OFFSET_FIRST_RECORD;
        }
        return new LogSequenceNumber(fileNo, fileOffset);
    }

    private DBFileWriter getWALFileWriter(LogSequenceNumber lsn) throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile;
        try {
            walFile = openWALFile(fileNo);
        } catch (FileNotFoundException e) {
            LOGGER.debug("WAL file doesn't exist!  WAL is expanding into a new file.");
            walFile = createWALFile(fileNo);
            // TODO: Write the previous WAL file's last file-offset into the new
            // WAL file's start.
        }

        DBFileWriter writer = new DBFileWriter(walFile, storageService);
        writer.setPosition(offset);

        return writer;
    }

    /**
     * 根据LSN找到对应的日志文件，移动到指定位置
     * 
     * @param lsn fileNo+offset,指定要读取的日志的起始位置
     * @return 从指定位置开始的一个DBFileReader
     * @throws IOException e
     */
    private DBFileReader getWALFileReader(LogSequenceNumber lsn) throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile = openWALFile(fileNo);
        DBFileReader reader = new DBFileReader(walFile);
        reader.setPosition(offset);

        return reader;
    }

    /**
     * 写一个事务分界点到日志中
     *
     * @see WALRecordType#START_TXN
     * @see WALRecordType#COMMIT_TXN
     * @see WALRecordType#ABORT_TXN
     * @param type WALRecordType枚举
     * @return 写入到日志中的内容对应的LSN
     * @throws IOException the write-ahead log 不能更新
     * @throws IllegalStateException 当前没有事务
     */
    @Override
    public LogSequenceNumber writeTxnRecord(LogSequenceNumber lsn, WALRecordType type) throws IOException {

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException("No transaction is currently in progress!");
        }

        LogSequenceNumber next = writeTxnRecord(lsn, type, txnState.getTransactionID(), txnState.getLastLSN());
        // 当前事务的最后一个lsn
        txnState.setLastLSN(lsn);
        return next;
    }

    /**
     * 写一个事务分界点到日志中
     * 
     * @param type 日志类型
     * @param transactionID 事务ID
     * @param prevLSN 前一个LSN
     * @return 下次写的LSN
     * @throws IOException e
     */
    @Override
    public LogSequenceNumber writeTxnRecord(LogSequenceNumber lsn, WALRecordType type, int transactionID,
            LogSequenceNumber prevLSN) throws IOException {

        if (type != WALRecordType.START_TXN && type != WALRecordType.COMMIT_TXN && type != WALRecordType.ABORT_TXN) {
            throw new IllegalArgumentException("Invalid record type " + type + " passed to writeTxnRecord().");
        }

        if ((type == WALRecordType.COMMIT_TXN || type == WALRecordType.ABORT_TXN) && prevLSN == null) {
            throw new IllegalArgumentException("prevLSN must be specified for records of type " + type);
        }

        LOGGER.debug("Writing a " + type + " record for transaction " + transactionID + " at LSN " + lsn);

        // 开始写内容
        DBFileWriter walWriter = getWALFileWriter(lsn);
        walWriter.writeByte(type.getID());
        walWriter.writeInt(transactionID);
        if (type == WALRecordType.START_TXN) {
            // 每个日志rec后面都有type
            walWriter.writeByte(type.getID());
            // TypeID (1B) + TransactionID (4B) + TypeID (1B)
            lsn.setRecordSize(6);
        } else {
            walWriter.writeShort(prevLSN.getLogFileNo());
            walWriter.writeInt(prevLSN.getFileOffset());
            walWriter.writeByte(type.getID());
            // TypeID (1B) + TransactionID (4B) + PrevLSN (6B) + TypeID (1B)
            lsn.setRecordSize(12);
        }

        LogSequenceNumber nextLSN = computeNextLSN(lsn.getLogFileNo(), walWriter.getPosition());
        LOGGER.debug("Next-LSN value is now " + nextLSN);

        return nextLSN;
    }

    /**
     * 将dbPage里的新数据写到write-ahead log中，包括undo and redo details.<br/>
     * 不同数据段，数组a,b中，数组元素不同的一段数据<br/>
     * 相同数据段，数组a,b中，数组元素相同的一段数据<br/>
     *
     * @param dbPage 数据页
     * @return 本次记录日志的LogSequenceNumber
     * @throws IOException e
     */
    @Override
    public LogSequenceNumber writeUpdateRecord(LogSequenceNumber lsn, DBPage dbPage, TransactionState txnState)
            throws IOException {

        if (dbPage == null) {
            throw new IllegalArgumentException("dbPage must be specified");
        }
        if (!dbPage.isDirty()) {
            throw new IllegalArgumentException("dbPage has no updates to store");
        }
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException("No transaction is currently in progress!");
        }

        LOGGER.debug("Writing an {} record for transaction {} at LSN {}", WALRecordType.UPDATE_PAGE,
                txnState.getTransactionID(), lsn);

        // Record the WAL record. First thing to do: figure out where it goes.
        DBFileWriter walWriter = getWALFileWriter(lsn);
        walWriter.writeByte(WALRecordType.UPDATE_PAGE.getID());
        walWriter.writeInt(txnState.getTransactionID());

        // We need to store the previous log sequence number for this record.
        LogSequenceNumber prevLSN = txnState.getLastLSN();
        walWriter.writeShort(prevLSN.getLogFileNo());
        walWriter.writeInt(prevLSN.getFileOffset());

        walWriter.writeVarString255(dbPage.getDBFile().getDataFile().getName());
        walWriter.writeShort(dbPage.getPageNo());

        // 存放segment的数量
        int segCountOffset = walWriter.getPosition();
        walWriter.writeShort(-1);

        byte[] oldData = dbPage.getOldPageData();
        byte[] newData = dbPage.getPageData();
        int pageSize = dbPage.getPageSize();

        int numSegments = 0;
        int index = 0;
        while (index < pageSize) {
            LOGGER.debug("Skipping identical bytes starting at index " + index);

            // Skip data until we find stuff that's different.
            index += ArrayUtil.sizeOfIdenticalRange(oldData, newData, index);
            assert index <= pageSize;
            if (index == pageSize) {
                break;
            }
            LOGGER.debug("Recording changed bytes starting at index " + index);

            // 找出不同数据的数据段，写到日志中
            int size = 0;
            while (index + size < pageSize) {
                size += ArrayUtil.sizeOfDifferentRange(oldData, newData, index + size);
                assert index + size <= pageSize;
                if (index + size == pageSize) {
                    break;
                }

                // 在不同数据段后，相同数据段的长度>4,此不同段就结束了
                int sameSize = ArrayUtil.sizeOfIdenticalRange(oldData, newData, index + size);
                if (sameSize > 4 || index + size + sameSize == pageSize) {
                    break;
                }
                size += sameSize;
            }

            LOGGER.debug("Found " + size + " changed bytes starting at index " + index);

            // 写 不同数据段的起始位置的和大小
            walWriter.writeShort(index);
            walWriter.writeShort(size);

            // Write the old data (undo), and then the new data (redo).
            walWriter.write(oldData, index, size);
            walWriter.write(newData, index, size);

            numSegments++;
            index += size;
        }
        assert index == pageSize;

        // 写段的个数
        int currOffset = walWriter.getPosition();
        walWriter.setPosition(segCountOffset);
        walWriter.writeShort(numSegments);
        walWriter.setPosition(currOffset);

        // 写当前lsn的offset和typeId，方便从后往前查看日志
        walWriter.writeInt(lsn.getFileOffset());
        walWriter.writeByte(WALRecordType.UPDATE_PAGE.getID());

        // Store the LSN of the change on the page.
        lsn.setRecordSize(walWriter.getPosition() - lsn.getFileOffset());
        dbPage.setPageLSN(lsn);
        txnState.setLastLSN(lsn);
        LogSequenceNumber nextLSN = computeNextLSN(lsn.getLogFileNo(), walWriter.getPosition());
        return nextLSN;
    }

    private void test(int pageSize, int[] newData, int[] oldData) {
        int i = 0;
        while (i < pageSize) {
            boolean same = true;
            for (int j = 0; j < 32; j++) {
                if (oldData[i + j] != newData[i + j]) {
                    same = false;
                    break;
                }
            }

            if (!same) {
                System.err.printf("%04X OLD: ", i);
                for (int j = 0; j < 32; j++)
                    System.err.printf(" %02X", oldData[i + j]);
                System.err.println();

                System.err.printf("%04X NEW: ", i);
                for (int j = 0; j < 32; j++) {
                    if (newData[i + j] != oldData[i + j])
                        System.err.printf(" %02X", newData[i + j]);
                    else
                        System.err.print(" ..");
                }
                System.err.println();
            }

            i += 32;
        }
    }

    /**
     * 从WAL中读取数据重做到数据文件dbPage中
     * 
     * @param type WAL操作类型
     * @param walReader 从WAL中读取日志的reader
     * @param dbPage 重做的数据页
     * @param numSegments 要重做的数据段数
     * @throws IOException e
     */
    private void applyRedo(WALRecordType type, DBFileReader walReader, DBPage dbPage, int numSegments)
            throws IOException {

        for (int iSeg = 0; iSeg < numSegments; iSeg++) {
            // 获取在数据页的位置和重做数据大小
            int index = walReader.readUnsignedShort();
            int size = walReader.readUnsignedShort();

            // UPDATE_PAGE 会先写undo data然后再写redo data,此处要跳过undo
            if (type == WALRecordType.UPDATE_PAGE) {
                walReader.movePosition(size);
            }

            // 从WAL中获取重做数据，写入到数据页中
            byte[] redoData = new byte[size];
            walReader.read(redoData);
            dbPage.write(index, redoData);
        }
    }

    /**
     * 对数据页执行Undo，将undo数据返回
     * 
     * @param walReader 日志reader
     * @param dbPage 要回滚的数据页
     * @param numSegments 回滚的数据段个数
     * @return undo的数据
     * @throws IOException e
     */
    private byte[] applyUndo(DBFileReader walReader, DBPage dbPage, int numSegments) throws IOException {

        ByteArrayOutputStream redoOnlyBAOS = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(redoOnlyBAOS);

        for (int i = 0; i < numSegments; i++) {
            // 获取每段在数据页中的起始位置和长度
            int start = walReader.readUnsignedShort();
            int length = walReader.readUnsignedShort();

            // 对数据页执行undo.
            byte[] undoData = new byte[length];
            walReader.read(undoData);
            dbPage.write(start, undoData);

            // 跳过 redo data, 在undo数据后面接着就是redo数据.
            walReader.movePosition(length);

            // Record what we wrote into the redo-only record data.
            dos.writeShort(start);
            dos.writeShort(length);
            dos.write(undoData);
        }

        // Return the data that will appear in the redo-only record body.
        dos.flush();
        return redoOnlyBAOS.toByteArray();
    }

    /**
     * 将变化数据changes写到dbPage上
     *
     * @param dbPage 数据页
     * @param numSegments 总数据段
     * @param changes 更新数据
     * @return 当前记录日志的LSN
     * @throws IOException e
     */
    @Override
    public LogSequenceNumber writeRedoRecord(LogSequenceNumber lsn, DBPage dbPage, int numSegments, byte[] changes)
            throws IOException {
        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException("No transaction is currently in progress!");
        }
        LogSequenceNumber next = writeRedoRecord(lsn, txnState.getTransactionID(), txnState.getLastLSN(), dbPage,
                numSegments, changes);
        txnState.setLastLSN(lsn);
        return next;
    }

    /**
     * 只写redo log，
     * 
     * <pre>
     * |    1B    | 4B  |      2B     |      4B     |    x B   |  2B  |    2B     |
     * |WALRecType|txnId|prevLSNFileNo|prevLSNOffset|DBFileName|PageNo|numSegments|
     *
     * | 2B  | 2B |    xB   | 2B  | 2B |     xB   |...|        4B      |    1B    |
     * |index|size|redo data|index|size| redo data|...|rec's fileOffset|WALRecType|
     * </pre>
     * 
     * @param transactionID transactionID
     * @param prevLSN 前一条日志的LSN
     * @param dbPage 数据页
     * @param numSegments 有修改的数据段
     * @param changes 修改的数据
     * @return 当前记录日志的LSN
     * @throws IOException e
     */
    @Override
    public LogSequenceNumber writeRedoRecord(LogSequenceNumber lsn, int transactionID, LogSequenceNumber prevLSN,
            DBPage dbPage, int numSegments, byte[] changes) throws IOException {

        if (dbPage == null) {
            throw new IllegalArgumentException("dbPage must be specified");
        }
        if (changes == null) {
            throw new IllegalArgumentException("changes must be specified");
        }

        // Record the WAL record. First thing to do: figure out where it goes.
        LOGGER.debug("Writing redo-only update record for transaction {} at LSN {}. PrevLSN = {}", transactionID, lsn,
                prevLSN);

        DBFileWriter walWriter = getWALFileWriter(lsn);
        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());
        walWriter.writeInt(transactionID);

        // write prevLSN
        walWriter.writeShort(prevLSN.getLogFileNo());
        walWriter.writeInt(prevLSN.getFileOffset());
        // 记录修改的数据页
        walWriter.writeVarString255(dbPage.getDBFile().getDataFile().getName());
        walWriter.writeShort(dbPage.getPageNo());
        // Write the redo-only data.
        walWriter.writeShort(numSegments);
        walWriter.write(changes);

        // 记录当前lsn的偏移量和typeId
        walWriter.writeInt(lsn.getFileOffset());
        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());
        // Store the LSN of the change on the page.
        lsn.setRecordSize(walWriter.getPosition() - lsn.getFileOffset());
        dbPage.setPageLSN(lsn);
        LogSequenceNumber nextLSN = computeNextLSN(lsn.getLogFileNo(), walWriter.getPosition());
        return nextLSN;
    }

    @Override
    public void rollbackTransaction(LogSequenceNumber next, int transactionID, LogSequenceNumber lsn)
            throws IOException {
        if (transactionID == TransactionState.NO_TRANSACTION) {
            LOGGER.info("No transaction in progress - rollback is a no-op.");
            return;
        }
        LOGGER.info("Rolling back transaction " + transactionID + ".  Last LSN = " + lsn);

        // 由后往前扫描回滚
        while (true) {
            DBFileReader walReader = getWALFileReader(lsn);

            WALRecordType type = WALRecordType.valueOf(walReader.readByte());
            int recordTxnID = walReader.readInt();
            if (recordTxnID != transactionID) {
                throw new WALFileException(String.format(
                        "Sent to WAL record " + "for transaction %d at LSN %s, during rollback of " + "transaction %d.",
                        recordTxnID, lsn, transactionID));
            }

            LOGGER.debug("Undoing WAL record at {}.  Type = {}, TxnID = {}", lsn, type, transactionID);

            if (type == WALRecordType.START_TXN) {
                // Done rolling back the transaction.
                LOGGER.debug("Hit the start of the transaction, rollback done.");
                break;
            }

            // Read out the "previous LSN" value.
            int prevFileNo = walReader.readUnsignedShort();
            int prevOffset = walReader.readInt();
            LogSequenceNumber prevLSN = new LogSequenceNumber(prevFileNo, prevOffset);
            LOGGER.debug("Read PrevLSN of " + prevLSN);

            if (type == WALRecordType.UPDATE_PAGE) {
                // Undo this change.
                // Read the file and page with the changes to undo.
                String filename = walReader.readVarString255();
                int pageNo = walReader.readUnsignedShort();

                // Open the specified file and retrieve the data page to undo.
                DBFile dbFile = storageService.openDBFile(filename);
                DBPage dbPage = storageService.loadDBPage(dbFile, pageNo);

                // Read the number of segments in the redo/undo record, and
                // undo the writes. While we do this, the data for a redo-only
                // record is also accumulated.
                int numSegments = walReader.readUnsignedShort();
                LOGGER.debug("UPDATE_PAGE record is for file {}, page {}.  Record contains {} segments.", filename,
                        pageNo, numSegments);
                // 执行回退，并将undo数据返回
                byte[] redoOnlyData = applyUndo(walReader, dbPage, numSegments);
                LOGGER.debug("Generated " + redoOnlyData.length + " bytes of redo-only data.");

                // Finally, update the WAL with the redo-only record. The
                // method takes care of setting the DBPage's PageLSN value.
                writeRedoRecord(next, dbPage, numSegments, redoOnlyData);
            } else {
                LOGGER.warn("Encountered unexpected WAL-record type {} while rolling back transaction {}.", type,
                        transactionID);
            }

            // Go to the immediately preceding record in the logs for this
            // transaction.
            lsn = prevLSN;
        }

        // All done rolling back the transaction! Record that it was aborted
        // in the WAL.
        writeTxnRecord(next, WALRecordType.ABORT_TXN);
        LOGGER.info(String.format("Transaction %d:  Rollback complete.", transactionID));
    }

    @Override
    public LogSequenceNumber forceWAL(LogSequenceNumber start, LogSequenceNumber end) throws IOException {
        // start>=end,就不处理了
        if (start.compareTo(end) >= 0) {
            LOGGER.debug("Request to force WAL to LSN {} unnecessary; already forced to {}.", end, start);
            return start;
        }

        // 将缓存中所有的WAL日志文件落盘
        for (int fileNo = start.getLogFileNo(); fileNo < end.getLogFileNo(); fileNo++) {
            String walFileName = WALManager.getWALFileName(fileNo);
            DBFile walFile = storageService.getFile(walFileName);
            if (walFile != null) {
                storageService.writeDBFile(walFile, true);
            }
        }

        // 对于最后一个WAL文件，我们只刷新到lsn指定记录所在的页
        String walFileName = WALManager.getWALFileName(end.getLogFileNo());
        DBFile walFile = storageService.getFile(walFileName);
        if (walFile != null) {
            int lastPosition = end.getFileOffset() + end.getRecordSize();
            int pageNo = lastPosition / walFile.getPageSize();
            storageService.writeDBFile(walFile, 0, pageNo, true);
        }

        LOGGER.debug("WAL was successfully forced to LSN {} (plus {} bytes)", end, end.getRecordSize());
        return end;
    }

}

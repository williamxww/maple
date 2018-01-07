package com.bow.lab.transaction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.bow.lab.storage.IStorageService;
import com.bow.maple.util.ExtensionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bow.maple.client.SessionState;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.writeahead.LogSequenceNumber;
import com.bow.maple.storage.writeahead.RecoveryInfo;
import com.bow.maple.storage.writeahead.WALRecordType;
import com.bow.maple.transactions.TransactionException;
import com.bow.maple.transactions.TransactionState;
import com.bow.maple.transactions.TransactionStatePage;
import com.bow.maple.util.PropertiesUtil;

/**
 *
 * txnstate.dat
 * 
 * <pre>
 *     |    1B  |       1B     |   4B      |      2B      |      4B      |      2B     |      4B     |
 *     |FileType|encodePageSize|NEXT_TXN_ID|firstLSNFileNo|firstLSNOffset|nextLSNFileNo|nextLSNOffset|
 * </pre>
 * 
 * FirstLSN,起始日志位置，事务恢复时从此处开始<br/>
 * NextLSN,下一个日志的位置,下一个事务日志接着此处记录.
 */
public class TransactionService implements ITransactionService {
    private static Logger logger = LoggerFactory.getLogger(TransactionService.class);

    /**
     * The system property that can be used to turn on or off transaction
     * processing.
     */
    public static final String PROP_TXNS = "nanodb.transactions";

    /**
     * This is the name of the file that the Transaction Manager uses to keep
     * track of overall transaction state.
     */
    public static final String TXNSTATE_FILENAME = "txnstate.dat";

    /**
     * Returns true if the transaction processing system is enabled, or false
     * otherwise.
     *
     * @return true if the transaction processing system is enabled, or false
     *         otherwise.
     */
    public static boolean isEnabled() {
        return "on".equalsIgnoreCase(PropertiesUtil.getProperty(PROP_TXNS, "on"));
    }

    private IStorageService storageService;

    private IWALService walService;

    /**
     * 下次事务的ID
     */
    private AtomicInteger nextTxnID;

    /**
     * txnStat文件中的firstLsn
     */
    private LogSequenceNumber firstLsnInFile;

    /**
     * txnStat文件中的nextLsn
     */
    private LogSequenceNumber nextLsnInFile;

    /**
     * 下次写的位置
     */
    private LogSequenceNumber nextLsn;

    public TransactionService(IStorageService storageService) {
        this.storageService = storageService;
        this.nextTxnID = new AtomicInteger();
        this.walService = new WALService(storageService);
    }

    public TransactionService() throws IOException {
        this.storageService = ExtensionLoader.getExtensionLoader(IStorageService.class).getExtension();
        this.nextTxnID = new AtomicInteger();
        this.walService = new WALService(storageService);
    }

    @Override
    public void initialize() throws IOException {
        if (!isEnabled()) {
            throw new IllegalStateException("Transactions are disabled!");
        }

        TransactionStatePage txnState;
        try {
            txnState = loadTxnStateFile();
        } catch (FileNotFoundException e) {
            logger.info("Couldn't find transaction-state file {}, creating.", TXNSTATE_FILENAME);
            txnState = createTxnStateFile();
        }

        // Perform recovery
        LogSequenceNumber start = txnState.getFirstLSN();
        LogSequenceNumber end = txnState.getNextLSN();
        logger.debug("Txn State has FirstLSN = {}, NextLSN = {}", start, end);
        RecoveryInfo recoveryInfo = new RecoveryInfo(start, end);
        this.nextLsnInFile = walService.doRecovery(this.nextLsnInFile, recoveryInfo);

        // 存储到文件
        storeTxnStateToFile();

        // Register the component that manages indexes when tables are modified.
        // EventDispatcher.getInstance().addCommandEventListener(new
        // TransactionStateUpdater(this, bufferManager));
    }

    private TransactionStatePage createTxnStateFile() throws IOException {
        // 创建事务文件并写入fileType和pageSize
        DBFile stateFile = storageService.createDBFile(TXNSTATE_FILENAME, DBFileType.TXNSTATE_FILE,
                DBFile.DEFAULT_PAGESIZE);
        DBPage statePage = storageService.loadDBPage(stateFile, 0);
        TransactionStatePage txnState = new TransactionStatePage(statePage);

        // Set the "next transaction ID" value to an initial default.
        this.nextTxnID.set(1);
        txnState.setNextTransactionID(1);

        // Set the "first LSN" and "next LSN values to initial defaults.
        LogSequenceNumber lsn = new LogSequenceNumber(0, WALService.OFFSET_FIRST_RECORD);
        txnState.setFirstLSN(lsn);
        txnState.setNextLSN(lsn);
        this.firstLsnInFile = lsn;
        this.nextLsnInFile = lsn;
        this.nextLsn = lsn;
        storageService.writeDBFile(stateFile, true);
        return txnState;
    }

    private TransactionStatePage loadTxnStateFile() throws IOException {
        DBFile dbFile = storageService.openDBFile(TXNSTATE_FILENAME);
        DBPage dbPage = storageService.loadDBPage(dbFile, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbPage);

        // Set the "next transaction ID" value properly.
        this.nextTxnID.set(txnState.getNextTransactionID());

        // Retrieve the "first LSN" and "next LSN values so we know the range of
        // the write-ahead log that we need to apply for recovery.
        this.nextLsnInFile = txnState.getNextLSN();
        this.firstLsnInFile = txnState.getFirstLSN();
        this.nextLsn = nextLsnInFile;
        return txnState;
    }

    private void storeTxnStateToFile() throws IOException {
        DBFile dbFile = storageService.openDBFile(TXNSTATE_FILENAME);
        DBPage dbPage = storageService.loadDBPage(dbFile, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbPage);

        txnState.setNextTransactionID(this.nextTxnID.get());
        txnState.setFirstLSN(this.firstLsnInFile);
        txnState.setNextLSN(this.nextLsnInFile);
        storageService.writeDBFile(dbFile, true);
    }

    @Override
    public int getAndIncrementNextTxnID() {
        return nextTxnID.getAndIncrement();
    }

    /**
     * 开始事务，只需要在SessionState设置初始变量即可。
     * 
     * @param userStarted 是用户指定开启，还是程序自动开启
     * @throws TransactionException 事务异常
     */
    @Override
    public void startTransaction(boolean userStarted) throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (txnState.isTxnInProgress()) {
            throw new IllegalStateException("A transaction is already in progress!");
        }

        int txnID = getAndIncrementNextTxnID();
        txnState.setTransactionID(txnID);
        txnState.setUserStartedTxn(userStarted);
        logger.debug("Starting transaction with ID " + txnID + (userStarted ? " (user-started)" : ""));
        // 当用户真正改了数据页，再真正记录事务启动日志
    }

    @Override
    public void recordPageUpdate(DBPage dbPage) throws IOException {
        if (!dbPage.isDirty()) {
            logger.debug("Page reports it is not dirty; not logging update.");
            return;
        }
        logger.debug("Recording page-update for page " + dbPage.getPageNo() + " of file " + dbPage.getDBFile());

        // 开启一个事务
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.hasLoggedTxnStart()) {
            // 若此事务没有开启记录WAL日志，则开启
            txnState.setLoggedTxnStart(true);
            this.nextLsn = walService.writeTxnRecord(this.nextLsn, WALRecordType.START_TXN);
        }
        this.nextLsn = walService.writeUpdateRecord(this.nextLsn, dbPage, txnState);
        dbPage.syncOldPageData();
    }

    @Override
    public void commitTransaction() throws TransactionException {
        SessionState sessionState = SessionState.get();
        TransactionState txnState = sessionState.getTxnState();

        if (!txnState.isTxnInProgress()) {
            sessionState.getOutputStream().println("No transaction is currently in progress.");
            return;
        }

        int txnID = txnState.getTransactionID();
        if (txnState.hasLoggedTxnStart()) {
            try {
                // 写提交日志
                this.nextLsn = walService.writeTxnRecord(this.nextLsn, WALRecordType.COMMIT_TXN);
                // 强制WAL落盘
                forceWAL(this.nextLsn);
            } catch (IOException e) {
                throw new TransactionException("Couldn't commit transaction " + txnID + "!", e);
            }
        } else {
            logger.debug(
                    "Transaction " + txnID + " has made no changes; not " + "recording transaction-commit to WAL.");
        }

        // 清理txnState
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }

    @Override
    public void rollbackTransaction() throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (!txnState.isTxnInProgress()) {
            state.getOutputStream().println("No transaction is currently in progress.");
            return;
        }

        int txnID = txnState.getTransactionID();
        if (txnState.hasLoggedTxnStart()) {
            try {
                walService.rollbackTransaction(this.nextLsnInFile, txnID, txnState.getLastLSN());
            } catch (IOException e) {
                throw new TransactionException("Couldn't rollback transaction " + txnID + "!", e);
            }
        } else {
            logger.debug("Transaction {} has made no changes; not recording transaction-rollback to WAL.", txnID);
        }

        // 清理txnState
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }

    /**
     * 强制将到lsn为止的所有WAL落盘。
     * @param lsn lsn之前的日志全部要落盘
     * @throws IOException 此处失败了，有可能导致数据库出问题
     */
    public void forceWAL(LogSequenceNumber lsn) throws IOException {
        this.nextLsnInFile = walService.forceWAL(nextLsnInFile, lsn);

        // 最后更新txnState文件的txnStateNextLSN
        storeTxnStateToFile();
        logger.debug("WAL was successfully forced to LSN {} (plus {} bytes)", lsn, lsn.getRecordSize());
    }

    public void forceWAL() throws IOException {
        forceWAL(this.nextLsn);
    }
}

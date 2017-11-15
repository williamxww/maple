package com.bow.lab.transaction;

import java.io.IOException;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.writeahead.LogSequenceNumber;
import com.bow.maple.storage.writeahead.RecoveryInfo;
import com.bow.maple.storage.writeahead.WALRecordType;
import com.bow.maple.transactions.TransactionState;

/**
 * @author vv
 * @since 2017/11/11.
 */
public interface IWALService {


    /**
     * 根据文件号创建write ahead log file(WALFile)
     *
     * @param fileNo 文件号
     * @return DBFile
     * @throws IOException e
     */
    DBFile createWALFile(int fileNo) throws IOException;

    /**
     * 根据文件号，打开指定WAL日志文件
     *
     * @param fileNo 文件号
     * @return 打开的WAL日志文件
     * @throws IOException 文件不存在或是文件类型不是WAL
     */
    DBFile openWALFile(int fileNo) throws IOException;

    /**
     * 根据recoveryInfo中WAL的起止位置，执行恢复操作。
     *
     * @param next 恢复时，写日志的位置
     * @param recoveryInfo 恢复的起止位置
     * @return 下次写日志的位置
     * @throws IOException 文件操作异常
     */
    LogSequenceNumber doRecovery(LogSequenceNumber next, RecoveryInfo recoveryInfo)throws IOException;

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
    LogSequenceNumber writeTxnRecord(LogSequenceNumber next, WALRecordType type) throws IOException;

    /**
     * 写一个事务分界点到日志中
     *
     * @param type 日志类型
     * @param transactionID 事务ID
     * @param prevLSN 前一个LSN
     * @return 当前日志操作的LSN
     * @throws IOException e
     */
    LogSequenceNumber writeTxnRecord(LogSequenceNumber next, WALRecordType type, int transactionID, LogSequenceNumber prevLSN)
            throws IOException;

    /**
     * 将dbPage里的新数据写到write-ahead log中，包括undo and redo details.
     *
     * @param dbPage 数据页
     * @return 本次记录日志的LogSequenceNumber
     * @throws IOException e
     */
    LogSequenceNumber writeUpdatePageRecord(LogSequenceNumber next,DBPage dbPage, TransactionState txnState) throws IOException;

    /**
     * 只写redo log
     * <br/>
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
    LogSequenceNumber writeRedoOnlyUpdatePageRecord(LogSequenceNumber next, int transactionID, LogSequenceNumber prevLSN, DBPage dbPage,
            int numSegments, byte[] changes) throws IOException;

    /**
     * 将变化数据changes写到dbPage上
     * 
     * @param dbPage 数据页
     * @param numSegments 总数据段
     * @param changes 更新数据
     * @return 当前记录日志的LSN
     * @throws IOException e
     */
    LogSequenceNumber writeRedoOnlyUpdatePageRecord(LogSequenceNumber next,DBPage dbPage, int numSegments, byte[] changes) throws IOException;

    void rollbackTransaction(LogSequenceNumber next,int transactionID, LogSequenceNumber lsn) throws IOException;

    /**
     * 强制WAL落盘
     * 
     * @param start 起始的LSN
     * @param end 结束的LSN
     * @return 结束的LSN
     * @throws IOException 文件操作异常
     */
    LogSequenceNumber forceWAL(LogSequenceNumber start, LogSequenceNumber end) throws IOException;
}

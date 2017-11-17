package com.bow.lab.transaction;

import com.bow.maple.storage.DBPage;
import com.bow.maple.transactions.TransactionException;

import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/11.
 */
public interface ITransactionService {

    void initialize()  throws IOException;
    /**
     * 增加transaction id
     * @return transaction id
     */
    int getAndIncrementNextTxnID();

    /**
     * 开启一个事务
     * @param userStarted 用户启动
     * @throws TransactionException 事务异常
     */
    void startTransaction(boolean userStarted) throws TransactionException;

    /**
     * 记录db page
     * @param dbPage 数据页
     * @throws IOException e
     */
    void recordPageUpdate(DBPage dbPage) throws IOException;

    /**
     * 提交事务
     * @throws TransactionException 事务异常
     */
    void commitTransaction() throws TransactionException;

    /**
     * 回滚事务
     * @throws TransactionException 事务异常
     */
    void rollbackTransaction() throws TransactionException;
}

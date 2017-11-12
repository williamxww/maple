package com.bow.lab.transaction;

import com.bow.maple.storage.DBPage;
import com.bow.maple.transactions.TransactionException;

import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/11.
 */
public interface ITransactionService {

    int getAndIncrementNextTxnID();

    void startTransaction(boolean userStarted) throws TransactionException;

    void recordPageUpdate(DBPage dbPage) throws IOException;

    void commitTransaction() throws TransactionException;

    void rollbackTransaction() throws TransactionException;
}

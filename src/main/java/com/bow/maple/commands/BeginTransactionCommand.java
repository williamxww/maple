package com.bow.maple.commands;


import com.bow.maple.transactions.TransactionException;
import com.bow.maple.transactions.TransactionManager;
import com.bow.maple.storage.StorageManager;


/**
 * This class represents a command that starts a transaction, such as
 * <tt>BEGIN</tt>, <tt>BEGIN WORK</tt>, or <tt>START TRANSACTION</tt>.
 */
public class BeginTransactionCommand extends Command {
    public BeginTransactionCommand() {
        super(Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        // Begin a transaction.

        TransactionManager txnMgr =
            StorageManager.getInstance().getTransactionManager();

        try {
            // Pass true for the "user-started transaction" flag, since the
            // user issued the command to do it!
            txnMgr.startTransaction(true);
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}

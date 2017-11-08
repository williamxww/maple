package com.bow.maple.commands;


import com.bow.maple.storage.StorageManager;
import com.bow.maple.transactions.TransactionException;
import com.bow.maple.transactions.TransactionManager;


/**
 * This class represents a command that commits a transaction, such as
 * <tt>COMMIT</tt> or <tt>COMMIT WORK</tt>.
 */
public class CommitTransactionCommand extends Command {
    public CommitTransactionCommand() {
        super(Type.UTILITY);
    }


    public void execute() throws ExecutionException {
        // Commit the transaction.

        TransactionManager txnMgr =
            StorageManager.getInstance().getTransactionManager();

        try {
            txnMgr.commitTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}

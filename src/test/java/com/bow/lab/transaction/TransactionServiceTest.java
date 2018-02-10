package com.bow.lab.transaction;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/15.
 */
public class TransactionServiceTest extends AbstractTest{


    private ITransactionService service;

    @Before
    public void setup() throws IOException {
        super.setup();
        service = new TransactionService(storageService);
    }

    @Test
    public void recordPageUpdate() throws Exception {
        service.initialize();
        service.startTransaction(true);
        service.recordPageUpdate(writeDbPage());
        service.commitTransaction();
    }

    public DBPage writeDbPage() throws IOException {
        DBFile dbFile = storageService.createDBFile("testData", DBFileType.CS_DATA_FILE, DBFile.DEFAULT_PAGESIZE);
        DBPage dbPage = storageService.loadDBPage(dbFile, 0);
        dbPage.write(6, new byte[] { 0x1F, 0x2F, 0x3F, 0x4F, 0x5F });
        return dbPage;
    }

    @Test
    public void rollbackTransaction() throws Exception {
        service.rollbackTransaction();
    }

}
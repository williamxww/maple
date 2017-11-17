package com.bow.lab.transaction;

import com.bow.lab.storage.BufferService;
import com.bow.lab.storage.StorageService;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/17.
 */
public class AbstractTest {

    protected StorageService storageService;

    public void setup() {
        File dir = new File("test");
        FileManager fileManager = new FileManager(dir);
        BufferService bufferService = new BufferService(fileManager);
        storageService = new StorageService(fileManager, bufferService);
    }

    public DBPage writeDbPage() throws IOException{
        DBFile dbFile = storageService.createDBFile("testData", DBFileType.CS_DATA_FILE, DBFile.DEFAULT_PAGESIZE);
        DBPage dbPage = storageService.loadDBPage(dbFile, 0);
        dbPage.write(6, new byte[] { 0x1F, 0x2F, 0x3F, 0x4F, 0x5F });
        return dbPage;
    }


}

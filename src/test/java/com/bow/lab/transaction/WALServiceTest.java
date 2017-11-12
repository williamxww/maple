package com.bow.lab.transaction;

import com.bow.lab.storage.BufferService;
import com.bow.lab.storage.StorageService;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;
import com.bow.maple.storage.heapfile.HeaderPage;
import com.bow.maple.storage.writeahead.LogSequenceNumber;
import com.bow.maple.storage.writeahead.WALRecordType;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author vv
 * @since 2017/11/11.
 */
public class WALServiceTest {

    private IWALService service;

    private StorageService storageService;

    @Before
    public void setup() {
        File dir = new File("test");
        FileManager fileManager = new FileManager(dir);
        BufferService bufferService = new BufferService(fileManager);
        storageService = new StorageService(fileManager, bufferService);
        service = new WALService(storageService, bufferService);
    }

    @Test
    public void createWALFile() throws Exception {
        DBFile dbFile = service.createWALFile(1);
        System.out.println(dbFile);
    }

    @Test
    public void openWALFile() throws Exception {
        DBFile dbFile = service.openWALFile(1);
        System.out.println(dbFile);
    }

    @Test
    public void writeTxnRecord() throws Exception {
        LogSequenceNumber begin = new LogSequenceNumber(0, WALService.OFFSET_FIRST_RECORD);
        LogSequenceNumber next = service.writeTxnRecord(begin, WALRecordType.START_TXN, 8, null);
        service.forceWAL(begin, next);
    }

    @Test
    public void writeTxnRecord2() throws Exception {
        LogSequenceNumber prev = new LogSequenceNumber(0, WALService.OFFSET_FIRST_RECORD);
        LogSequenceNumber begin = new LogSequenceNumber(0, 12);
        LogSequenceNumber next = service.writeTxnRecord(begin, WALRecordType.COMMIT_TXN, 8, prev);
        service.forceWAL(begin, next);
    }

    @Test
    public void writeUpdatePageRecord() throws Exception {
        DBFile dbFile = storageService.createDBFile("testData", DBFileType.CS_DATA_FILE, DBFile.DEFAULT_PAGESIZE);
        DBPage dbPage = storageService.loadDBPage(dbFile,0);
        dbPage.write(6, new byte[]{0x1F,0x2F,0x3F,0x4F,0x5F});

        LogSequenceNumber begin = new LogSequenceNumber(0, 12);
        LogSequenceNumber next =service.writeUpdatePageRecord(begin,dbPage);
        next = service.writeTxnRecord(next, WALRecordType.COMMIT_TXN, 8, begin);
        System.out.println(next);
    }

}
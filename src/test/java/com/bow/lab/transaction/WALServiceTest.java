package com.bow.lab.transaction;

import com.bow.lab.storage.BufferService;
import com.bow.lab.storage.StorageService;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.FileManager;
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

    @Before
    public void setup(){
        File dir = new File("test");
        FileManager fileManager = new FileManager(dir);
        BufferService bufferService = new BufferService(fileManager);
        StorageService storageService = new StorageService(fileManager,bufferService);
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
        LogSequenceNumber next = service.writeTxnRecord(begin, WALRecordType.START_TXN, 1,null);
        service.forceWAL(begin, next);
        System.out.println(next);
    }

}
package com.bow.lab.storage;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;


/**
 * @author vv
 * @since 2017/11/10.
 */
public class StorageServiceTest {

    private StorageService service;

    @Before
    public void setup(){
        File dir = new File("test");
        IFileService fileManager = new FileService(dir);
        BufferService bufferManager = new BufferService(fileManager);
        service = new StorageService(fileManager,bufferManager);
    }

    @Test
    public void createDBFile() throws Exception {
        DBFile dbFile = service.createDBFile("foo", DBFileType.FRM_FILE, DBFile.DEFAULT_PAGESIZE);
        File file = dbFile.getDataFile();
        System.out.println(file.exists());
    }

    @Test
    public void openDBFile() throws Exception {
        DBFile dbFile = service.openDBFile("foo");
        File file = dbFile.getDataFile();
        System.out.println(file.exists());
    }

    @Test
    public void loadDBPage() throws Exception {
        DBFile dbFile = service.openDBFile("foo");
        DBPage firstPage = service.loadDBPage(dbFile,0);
        byte a = firstPage.readByte(0);
        System.out.println(a);
    }

}
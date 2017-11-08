package com.bow.maple.storage;

import java.io.File;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 此类便于演示FileManager的功能
 */
@Ignore
public class TestFileManager {

    private FileManager fileMgr;

    @Before
    public void setup() {
        File baseDir = new File("test");
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
        fileMgr = new FileManager(baseDir);
    }

    @Test
    public void createDBFile() throws Exception {
        DBFile dbf = fileMgr.createDBFile("foo", DBFileType.FRM_FILE, DBFile.MIN_PAGESIZE);
        File f = dbf.getDataFile();
        System.out.println(f.getName());
    }

    @Test
    public void openDBFile() throws Exception {
        DBFile dbf = fileMgr.openDBFile("foo");
        DBPage dbp = fileMgr.loadDBPage(dbf, 0);
        byte b = dbp.readByte(0);
        System.out.println(b);
    }

    @Test
    public void deleteDBFile() throws Exception {
        DBFile dbf = fileMgr.openDBFile("foo");
        fileMgr.deleteDBFile(dbf);
    }

}

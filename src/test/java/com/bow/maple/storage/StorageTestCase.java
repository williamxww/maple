package com.bow.maple.storage;

import java.io.File;

public class StorageTestCase {

    protected File testBaseDir;

    public StorageTestCase() {
        testBaseDir = new File("test");
        if (!testBaseDir.exists()) {
            testBaseDir.mkdirs();
        }
    }
}

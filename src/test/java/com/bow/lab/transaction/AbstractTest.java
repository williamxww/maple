package com.bow.lab.transaction;

import com.bow.lab.storage.BufferService;
import com.bow.lab.storage.FileService;
import com.bow.lab.storage.IBufferService;
import com.bow.lab.storage.IFileService;
import com.bow.lab.storage.IStorageService;
import com.bow.lab.storage.StorageService;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;
import com.bow.maple.util.ExtensionLoader;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/17.
 */
public class AbstractTest {

    protected IStorageService storageService;

    public void setup() throws IOException {
        // 创建测试用组件，然后交由ExtensionLoader容器管理
        File dir = new File("test");
        IFileService fileService = new FileService(dir);
        IBufferService bufferService = new BufferService(fileService);
        storageService = new StorageService(fileService, bufferService);
        ExtensionLoader.getExtensionLoader(IFileService.class).putExtension(fileService);
        ExtensionLoader.getExtensionLoader(IBufferService.class).putExtension(bufferService);
        ExtensionLoader.getExtensionLoader(IStorageService.class).putExtension(storageService);
    }



}

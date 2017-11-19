package com.bow.lab.storage;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.StorageManager;
import com.bow.maple.util.ExtensionLoader;

import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/9.
 */
public class StorageService implements IStorageService {

    private IBufferService bufferService;

    private IFileService fileService;

    /**
     * 传入fileManager和bufferManager构造一个StorageService实例
     * 
     * @param fileService 文件管理器
     * @param bufferService 缓存管理器
     */
    public StorageService(IFileService fileService, IBufferService bufferService) {
        this.bufferService = bufferService;
        this.fileService = fileService;
    }

    /**
     * 默认构造器供 service loader 加载
     */
    public StorageService(){
        this.bufferService = ExtensionLoader.getExtensionLoader(IBufferService.class).getExtension();
        this.fileService = ExtensionLoader.getExtensionLoader(IFileService.class).getExtension();;
    }

    @Override
    public DBFile createDBFile(String filename, DBFileType type, int pageSize) throws IOException {
        if (bufferService.getFile(filename) != null) {
            throw new IllegalStateException(
                    "A file " + filename + " is already cached in the Buffer Manager!  Does it already exist?");
        }
        DBFile dbFile = fileService.createDBFile(filename, type, StorageManager.getCurrentPageSize());
        bufferService.addFile(dbFile);
        return dbFile;
    }

    /**
     * 打开指定文件
     * 
     * @param filename 文件名，如xxx.tbl
     * @return 文件对象
     * @throws IOException 当指定文件不存在的时候会抛出错误
     */
    @Override
    public DBFile openDBFile(String filename) throws IOException {
        DBFile dbFile = bufferService.getFile(filename);
        if (dbFile == null) {
            dbFile = fileService.openDBFile(filename);
            bufferService.addFile(dbFile);
        }
        return dbFile;
    }

    @Override
    public DBFile getFile(String filename) {
        return bufferService.getFile(filename);
    }

    @Override
    public void closeDBFile(DBFile dbFile) throws IOException {
        bufferService.removeDBFile(dbFile);
        fileService.closeDBFile(dbFile);
    }

    /**
     * 加载指定页
     *
     * @param dbFile 数据文件
     * @param pageNo 页号
     * @return 数据页
     * @throws java.io.EOFException 加载了不在dbFile内的页
     */
    @Override
    public DBPage loadDBPage(DBFile dbFile, int pageNo) throws IOException {
        return loadDBPage(dbFile, pageNo, false);
    }

    /**
     * 加载指定页
     * 
     * @param dbFile 数据文件
     * @param pageNo 页号
     * @param create true,没有时就创建
     * @return 数据页
     * @throws java.io.EOFException create设置为false时，加载了不在dbFile内的页
     */
    @Override
    public DBPage loadDBPage(DBFile dbFile, int pageNo, boolean create) throws IOException {

        // 从buffer中获取
        DBPage dbPage = bufferService.getPage(dbFile, pageNo);
        if (dbPage == null) {
            dbPage = fileService.loadDBPage(dbFile, pageNo, create);
            bufferService.addPage(dbPage);
        }
        return dbPage;
    }

    @Override
    public void unpinDBPage(DBPage dbPage) {
        bufferService.unpinPage(dbPage);
    }

    @Override
    public void flushDBFile(DBFile dbFile) throws IOException {
        bufferService.flushDBFile(dbFile);
    }

    @Override
    public void writeDBFile(DBFile dbFile, int minPageNo, int maxPageNo, boolean sync) throws IOException {
        bufferService.writeDBFile(dbFile, minPageNo, maxPageNo, sync);
    }

    @Override
    public void writeDBFile(DBFile dbFile, boolean sync) throws IOException {
        bufferService.writeDBFile(dbFile, sync);
    }

    @Override
    public void writeAll(boolean sync) throws IOException {
        bufferService.writeAll(sync);
    }
}

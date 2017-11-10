package com.bow.lab.storage;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;
import com.bow.maple.storage.StorageManager;

import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/9.
 */
public class StorageService implements IStorageService {

    private BufferService bufferManager;

    private FileManager fileManager;

    /**
     * 传入fileManager和bufferManager构造一个StorageService实例
     * @param fileManager 文件管理器
     * @param bufferManager 缓存管理器
     */
    public StorageService(FileManager fileManager, BufferService bufferManager){
        this.bufferManager = bufferManager;
        this.fileManager = fileManager;
    }

    @Override
    public DBFile createDBFile(String filename, DBFileType type) throws IOException {
        if (bufferManager.getFile(filename) != null) {
            throw new IllegalStateException(
                    "A file " + filename + " is already cached in the Buffer Manager!  Does it already exist?");
        }
        DBFile dbFile = fileManager.createDBFile(filename, type, StorageManager.getCurrentPageSize());
        bufferManager.addFile(dbFile);
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
        DBFile dbFile = bufferManager.getFile(filename);
        if (dbFile == null) {
            dbFile = fileManager.openDBFile(filename);
            bufferManager.addFile(dbFile);
        }
        return dbFile;
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
        DBPage dbPage = bufferManager.getPage(dbFile, pageNo);
        if (dbPage == null) {
            dbPage = fileManager.loadDBPage(dbFile, pageNo, create);
            bufferManager.addPage(dbPage);
        }
        return dbPage;
    }

    @Override
    public void unpinDBPage(DBPage dbPage) {
        bufferManager.unpinPage(dbPage);
    }
}

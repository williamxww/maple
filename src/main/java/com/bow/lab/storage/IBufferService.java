package com.bow.lab.storage;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;

import java.io.IOException;
import java.util.List;

/**
 * 缓存服务
 * 
 * @author vv
 * @since 2017/11/16.
 */
public interface IBufferService {

    /**
     * 从缓存中取出DBFile
     * 
     * @param filename 文件名
     * @return DBFile
     */
    DBFile getFile(String filename);

    /**
     * 将dbFile存放在内存中
     *
     * @param dbFile 打开的文件
     */
    void addFile(DBFile dbFile);

    void pinPage(DBPage dbPage);

    void unpinPage(DBPage dbPage);

    void unpinAllPages();

    DBPage getPage(DBFile dbFile, int pageNo);

    /**
     * 将指定page加入到缓存中
     *
     * @param dbPage 数据页
     * @throws IOException e
     */
    void addPage(DBPage dbPage) throws IOException;

    /**
     * 将dbFile中指定范围内的脏数据页刷出到磁盘
     * 
     * @param dbFile 指定文件
     * @param minPageNo 指定页范围
     * @param maxPageNo 指定页范围
     * @param sync 同步到磁盘{@link FileManager#syncDBFile(DBFile)}
     * @throws IOException 文件操作异常
     */
    void writeDBFile(DBFile dbFile, int minPageNo, int maxPageNo, boolean sync) throws IOException;

    /**
     * dbFile中所有脏页刷出到磁盘
     * 
     * @param dbFile 数据文件
     * @param sync 同步
     * @throws IOException 文件操作异常
     */
    void writeDBFile(DBFile dbFile, boolean sync) throws IOException;

    /**
     * 将所有的缓存页刷出到磁盘
     * 
     * @param sync 同步到磁盘
     * @throws IOException 文件异常
     */
    void writeAll(boolean sync) throws IOException;

    /**
     * 将dbFile的缓存页刷出到磁盘，并减少统计数量。
     *
     * @param dbFile 指定文件
     * @throws IOException 文件操作异常
     */
    void flushDBFile(DBFile dbFile) throws IOException;

    /**
     * 将所有的缓存页刷出到磁盘，并删除缓存页
     *
     * @throws IOException 文件操作异常
     */
    void flushAll() throws IOException;

    /**
     * 将dbFile从缓存中移除，先将所有缓存页刷出
     * 
     * @param dbFile dbFile
     * @throws IOException 文件操作异常
     */
    void removeDBFile(DBFile dbFile) throws IOException;

    /**
     * 移除缓存中所有的文件，一般是在系统关闭时调用。
     * 
     * @return 被移除的List<DBFile>
     * @throws IOException 文件操作异常
     */
    List<DBFile> removeAll() throws IOException;
}

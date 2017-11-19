package com.bow.lab.storage;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;

import java.io.File;
import java.io.IOException;

/**
 * @author vv
 * @since 2017/11/19.
 */
public interface IFileService {

    DBFile createDBFile(String filename, DBFileType type, int pageSize) throws IOException;

    DBFile initDBFile(File f, DBFileType type, int pageSize) throws IOException;

    DBFile openDBFile(String filename) throws IOException;

    DBPage loadDBPage(DBFile dbFile, int pageNo, boolean create) throws IOException;

    DBPage loadDBPage(DBFile dbFile, int pageNo) throws IOException;

    /**
     * Saves a page to the DB file, and then clears the page's dirty flag. Note
     * that the data might not actually be written to disk until a sync
     * operation is performed.
     * @param page the page to write back to the data file
     * @throws IOException if an error occurs while writing the page to disk
     */
    void saveDBPage(DBPage page) throws IOException;

    /**
     * 此方法确保所有的写操作都已同步到磁盘。一般即使调用了{@link #saveDBPage}，
     * 文件系统或是磁盘驱动因为一些原因缓冲了这些数据。若操作系统崩溃，这部分数据回丢失。本方法可以确保所有的数据真正写到了磁盘。
     *
     * @param dbFile the database file to synchronize
     * @throws java.io.SyncFailedException e
     * @throws IOException 文件IO错误
     */
    void syncDBFile(DBFile dbFile) throws IOException;

    void closeDBFile(DBFile dbFile) throws IOException;

    void deleteDBFile(String filename) throws IOException;

    void deleteDBFile(File f) throws IOException;

    void deleteDBFile(DBFile dbFile) throws IOException;
}

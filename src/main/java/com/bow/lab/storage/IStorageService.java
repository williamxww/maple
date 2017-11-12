package com.bow.lab.storage;

import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;

import java.io.IOException;

/**
 * 存储服务，供上层组件操作文件，通过buffer管理加速文件的读取。
 * 
 * @author vv
 * @since 2017/11/9.
 */
public interface IStorageService {

    /**
     * 创建数据文件
     * 
     * @param filename 文件名
     * @param type 文件类型
     * @return 数据文件
     * @throws IOException if the specified file already exists.
     */
    DBFile createDBFile(String filename, DBFileType type, int pageSize) throws IOException;

    /**
     * 打开数据文件
     * 
     * @param filename 文件名称
     * @return 数据文件
     * @throws IOException 文件不存在
     */
    DBFile openDBFile(String filename) throws IOException;

    void closeDBFile(DBFile dbFile) throws IOException;
    /**
     * 加载指定页
     *
     * @param dbFile 数据文件
     * @param pageNo 页号
     * @return 数据页
     * @throws java.io.EOFException 加载了不在dbFile内的页
     */
    DBPage loadDBPage(DBFile dbFile, int pageNo) throws IOException;

    /**
     * 加载指定页
     *
     * @param dbFile 数据文件
     * @param pageNo 页号
     * @param create true,没有时就创建
     * @return 数据页
     * @throws java.io.EOFException create设置为false时，加载了不在dbFile内的页
     */
    DBPage loadDBPage(DBFile dbFile, int pageNo, boolean create) throws IOException;

    void unpinDBPage(DBPage dbPage);

    void flushDBFile(DBFile dbFile) throws IOException;
}

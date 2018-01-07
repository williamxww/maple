package com.bow.lab.storage;


import java.io.IOException;
import java.util.Map;

import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.FilePointer;
import com.bow.maple.storage.InvalidFilePointerException;
import com.bow.maple.storage.TableFileInfo;

/**
 * 数据级别的相关操作，如增删改查相关数据。
 * @author vv
 * @since 2017/11/8.
 */
public interface ITableService {


    /**
     * 根据TableFileInfo创建表文件
     * @param tblFileInfo
     * @throws IOException
     */
    void createTable(TableFileInfo tblFileInfo) throws IOException;

    /**
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    TableFileInfo openTable(String tableName) throws IOException;
    /**
     *
     * @param tblFileInfo
     * @throws IOException
     */
    void closeTable(TableFileInfo tblFileInfo) throws IOException;
    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if there are
     * no tuples in the file.
     *
     * @param tblFileInfo the opened table to get the first tuple from
     *
     * @return the first tuple, or <tt>null</tt> if the table is empty
     *
     * @throws IOException if an IO error occurs while trying to read out the
     *         first tuple
     */
    Tuple getFirstTuple(TableFileInfo tblFileInfo) throws IOException;


    /**
     * Returns the tuple that follows the specified tuple, or <tt>null</tt> if
     * there are no more tuples in the file.
     *
     * @param tblFileInfo the opened table to get the next tuple from
     *
     * @param tup the "previous" tuple in the table
     *
     * @return the tuple following the previous tuple, or <tt>null</tt> if the
     *         previous tuple is the last one in the table
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         next tuple.
     */
    Tuple getNextTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException;


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by other features in the database, such as indexes.
     *
     * @param tblFileInfo the opened table to get the specified tuple from
     *
     * @param fptr a file-pointer specifying the tuple to retrieve
     *
     * @return the tuple referenced by <tt>fptr</tt>
     *
     * @throws InvalidFilePointerException if the specified file-pointer doesn't
     *         actually point to a real tuple.
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         specified tuple.
     */
    Tuple getTuple(TableFileInfo tblFileInfo, FilePointer fptr)
        throws InvalidFilePointerException, IOException;


    /**
     * Adds the specified tuple into the table file, returning a new object
     * corresponding to the actual tuple added to the table.
     *
     * @param tblFileInfo the opened table to add the tuple to
     *
     * @param tup a tuple object containing the values to add to the table
     *
     * @return a tuple object actually backed by this table
     *
     * @throws IOException if an IO error occurs while trying to add the new
     *         tuple to the table.
     */
    Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException;


    /**
     * Modifies the values in the specified tuple.
     *
     * @param tblFileInfo the opened table to add the tuple to
     *
     * @param tup the tuple to modify in the table
     *
     * @param newValues a map containing the name/value pairs to use to update
     *        the tuple.  Values in this map will be coerced to the column-type
     *        of the specified columns.  Only the columns being modified need to
     *        be specified in this collection.
     *
     * @throws IOException if an IO error occurs while trying to modify the
     *         tuple's values.
     */
    void updateTuple(TableFileInfo tblFileInfo, Tuple tup,
                     Map<String, Object> newValues) throws IOException;


    /**
     * Deletes the specified tuple from the table.
     *
     * @param tblFileInfo the opened table to delete the tuple from
     *
     * @param tup the tuple to delete from the table
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         tuple.
     */
    void deleteTuple(TableFileInfo tblFileInfo, Tuple tup)
        throws IOException;


}

package com.bow.lab.indexes;


import com.bow.maple.relations.ColumnIndexes;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.storage.DBFile;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.TableFileInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * This class is used to hold information about a single index in the database.
 * It stores the table's name, the schema details of the table, and the
 * {@link DBFile} object where the table's data is actually stored.
 */
public class IndexFileInfo {

    /** The name of the index. */
    private String indexName;


    /** The details of the table that the index is built against. */
    private TableFileInfo tblFileInfo;


    /** The actual details about what columns are in the index, etc. */
    private IndexInfo indexInfo;


    /**
     * The type of index to create.  The default type is
     * {@link DBFileType#BTREE_INDEX_FILE}.
     */
    private DBFileType indexType = DBFileType.BTREE_INDEX_FILE;


    /** The index manager used to access this index file. */
    private IndexManager indexManager;


    /**
     * If the index file has been opened, this is the actual data file that
     * the index is stored in.  Otherwise, this will be <tt>null</tt>.
     */
    private DBFile dbFile;


    /**
     * This value specifies what table-columns appear in the index, by
     * specifying the indexes of the columns from the schema.
     */
    private ColumnIndexes columnIndexes;
    

    /**
     * This array of column-info objects describes the schema of the index.
     */
    private ArrayList<ColumnInfo> columnInfos;


    /**
     * @param indexName
     * @param tblFileInfo
     * @param idxFile
     */
    public IndexFileInfo(String indexName, TableFileInfo tblFileInfo,
                         DBFile idxFile) {
        // if (indexName == null)
        //     throw new IllegalArgumentException("indexName must be specified");

        if (tblFileInfo == null)
            throw new IllegalArgumentException("tblFileInfo must be specified");

        this.indexName = indexName;
        this.tblFileInfo = tblFileInfo;
        this.dbFile = idxFile;
    }


    /**
     * Construct an index file information object for the specified index name.
     * This constructor is used by the <tt>CREATE TABLE</tt> command to hold the
     * table's schema, before the table has actually been created.  After the
     * table is created, the {@link #setDBFile} method is used to store the
     * database-file object onto this object.
     *
     * @param indexName the name of the index that this object represents
     * @param tblFileInfo details of the table that the index is built against
     */
    public IndexFileInfo(String indexName, TableFileInfo tblFileInfo,
                         IndexInfo indexInfo) {
        // if (indexName == null)
        //     throw new IllegalArgumentException("indexName must be specified");

        if (tblFileInfo == null)
            throw new IllegalArgumentException("tblFileInfo must be specified");

        this.indexName = indexName;
        this.tblFileInfo = tblFileInfo;
        this.indexInfo = indexInfo;


    }


    /**
     *
     * @param schema the table-schema object for the table that the index is
     *        defined on.
     *
     * @param indexName the unique name of the index.
     */
    private void initIndexDetails(TableSchema schema, String indexName) {
        columnIndexes = schema.getIndexes().get(indexName);
        if (columnIndexes == null) {
            throw new IllegalArgumentException("No index named " + indexName +
                " on schema " + schema);
        }

        // Get the schema of the index so that we can interpret the key-values.
        columnInfos = schema.getColumnInfos(columnIndexes);
        columnInfos.add(new ColumnInfo("#TUPLE_FP",
            new ColumnType(SQLDataType.FILE_POINTER)));
    }

    
    public DBFileType getIndexType() {
        return indexType;
    }

    
    public IndexInfo getIndexInfo() {
        return indexInfo;
    }
    

    /**
     * Returns the actual database file that holds this index's data.
     *
     * @return the actual database file that holds this index's data, or
     *         <tt>null</tt> if it hasn't yet been set.
     */
    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Method for storing the database-file object onto this index-file
     * information object, for example after successful completion of a
     * <tt>CREATE INDEX</tt> command.
     *
     * @param dbFile the database file that the index's data is stored in.
     */
    public void setDBFile(DBFile dbFile) {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile must not be null!");

        if (this.dbFile != null)
            throw new IllegalStateException("This object already has a dbFile!");

        this.dbFile = dbFile;
    }


    /**
     * Returns the index name.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    

    /**
     * Returns the associated table name.
     *
     * @return the associated table name
     */
    public String getTableName() {
        return tblFileInfo.getTableName();
    }


    public List<ColumnInfo> getIndexSchema() {
        if (columnInfos == null)
            initIndexDetails(tblFileInfo.getSchema(), indexName);

        return Collections.unmodifiableList(columnInfos);
    }


    public ColumnIndexes getTableColumnIndexes() {
        if (columnIndexes == null)
            initIndexDetails(tblFileInfo.getSchema(), indexName);

        return columnIndexes;
    }



    public IndexManager getIndexManager() {
        return indexManager;
    }


    public void setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }
}



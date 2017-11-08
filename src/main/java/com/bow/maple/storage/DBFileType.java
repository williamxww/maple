package com.bow.maple.storage;


/**
 * This enumeration specifies the different types of data file that the
 * database knows about.  Each file type is assigned a unique integer value in
 * the range [0, 255], which is stored as the very first byte of data files of
 * that type.  This way, it is straightforward to determine a file's type by
 * examination.
 */
public enum DBFileType {

    /**
     * Represents a heap data file, which supports variable-size tuples and
     * stores them in no particular order.
     * 支持变长元组存储，并且是没有指定顺序的
     */
    HEAP_DATA_FILE(0x01),


    /**
     * Represents an index file that uses a btree structure to organize the
     * data.
     */
    BTREE_INDEX_FILE(0x0A),


    /**
     * Represents a transaction-state file used for write-ahead logging and
     * recovery.
     */
    TXNSTATE_FILE(0x14),

    /**
     * Represents a write-ahead log file used for transaction processing and
     * recovery.
     */
    WRITE_AHEAD_LOG_FILE(0x15),
    
    
    /**
     * 列式存储的文件类型
     */
    CS_DATA_FILE(0x1F),

    /**
     * 表结构描述文件
     */
    FRM_FILE(0x20);


    private int id;


    DBFileType(int id) {
        this.id = id;
    }


    public int getID() {
        return id;
    }


    /**
     * Given a numeric type ID, returns the corresponding type value for the ID,
     * or <tt>null</tt> if no type corresponds to the ID.
     *
     * @param id the numeric ID of the type to retrieve
     *
     * @return the type-value with that ID, or <tt>null</tt> if not found
     */
    public static DBFileType valueOf(int id) {
        for (DBFileType type : values()) {
            if (type.id == id)
                return type;
        }
        return null;
    }
}

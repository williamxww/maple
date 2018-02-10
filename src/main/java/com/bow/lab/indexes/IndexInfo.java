package com.bow.lab.indexes;


import com.bow.maple.relations.ColumnIndexes;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.relations.Schema;
import com.bow.maple.relations.TableConstraintType;


/**
 * 索引详细信息
 */
public class IndexInfo {
    /**
     * 索引类型：hash index, order index, bitmap index
     */
    private IndexType type;


    /** The name of the table that the index is built against. */
    private String tableName;


    /** The schema for the table that the index is built against. */
    private Schema tableSchema;


    /**
     * 约束类型：主键，外键等
     */
    private TableConstraintType constraintType;


    /**
     * A flag indicating whether the index is a unique index (i.e. each value
     * appears only once) or not.
     */
    private boolean unique;


    /**
     * 索引所在的列
     */
    private ColumnIndexes columnIndexes;


    public TableConstraintType getConstraintType() {
        return constraintType;
    }
    
    
    public IndexInfo(String tableName, TableSchema tableSchema,
                     ColumnIndexes columnIndexes, boolean unique) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.columnIndexes = columnIndexes;
        this.unique = unique;
    }


    public void setConstraintType(TableConstraintType constraintType) {
        this.constraintType = constraintType;
    }


    public ColumnIndexes getColumnIndexes() {
        return columnIndexes;
    }
}

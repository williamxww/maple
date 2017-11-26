package com.bow.maple.storage;

import java.util.Collections;
import java.util.List;

import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.Tuple;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.Schema;

/**
 * <p>
 * This class is a partial implementation of the {@link Tuple} interface that
 * handles reading and writing tuple data against a {@link DBPage} object. This
 * can be used to read and write tuples in a table file, keys in an index file,
 * etc. It could also be used to store and manage tuples in memory, although
 * it's generally much faster and simpler to use an optimized in-memory
 * representation for tuples in memory.
 * </p>
 * <p>
 * Each tuple is stored in a layout like this:
 * </p>
 * <ul>
 * <li>The first one or more bytes are dedicated to a <tt>NULL</tt>-bitmap,
 * which records columns that are currently <tt>NULL</tt>.</li>
 * <li>The remaining bytes are dedicated to storing the non-<tt>NULL</tt> values
 * for the columns in the tuple.</li>
 * </ul>
 * <p>
 * In order to make this class' functionality generic, certain operations must
 * be implemented by subclasses: specifically, any operation that changes a
 * tuple's size (e.g. writing a non-<tt>NULL</tt> value to a previously
 * <tt>NULL</tt> column or vice versa, or changing the size of a variable-size
 * column). The issue with these operations is that they require page-level data
 * management, which is beyond the scope of what this class can provide. Thus,
 * concrete subclasses of this class can provide page-level data management as
 * needed.
 * </p>
 */
public abstract class PageTuple implements Tuple {

    /**
     * 当列值为NULL时，{@link #valueOffsets}中对应值设置为NULL_OFFSET
     */
    public static final int NULL_OFFSET = 0;

    private DBPage dbPage;

    /**
     * tuple在页中的起始位置
     */
    private int pageOffset;

    /**
     * 此tuple的结束位置(最后一个字节的下一byte)
     */
    private int endOffset;

    /**
     * The columns that appear within the tuple. We don't use a {@link Schema}
     * object so that we can use this class in a wider range of contexts.
     */
    private List<ColumnInfo> colInfos;

    /**
     * 此tuple中每个值的offset，NULL的offset被设置为0
     * 
     * @see #NULL_OFFSET
     */
    private int[] valueOffsets;

    /**
     * 构造一个能够读取或是写出到dbPage的tuple
     *
     * @param dbPage 拥有此tuple的数据页
     * @param pageOffset tuple在数据页中的偏移量
     * @param colInfos tuple的列信息
     */
    public PageTuple(DBPage dbPage, int pageOffset, List<ColumnInfo> colInfos) {
        if (dbPage == null) {
            throw new NullPointerException("dbPage must be specified");
        }
        if (pageOffset < 0 || pageOffset >= dbPage.getPageSize()) {
            throw new IllegalArgumentException(
                    "pageOffset must be in range [0, " + dbPage.getPageSize() + "); got " + pageOffset);
        }

        this.dbPage = dbPage;
        this.pageOffset = pageOffset;
        this.colInfos = colInfos;
        this.valueOffsets = new int[colInfos.size()];
        computeValueOffsets();
    }

    /**
     * Since page-tuples are backed by data pages managed by the Buffer Manager,
     * they are <em>not</em> cacheable.
     *
     * @return <tt>false</tt> because page-tuples are never cacheable.
     */
    public boolean isCacheable() {
        return false;
    }

    /**
     * This method returns an external reference to the tuple, which can be
     * stored and used to look up this tuple. The external reference is
     * represented as a file-pointer. The default implementation simply returns
     * a file-pointer to the tuple-data itself, but specific storage formats may
     * introduce a level of indirection into external references.
     *
     * @return a file-pointer that can be used to look up this tuple
     */
    public FilePointer getExternalReference() {
        return new FilePointer(dbPage.getPageNo(), pageOffset);
    }

    public DBPage getDBPage() {
        return dbPage;
    }

    public int getOffset() {
        return pageOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    /**
     * tuple存储所占大小
     * 
     * @return tuple存储所占大小.
     */
    public int getSize() {
        return endOffset - pageOffset;
    }

    public List<ColumnInfo> getColumnInfos() {
        return Collections.unmodifiableList(colInfos);
    }

    private void checkColumnIndex(int colIndex) {
        if (colIndex < 0 || colIndex >= colInfos.size()) {
            throw new IllegalArgumentException(
                    "Column index must be in range [0," + (colInfos.size() - 1) + "], got " + colIndex);
        }
    }

    /**
     * 返回列数
     */
    public int getColumnCount() {
        return colInfos.size();
    }

    /**
     * 计算出指定列的值是否为NULL。根据colIndex找出其对应的byte和在byte中的位置，将colIndex对应的bit移动到最低有效位(
     * LSB),然后&0x01
     * 
     * @see #isNullValue(int)
     * @param colIndex 指定列
     * @return 值为NULL则返回true
     */
    private boolean getNullFlag(int colIndex) {
        checkColumnIndex(colIndex);
        int nullFlagOffset = pageOffset + (colIndex / 8);
        // Shift the flags in that byte right, so that the flag for the
        // requested column is in the least significant bit (LSB).
        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);
        nullFlag = nullFlag >> (colIndex % 8);
        // If the LSB is 1 then the column's value is NULL.
        return ((nullFlag & 0x01) != 0);
    }

    /**
     * 给colIndex对应的nullFlag设置值
     * 
     * @param colIndex 指定列
     * @param value 是否为null
     */
    private void setNullFlag(int colIndex, boolean value) {
        checkColumnIndex(colIndex);
        // 找到colIndex对应的null-flag标志位
        int nullFlagOffset = pageOffset + (colIndex / 8);
        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);
        // 找到colIndex对应的bit
        int mask = 1 << (colIndex % 8);
        if (value) {
            nullFlag = nullFlag | mask;
        } else {
            nullFlag = nullFlag & ~mask;
        }
        dbPage.writeByte(nullFlagOffset, nullFlag);
    }

    /**
     * 计算tuple中每个值的偏移量放置于valueOffsets(NULL则赋值0)，整个tuple的结束位置赋值给endOffset
     */
    private void computeValueOffsets() {
        int numCols = colInfos.size();
        int valOffset = getDataStartOffset();
        // 计算每列的offset
        for (int iCol = 0; iCol < numCols; iCol++) {
            if (getNullFlag(iCol)) {
                // 此列的值是NULL
                valueOffsets[iCol] = NULL_OFFSET;
            } else {
                // 缓存offset
                valueOffsets[iCol] = valOffset;
                ColumnType colType = colInfos.get(iCol).getType();
                valOffset += getColumnValueSize(colType, valOffset);
            }
        }
        endOffset = valOffset;
    }

    /**
     * 此tuple真实的起始位置
     * 
     * @return the starting index of the tuple's data
     */
    private int getDataStartOffset() {
        // 计算null-bitmap的大小
        int nullFlagBytes = getNullFlagsSize(colInfos.size());
        return pageOffset + nullFlagBytes;
    }

    /**
     * 获取从valueOffset开始的字段(colType)的长度
     * 
     * @param colType 字段类型
     * @param valueOffset 指定位置
     * @return 字段长度
     */
    private int getColumnValueSize(ColumnType colType, int valueOffset) {
        int dataLength = 0;
        if (colType.getBaseType() == SQLDataType.VARCHAR) {
            // 变长字段VARCHAR的起始2byte存放着数据长度
            dataLength = dbPage.readUnsignedShort(valueOffset);
        }
        return getStorageSize(colType, dataLength);
    }

    public boolean isNullValue(int colIndex) {
        checkColumnIndex(colIndex);
        return (valueOffsets[colIndex] == NULL_OFFSET);
    }

    /**
     * Returns the specified column's value as an <code>Object</code> reference.
     * The actual type of the object depends on the column type, and follows
     * this mapping:
     * <ul>
     * <li><tt>INTEGER</tt> produces {@link java.lang.Integer}</li>
     * <li><tt>SMALLINT</tt> produces {@link java.lang.Short}</li>
     * <li><tt>BIGINT</tt> produces {@link java.lang.Long}</li>
     * <li><tt>TINYINT</tt> produces {@link java.lang.Byte}</li>
     * <li><tt>FLOAT</tt> produces {@link java.lang.Float}</li>
     * <li><tt>DOUBLE</tt> produces {@link java.lang.Double}</li>
     * <li><tt>CHAR(<em>n</em>)</tt> produces {@link java.lang.String}</li>
     * <li><tt>VARCHAR(<em>n</em>)</tt> produces {@link java.lang.String}</li>
     * <li><tt>FILE_POINTER</tt> (internal) produces {@link FilePointer}</li>
     * </ul>
     */
    public Object getColumnValue(int colIndex) {
        checkColumnIndex(colIndex);

        Object value = null;
        if (!isNullValue(colIndex)) {
            int offset = valueOffsets[colIndex];

            ColumnType colType = colInfos.get(colIndex).getType();
            switch (colType.getBaseType()) {

                case INTEGER:
                    value = Integer.valueOf(dbPage.readInt(offset));
                    break;

                case SMALLINT:
                    value = Short.valueOf(dbPage.readShort(offset));
                    break;

                case BIGINT:
                    value = Long.valueOf(dbPage.readLong(offset));
                    break;

                case TINYINT:
                    value = Byte.valueOf(dbPage.readByte(offset));
                    break;

                case FLOAT:
                    value = Float.valueOf(dbPage.readFloat(offset));
                    break;

                case DOUBLE:
                    value = Double.valueOf(dbPage.readDouble(offset));
                    break;

                case CHAR:
                    value = dbPage.readFixedSizeString(offset, colType.getLength());
                    break;

                case VARCHAR:
                    value = dbPage.readVarString65535(offset);
                    break;

                case FILE_POINTER:
                    value = new FilePointer(dbPage.readUnsignedShort(offset), dbPage.readUnsignedShort(offset + 2));
                    break;

                default:
                    throw new UnsupportedOperationException("Cannot currently store type " + colType.getBaseType());
            }
        }

        return value;
    }

    /**
     * Sets the column to the specified value, or <tt>NULL</tt> if the value is
     * the Java <tt>null</tt> value.
     *
     * @param colIndex The index of the column to set.
     *
     * @param value the value to set the column to, or <tt>null</tt> if the
     *        column should be set to the SQL <tt>NULL</tt> value.
     */
    public void setColumnValue(int colIndex, Object value) {
        checkColumnIndex(colIndex);

        if (value == null) {
            // TODO: Make sure the column allows NULL values.
            setNullColumnValue(colIndex);
        } else {
            // Update the value stored in the tuple.
            setNonNullColumnValue(colIndex, value);
        }
    }

    /**
     * 将指定列的值改为NULL，并且释放此部分空间
     * 
     * @param iCol the index of the column to set to <tt>NULL</tt>
     */
    private void setNullColumnValue(int iCol) {
        if (!isNullValue(iCol)) {
            // Mark the value as NULL in the NULL-flags.
            setNullFlag(iCol, true);
            // 获取列宽
            ColumnType colType = colInfos.get(iCol).getType();
            int dataLength = 0;
            if (colType.getBaseType() == SQLDataType.VARCHAR) {
                dataLength = dbPage.readUnsignedShort(valueOffsets[iCol]);
            }
            int valueSize = getStorageSize(colType, dataLength);
            // 删除此列的值
            deleteTupleDataRange(valueOffsets[iCol], valueSize);
            // 在此值前面的offset都要加上valueSize
            for (int jCol = 0; jCol < iCol; jCol++) {
                if (valueOffsets[jCol] != NULL_OFFSET)
                    valueOffsets[jCol] += valueSize;
            }
            valueOffsets[iCol] = NULL_OFFSET;
        }
    }

    /**
     * 更改colIndex列的值value
     * 
     * @param colIndex 指定列.
     * @param value 指定值.
     * @throws NullPointerException if the specified value is <tt>null</tt>
     */
    private void setNonNullColumnValue(int colIndex, Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        ColumnInfo colInfo = colInfos.get(colIndex);
        ColumnType colType = colInfo.getType();

        // 计算原有字段的offset和dataSize
        int oldDataSize;
        int offset = valueOffsets[colIndex];
        if (offset == NULL_OFFSET) {
            // 找出前一个非NULL的列，取出其offset然后加上他的长度即是当前列的offset
            int prevCol = colIndex - 1;
            while (prevCol >= 0 && valueOffsets[prevCol] == NULL_OFFSET) {
                prevCol--;
            }
            if (prevCol < 0) {
                // 该列已是最前列了
                offset = getDataStartOffset();
            } else {
                // offset = prevOffset+prevColumnSize
                int prevOffset = valueOffsets[prevCol];
                ColumnType prevType = colInfos.get(prevCol).getType();
                offset = prevOffset + getColumnValueSize(prevType, prevOffset);
            }
            oldDataSize = 0;
        } else {
            oldDataSize = getColumnValueSize(colType, offset);
        }

        // 获取新字段的dataSize
        int newDataLength = 0;
        if (colType.getBaseType() == SQLDataType.VARCHAR) {
            String strValue = TypeConverter.getStringValue(value);
            newDataLength = strValue.length();
        }
        int newDataSize = getStorageSize(colType, newDataLength);

        // 为新字段申请空间
        if (newDataSize > oldDataSize) {
            insertTupleDataRange(offset, newDataSize - oldDataSize);
        } else {
            deleteTupleDataRange(offset, oldDataSize - newDataSize);
        }

        // 写值
        writeNonNullValue(dbPage, offset, colType, value);

        // 更新valueOffsets
        int changed = newDataSize - oldDataSize;
        for (int jCol = 0; jCol <= colIndex; jCol++) {
            if (valueOffsets[jCol] != NULL_OFFSET) {
                valueOffsets[jCol] -= changed;
            }
        }
    }

    /**
     * NULL Flag的大小。tuple中每列需要一个bit标识该值是否为NULL,若有9列则需要2Byte
     * 
     * @param numCols tuple的列数
     * @return null-bitmap所需字节数
     *
     * @review (donnie) This is really a table-file-level computation, not a
     *         tuple-level computation.
     */
    public static int getNullFlagsSize(int numCols) {
        if (numCols < 0) {
            throw new IllegalArgumentException("numCols must be >= 0; got " + numCols);
        }
        int nullFlagsSize = 0;
        if (numCols > 0) {
            nullFlagsSize = 1 + (numCols - 1) / 8;
        }
        return nullFlagsSize;
    }

    /**
     * 获取指定类型列的大小
     * 
     * @param colType 列类型
     * @param dataLength VARCHAR需要传入数据实际长度
     * @return 指定列的宽度
     */
    public static int getStorageSize(ColumnType colType, int dataLength) {
        int size;

        switch (colType.getBaseType()) {

            case INTEGER:
            case FLOAT:
                size = 4;
                break;

            case SMALLINT:
                size = 2;
                break;

            case BIGINT:
            case DOUBLE:
                size = 8;
                break;

            case TINYINT:
                size = 1;
                break;

            case CHAR:
                // CHAR是定长，长度由其length指定
                size = colType.getLength();
                break;

            case VARCHAR:
                // VARCHAR是变长，长度在其最开始的2byte中存储
                size = 2 + dataLength;
                break;

            case FILE_POINTER:
                // File-pointer = pageNo(2B)+offset(2B)
                size = 4;
                break;

            default:
                throw new UnsupportedOperationException("Cannot currently store type " + colType.getBaseType());
        }

        return size;
    }

    /**
     * 获取tuple的长度
     * 
     * @param columns 列信息
     * @param tuple tuple
     * @return 长度
     */
    public static int getTupleStorageSize(List<ColumnInfo> columns, Tuple tuple) {
        if (columns.size() != tuple.getColumnCount()) {
            throw new IllegalArgumentException("Tuple has different arity than target schema.");
        }
        int storageSize = getNullFlagsSize(columns.size());
        int iCol = 0;
        for (ColumnInfo colInfo : columns) {
            ColumnType colType = colInfo.getType();
            Object value = tuple.getColumnValue(iCol);
            // null不占空间
            if (value != null) {
                // VARCHAR 的长度由开头的2byte确定
                int dataLength = 0;
                if (colType.getBaseType() == SQLDataType.VARCHAR) {
                    String strValue = TypeConverter.getStringValue(value);
                    dataLength = strValue.length();
                }
                storageSize += getStorageSize(colType, dataLength);
            }
            iCol++;
        }
        return storageSize;
    }

    /**
     * 将tuple存入数据页的指定位置
     * 
     * @param dbPage 数据页
     * @param pageOffset 页内的位置
     * @param colInfos 列信息
     * @param tuple tuple
     * @return 存储后的offset
     */
    public static int storeTuple(DBPage dbPage, int pageOffset, List<ColumnInfo> colInfos, Tuple tuple) {

        if (colInfos.size() != tuple.getColumnCount()) {
            throw new IllegalArgumentException("Tuple has different arity than target schema.");
        }

        // Start writing data just past the NULL-flag bytes.
        int currOffset = pageOffset + getNullFlagsSize(colInfos.size());
        int iCol = 0;
        for (ColumnInfo colInfo : colInfos) {

            ColumnType colType = colInfo.getType();
            Object value = tuple.getColumnValue(iCol);
            int dataSize = 0;
            if (value == null) {
                setNullFlag(dbPage, pageOffset, iCol, true);
            } else {
                // Write in the data value.
                setNullFlag(dbPage, pageOffset, iCol, false);
                dataSize = writeNonNullValue(dbPage, currOffset, colType, value);
            }

            currOffset += dataSize;
            iCol++;
        }

        return currOffset;
    }

    /**
     * 给colIndex对应的nullFlag设置值
     * 
     * @see #setNullFlag(int, boolean)
     * @param dbPage 数据页
     * @param tupleStart tuple起始位置
     * @param colIndex 列
     * @param value 值
     */
    public static void setNullFlag(DBPage dbPage, int tupleStart, int colIndex, boolean value) {

        // 找到colIndex在null-flag中的对应byte
        int nullFlagOffset = tupleStart + (colIndex / 8);
        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);
        // 找到colIndex在null-flag中的对应bit
        int mask = 1 << (colIndex % 8);
        if (value) {
            nullFlag = nullFlag | mask;
        } else {
            nullFlag = nullFlag & ~mask;
        }
        dbPage.writeByte(nullFlagOffset, nullFlag);
    }

    /**
     * 给数据页的指定位置写入非空值
     * 
     * @param dbPage 数据页
     * @param offset 指定位置
     * @param colType 列类型
     * @param value 值
     * @return 写入的数据大小
     */
    public static int writeNonNullValue(DBPage dbPage, int offset, ColumnType colType, Object value) {
        return dbPage.writeObject(offset, colType, value);
    }

    /**
     * 申请空间
     * 
     * @param off 起始位置
     * @param len 长度
     */
    protected abstract void insertTupleDataRange(int off, int len);

    /**
     * 释放指定位置的空间
     * 
     * @param off 起始位置
     * @param len 长度
     */
    protected abstract void deleteTupleDataRange(int off, int len);
}

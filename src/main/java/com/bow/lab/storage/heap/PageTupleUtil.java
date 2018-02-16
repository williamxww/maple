package com.bow.lab.storage.heap;

import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FilePointer;

import java.util.List;

/**
 * PageTuple类方法太多，不宜阅读
 * 
 * @author vv
 * @since 2018/1/7.
 */
public class PageTupleUtil {

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
    public static Object getColumnValue(DBPage dbPage, int valueOffset, ColumnType colType) {
        Object value = null;
        switch (colType.getBaseType()) {

            case INTEGER:
                value = Integer.valueOf(dbPage.readInt(valueOffset));
                break;

            case SMALLINT:
                value = Short.valueOf(dbPage.readShort(valueOffset));
                break;

            case BIGINT:
                value = Long.valueOf(dbPage.readLong(valueOffset));
                break;

            case TINYINT:
                value = Byte.valueOf(dbPage.readByte(valueOffset));
                break;

            case FLOAT:
                value = Float.valueOf(dbPage.readFloat(valueOffset));
                break;

            case DOUBLE:
                value = Double.valueOf(dbPage.readDouble(valueOffset));
                break;

            case CHAR:
                value = dbPage.readFixedSizeString(valueOffset, colType.getLength());
                break;

            case VARCHAR:
                value = dbPage.readVarString65535(valueOffset);
                break;

            case FILE_POINTER:
                value = new FilePointer(dbPage.readUnsignedShort(valueOffset),
                        dbPage.readUnsignedShort(valueOffset + 2));
                break;

            default:
                throw new UnsupportedOperationException("Cannot currently store type " + colType.getBaseType());
        }
        return value;
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
     * 在nullFlag中，将colIndex对应的bit设置0/1, value=true时设置为1，反之
     *
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
}

package com.bow.maple.relations;


/**
 * This interface provides the operations that can be performed with a tuple.
 * In relational database theory, a tuple is an ordered set of attribute-value
 * pairs, but in this implementation the tuple's data and its schema are kept
 * completely separate.  This tuple interface simply provides an index-accessed
 * collection of values; the schema would be represented separately using the
 * {@link Schema} class.
 * <p>
 * There is a natural question that arises:  Where is the tuple data stored?
 * Some tuples may be straight out of a table file, and thus their data will be
 * backed by a buffer page that can be written back to the filesystem.  Other
 * tuples may exist entirely in memory, with no corresponding back-end storage.
 * <p>
 * SQL data types are mapped to/from Java data types as follows:
 * <ul>
 *   <li><tt>TINYINT</tt> - <tt>byte</tt> (8 bit signed integer)</li>
 *   <li><tt>SMALLINT</tt> - <tt>short</tt> (16 bit signed integer)</li>
 *   <li>
 *     <tt>INTEGER</tt> (<tt>INT</tt>) - <tt>int</tt> (32 bit signed
 *     integer)
 *   </li>
 *   <li><tt>BIGINT</tt> - <tt>long</tt> (64 bit signed integer)</li>
 *   <li><tt>CHAR</tt> and <tt>VARCHAR</tt> - <tt>java.lang.String</tt></li>
 *   <li><tt>NUMERIC</tt> - <tt>java.math.BigDecimal</tt></li>
 * </ul>
 *
 * @see Schema
 */
public interface Tuple {

    /**
     * Returns true if this tuple can be cached for an extended period of time
     * without going invalid.  Some tuple implementations hold their values in
     * memory, but others are backed by disk pages, and once the page goes out
     * of cache then the Tuple object will no longer be valid.  In cases where a
     * plan-node needs to hold on to a tuple for a long time (e.g. for sorting
     * or grouping), the plan node should make a copy of non-cacheable tuples.
     *
     * @return <tt>true</tt> if the tuple is cacheable for an extended period of
     *         time, or <tt>false</tt> if the tuple's data is disk-backed and
     *         must be duplicated if it is going to be cached.
     */
    boolean isCacheable();


    /**
     * 获取tuple的总列数
     * @return
     */
    int getColumnCount();


    /**
     * 判断指定列是否为NULL
     * @param colIndex
     * @return
     */
    boolean isNullValue(int colIndex);


    /**
     * 获取指定列的值
     * @param colIndex
     * @return
     */
    Object getColumnValue(int colIndex);


    /**
     * 设置colIndex列的值为value
     * @param colIndex 列号
     * @param value 值
     */
    void setColumnValue(int colIndex, Object value);
}

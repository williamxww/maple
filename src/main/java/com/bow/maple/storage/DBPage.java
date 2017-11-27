package com.bow.maple.storage;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.bow.maple.expressions.TypeConverter;

import com.bow.maple.relations.ColumnType;
import com.bow.maple.storage.writeahead.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a single page in a database file. The page's
 * (zero-based) index in the file, and whether the page has been changed in
 * memory, are tracked by the object.
 * <p>
 * Database pages do not provide any locking mechanisms to guard against
 * concurrent access. Locking must be managed at a level above what this class
 * provides.
 * <p>
 * The class provides methods to read and write a wide range of data types.
 * Multibyte values are stored in big-endian format, with the most significant
 * byte (MSB) stored at the lowest index, and the least significant byte (LSB)
 * stored at the highest index. (This is also the network byte order specified
 * by the Internet Protocol.)
 *
 * @see PageReader PageReader
 * @see PageWriter PageWriter
 *
 * @design (Donnie) It is very important that the page is marked dirty
 *         <em>before</em> any changes are made, because this is the point when
 *         the old version of the page data is copied before changes are made.
 *         Additionally, the page's data must not be manipulated separately from
 *         the methods provided by this class, or else the old version of the
 *         page won't be recorded properly.
 */
public class DBPage {

    private static Logger logger = LoggerFactory.getLogger(DBPage.class);

    private DBFile dbFile;

    /**
     * table file中的pageNo,第一页的pageNo为0
     */
    private int pageNo;

    /**
     * 此页被定的次数。此值大于0，则此页不能被从缓存中刷出，因为至少一个session在使用。
     */
    private int pinCount;

    /**
     * true表示此页在内存中被修改过
     */
    private boolean dirty;

    /**
     * 脏页中pageLSN存储最近修改数据的LSN。在此页被刷出之前，其对应的write-ahead log必须被写入，否则write-ahead
     * logging rule被违反。
     */
    private LogSequenceNumber pageLSN;

    /**
     * 此页的真实数据
     */
    private byte[] pageData;

    /**
     * 在修改数据前将数据备份于此，方便把数据更新记录到WAL日志。
     */
    private byte[] oldPageData;

    /**
     * 创建一个新的数据页。注意此时没有加载数据。
     * @param dbFile 包含此页的文件
     * @param pageNo 页号
     */
    public DBPage(DBFile dbFile, int pageNo) {
        if (dbFile == null){
            throw new NullPointerException("dbFile must not be null");
        }
        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be >= 0 (got " + pageNo + ")");
        }
        this.dbFile = dbFile;
        this.pageNo = pageNo;
        pinCount = 0;
        dirty = false;
        pageLSN = null;
        // Allocate the space for the page data.
        pageData = new byte[dbFile.getPageSize()];
        oldPageData = null;
    }

    public DBFile getDBFile() {
        return dbFile;
    }

    /**
     * 此页是否是databaseFile中的数据页
     * @param databaseFile 数据文件
     * @return true表示出自数据文件databaseFile
     */
    public boolean isFromDBFile(DBFile databaseFile) {
        return dbFile.equals(databaseFile);
    }

    public int getPageNo() {
        return pageNo;
    }

    public int getPageSize() {
        return pageData.length;
    }

    public void incPinCount() {
        pinCount++;
    }

    public void decPinCount() {
        if (pinCount <= 0) {
            throw new IllegalStateException("pinCount is not positive (value is " + pinCount + ")");
        }
        pinCount--;
    }

    public int getPinCount() {
        return pinCount;
    }

    public boolean isPinned() {
        return (pinCount > 0);
    }

    public byte[] getPageData() {
        return pageData;
    }

    /**
     * Returns the byte-array of the page's data at the last point when the page
     * became dirty, or <tt>null</tt> if the page is currently clean.
     *
     * @return a byte-array containing the last "clean" version of the page's
     *         data
     */
    public byte[] getOldPageData() {
        return oldPageData;
    }

    /**
     * 将新数据放到oldPageData里面去
     */
    public void syncOldPageData() {
        if (oldPageData == null){
            throw new IllegalStateException("No old page data to sync");
        }
        System.arraycopy(pageData, 0, oldPageData, 0, pageData.length);
    }

    /**
     * 是否被修改
     * @return true 表示此页被修改过
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * 将此页设置为dirty，表明被修改过。
     * @param dirty 是脏页
     */
    public void setDirty(boolean dirty) {
        if (!this.dirty && dirty) {
            // 以前是clean,本次要修改为dirty
            oldPageData = pageData.clone();
        } else if (this.dirty && !dirty) {
            // 以前是dirty,本次要修改为clean
            oldPageData = null;
            pageLSN = null;
        }
        this.dirty = dirty;
    }

    public LogSequenceNumber getPageLSN() {
        return pageLSN;
    }

    public void setPageLSN(LogSequenceNumber lsn) {
        pageLSN = lsn;
    }

    /**
     * 注销此页，清除其内部所有引用。此方法会被Buffer Manager从缓存中移除此页时使用。
     */
    public void invalidate() {
        dbFile = null;
        pageNo = -1;
        pageData = null;
        oldPageData = null;
    }

    /* ============================= */
    /* TYPED DATA ACCESS FUNCTIONS */
    /* ============================= */

    /**
     * 从pageData中的position开始读取len字节放入b中
     * @param position 从pageData的position开始读取
     * @param b 读入的目标数组
     * @param off 目标数组的偏移量
     * @param len 读入数据的长度
     */
    public void read(int position, byte[] b, int off, int len) {
        System.arraycopy(pageData, position, b, off, len);
    }

    /**
     * Read a sequence of bytes into the provided byte-array. The entire array
     * is filled from start to end.
     *
     * @param position the starting index within the page to start reading data
     *
     * @param b the destination buffer to save the data into
     */
    public void read(int position, byte[] b) {
        read(position, b, 0, b.length);
    }

    /**
     * Write a sequence of bytes from a byte-array into the page, starting with
     * the specified offset in the buffer, and writing the specified number of
     * bytes.
     *
     * @param position the starting index within the page to start writing data
     *
     * @param b the source buffer to read the data from
     *
     * @param off the starting offset to read data from the source buffer
     *
     * @param len the number of bytes to transfer from the source buffer
     */
    public void write(int position, byte[] b, int off, int len) {
        setDirty(true);
        System.arraycopy(b, off, pageData, position, len);
    }

    /**
     * Write a sequence of bytes from a byte-array into the page. The entire
     * contents of the array is written to the page.
     *
     * @param position the starting index within the page to start writing data
     *
     * @param b the source buffer to read the data from
     */
    public void write(int position, byte[] b) {
        // Use the version of write() with extra args.
        write(position, b, 0, b.length);
    }

    /**
     * Move the specified data region in the page.
     *
     * @param srcPosition The source offset to copy data from.
     * @param dstPosition The destination offset to copy data to.
     * @param length The number of bytes of data to move.
     */
    public void moveDataRange(int srcPosition, int dstPosition, int length) {
        setDirty(true);
        System.arraycopy(pageData, srcPosition, pageData, dstPosition, length);
    }

    /**
     * Write the specified value.
     *
     * @param position The starting position to write the value to.
     * @param length The number of bytes of data to set.
     * @param value The byte-value to write to the entire range.
     */
    public void setDataRange(int position, int length, byte value) {
        setDirty(true);
        for (int i = 0; i < length; i++)
            pageData[position + i] = value;
    }

    /**
     * Reads and returns a Boolean value from the specified position. The
     * Boolean value is encoded as a single byte; a zero value is interpreted as
     * <tt>false</tt>, and a nonzero value is interpreted as <tt>true</tt>.
     *
     * @param position the starting location in the page to start reading the
     *        value from
     *
     * @return the Boolean value
     */
    public boolean readBoolean(int position) {
        return (pageData[position] != 0);
    }

    /**
     * Writes a Boolean value to the specified position. The Boolean value is
     * encoded as a single byte; <tt>false</tt> is written as 0, and
     * <tt>true</tt> is written as 1.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the Boolean value
     */
    public void writeBoolean(int position, boolean value) {
        setDirty(true);
        pageData[position] = (byte) (value ? 1 : 0);
    }

    /**
     * Reads and returns a signed byte from the specified position.
     *
     * @param position the location in the page to read the value from
     *
     * @return the signed byte value
     */
    public byte readByte(int position) {
        return pageData[position];
    }

    /**
     * Writes a (signed or unsigned) byte to the specified position. The byte
     * value is specified as an integer for the sake of convenience
     * (specifically to avoid having to cast an argument to a byte value), but
     * the input is also truncated down to the low 8 bits.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the byte value
     */
    public void writeByte(int position, int value) {
        setDirty(true);
        pageData[position] = (byte) value;
    }

    /**
     * Reads and returns an unsigned byte from the specified position. The value
     * is returned as an <tt>int</tt> whose value will be between 0 and 255,
     * inclusive.
     *
     * @param position the location in the page to read the value from
     *
     * @return the unsigned byte value, as an integer
     */
    public int readUnsignedByte(int position) {
        return pageData[position] & 0xFF;
    }

    /**
     * Reads and returns an unsigned short from the specified position. The
     * value is returned as an <tt>int</tt> whose value will be between 0 and
     * 65535, inclusive.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the unsigned short value, as an integer
     */
    public int readUnsignedShort(int position) {
        int value = ((pageData[position++] & 0xFF) << 8) | ((pageData[position] & 0xFF));

        return value;
    }

    /**
     * Reads and returns a signed short from the specified position. The value
     * is returned as a <tt>short</tt>.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the signed short value
     */
    public short readShort(int position) {
        // Don't chop off high-order bits. When byte is cast to int, the sign
        // will be extended, so if original byte is negative, the resulting
        // int will be too.
        int value = ((pageData[position++]) << 8) | ((pageData[position] & 0xFF));

        return (short) value;
    }

    /**
     * Writes a (signed or unsigned) short to the specified position. The short
     * value is specified as an integer for the sake of convenience
     * (specifically to avoid having to cast an argument to a short value), but
     * the input is also truncated down to the low 16 bits.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the byte value
     */
    public void writeShort(int position, int value) {
        setDirty(true);

        pageData[position++] = (byte) (0xFF & (value >> 8));
        pageData[position] = (byte) (0xFF & value);
    }

    /**
     * Reads and returns a two-byte char value from the specified position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the char value
     */
    public char readChar(int position) {
        // NOTE: Exactly like readShort(), but result is cast to a different
        // type.

        // Don't chop off high-order bits. When byte is cast to int, the sign
        // will
        // be extended, so if original byte is negative, so will resulting int.
        int value = ((pageData[position++]) << 8) | ((pageData[position] & 0xFF));

        return (char) value;
    }

    /**
     * Writes a char to the specified position. The char value is specified as
     * an integer for the sake of convenience (specifically to avoid having to
     * cast an argument to a char value), but the input is also truncated down
     * to the low 16 bits.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the char value
     */
    public void writeChar(int position, int value) {
        // Implementation is identical to writeShort()...
        writeShort(position, value);
    }

    /**
     * Reads and returns a 4-byte unsigned integer value from the specified
     * position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the unsigned integer value, as a long
     */
    public long readUnsignedInt(int position) {
        long value = ((pageData[position++] & 0xFF) << 24) | ((pageData[position++] & 0xFF) << 16)
                | ((pageData[position++] & 0xFF) << 8) | ((pageData[position] & 0xFF));

        return value;
    }

    /**
     * Reads and returns a 4-byte integer value from the specified position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the signed int value
     */
    public int readInt(int position) {
        int value = ((pageData[position++] & 0xFF) << 24) | ((pageData[position++] & 0xFF) << 16)
                | ((pageData[position++] & 0xFF) << 8) | ((pageData[position] & 0xFF));

        return value;
    }

    /**
     * Writes a 4-byte integer to the specified position.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the 4-byte integer value
     */
    public void writeInt(int position, int value) {
        setDirty(true);

        pageData[position++] = (byte) (0xFF & (value >> 24));
        pageData[position++] = (byte) (0xFF & (value >> 16));
        pageData[position++] = (byte) (0xFF & (value >> 8));
        pageData[position] = (byte) (0xFF & value);
    }

    /**
     * Reads and returns an 8-byte long integer value from the specified
     * position.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return the signed long value
     */
    public long readLong(int position) {
        long value = ((long) (pageData[position++] & 0xFF) << 56) | ((long) (pageData[position++] & 0xFF) << 48)
                | ((long) (pageData[position++] & 0xFF) << 40) | ((long) (pageData[position++] & 0xFF) << 32)
                | ((long) (pageData[position++] & 0xFF) << 24) | ((long) (pageData[position++] & 0xFF) << 16)
                | ((long) (pageData[position++] & 0xFF) << 8) | ((long) (pageData[position] & 0xFF));

        return value;
    }

    /**
     * Writes an 8-byte long integer to the specified position.
     *
     * @param position the location in the page to write the value to
     *
     * @param value the 8-byte long integer value
     */
    public void writeLong(int position, long value) {
        setDirty(true);

        pageData[position++] = (byte) (0xFF & (value >> 56));
        pageData[position++] = (byte) (0xFF & (value >> 48));
        pageData[position++] = (byte) (0xFF & (value >> 40));
        pageData[position++] = (byte) (0xFF & (value >> 32));
        pageData[position++] = (byte) (0xFF & (value >> 24));
        pageData[position++] = (byte) (0xFF & (value >> 16));
        pageData[position++] = (byte) (0xFF & (value >> 8));
        pageData[position] = (byte) (0xFF & value);
    }

    public float readFloat(int position) {
        return Float.intBitsToFloat(readInt(position));
    }

    public void writeFloat(int position, float value) {
        writeInt(position, Float.floatToIntBits(value));
    }

    public double readDouble(int position) {
        return Double.longBitsToDouble(readLong(position));
    }

    public void writeDouble(int position, double value) {
        writeLong(position, Double.doubleToLongBits(value));
    }

    /**
     * This method reads and returns a variable-length string whose maximum
     * length is 255 bytes. The string is expected to be in US-ASCII encoding,
     * so multibyte characters are not supported.
     * <p>
     * The string's data format is expected to be a single unsigned byte
     * <em>b</em> specifying the string's length, followed by <em>b</em> more
     * bytes consisting of the string value itself.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return a string object containing the stored value, up to a maximum of
     *         255 characters in length
     */
    public String readVarString255(int position) {
        int len = readUnsignedByte(position++);

        String str = null;

        try {
            str = new String(pageData, position, len, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs. So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }

    /**
     * 在写字符串前先写length然后再value<br/>
     * This method stores a variable-length string whose maximum length is 255
     * bytes. The string is expected to be in US-ASCII encoding, so multibyte
     * characters are not supported.
     * <p>
     * The string is stored as a single unsigned byte <em>b</em> specifying the
     * string's length, followed by <em>b</em> more bytes consisting of the
     * string value itself.
     *
     * @param position the location in the page to start writing the value to
     *
     * @param value the string object containing the data to store
     *
     * @throws NullPointerException if <tt>value</tt> is <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than 255
     *         characters
     */
    public void writeVarString255(int position, String value) {
        byte[] bytes;

        try {
            bytes = value.getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs. So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (bytes.length > 255)
            throw new IllegalArgumentException("value must be 255 bytes or less");

        // These functions set the dirty flag.
        writeByte(position, bytes.length);
        write(position + 1, bytes);
    }

    /**
     * This method reads and returns a variable-length string whose maximum
     * length is 65535 bytes. The string is expected to be in US-ASCII encoding,
     * so multibyte characters are not supported.
     * <p>
     * The string's data format is expected to be a single unsigned short (two
     * bytes) <em>s</em> specifying the string's length, followed by <em>s</em>
     * more bytes consisting of the string value itself.
     *
     * @param position the location in the page to start reading the value from
     *
     * @return a string object containing the stored value, up to a maximum of
     *         65535 characters in length
     */
    public String readVarString65535(int position) {
        int len = readUnsignedShort(position);
        position += 2;

        String str = null;

        try {
            str = new String(pageData, position, len, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs. So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }

    /**
     * This method stores a variable-length string whose maximum length is 65535
     * bytes. The string is expected to be in US-ASCII encoding, so multibyte
     * characters are not supported.
     * <p>
     * The string is stored as a single unsigned short <em>s</em> specifying the
     * string's length, followed by <em>s</em> more bytes consisting of the
     * string value itself.
     *
     * @param position the location in the page to start writing the value to
     *
     * @param value the string object containing the data to store
     *
     * @throws NullPointerException if <tt>value</tt> is <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than 65535
     *         characters
     */
    public void writeVarString65535(int position, String value) {
        byte[] bytes;

        try {
            bytes = value.getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs. So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (bytes.length > 65535)
            throw new IllegalArgumentException("value must be 65535 bytes or less");

        // These functions set the dirty flag.
        writeShort(position, bytes.length);
        write(position + 2, bytes);
    }

    /**
     * This method reads and returns a string whose length is fixed at a
     * constant size. The string is expected to be in US-ASCII encoding, so
     * multibyte characters are not supported.
     * <p>
     * Strings shorter than the specified length are padded with 0 bytes at the
     * end of the string, and this padding is removed when the string is read.
     *
     *
     * The string's characters are stored starting with the specified position.
     * If the string is shorter than the fixed length then the data is expected
     * to be terminated with a <tt>\\u0000</tt> (i.e. <tt>NUL</tt>) value. (If
     * the string is exactly the given length then no string terminator is
     * expected.) <b>The implication of this storage format is that embedded
     * <tt>NUL</tt> characters are not allowed with this storage format.</b>
     *
     * @param position the location in the page to start reading the value from
     *
     * @param len the length of the fixed-size string
     *
     * @return a string object containing the stored value, up to a maximum of
     *         <tt>len</tt> characters in length
     */
    public String readFixedSizeString(int position, int len) {
        String str = null;

        // Fixed-size strings are padded with 0-bytes, so trim these off the
        // end of the string value.
        while (len > 0 && pageData[position + len - 1] == 0)
            len--;

        try {
            str = new String(pageData, position, len, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs. So, this is not supposed
            // to happen.
            logger.error("The unthinkable has happened:  " + e);
        }

        return str;
    }

    /**
     * This method stores a string whose length is fixed at a constant size. The
     * string is expected to be in US-ASCII encoding, so multibyte characters
     * are not supported.
     * <p>
     * The string's characters are stored starting with the specified position.
     * If the string is shorter than the fixed length then the data is padded
     * with <tt>\\u0000</tt> (i.e. <tt>NUL</tt>) values. If the string is
     * exactly the given length then no string terminator is stored. <b>The
     * implication of this storage format is that embedded <tt>NUL</tt>
     * characters are not allowed with this storage format.</b>
     *
     * @param position the location in the page to start writing the value to
     *
     * @param value the string object containing the data to store
     *
     * @param len the number of bytes used to store the string field
     *
     * @throws NullPointerException if <tt>value</tt> is <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than
     *         <tt>len</tt> characters
     */
    public void writeFixedSizeString(int position, String value, int len) {
        byte[] bytes;

        try {
            bytes = value.getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // According to the Java docs, the US-ASCII character-encoding is
            // required to be supported by all JVMs. So, this is not supposed
            // happen.
            logger.error("The unthinkable has happened!", e);
            throw new RuntimeException("The unthinkable has happened!", e);
        }

        if (bytes.length > len) {
            throw new IllegalArgumentException("value must be " + len + " bytes or less");
        }

        // This function sets the dirty flag.
        write(position, bytes);

        // Zero out the rest of the fixed-size string value.
        Arrays.fill(pageData, position + bytes.length, position + len, (byte) 0);
    }

    /**
     * This method provides a higher-level wrapper around the other methods in
     * the <tt>DBPage</tt> class, allowing object-values to be read, as long as
     * the data-type is provided along with the object.
     *
     * @param position the location in the page to start reading the value from
     *
     * @param colType the type of the value being read
     *
     * @return the object read from the data
     *
     * @throws NullPointerException if <tt>colType</tt> or <tt>value</tt> is
     *         <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than
     *         <tt>len</tt> characters
     */
    public Object readObject(int position, ColumnType colType) {
        Object value = null;

        switch (colType.getBaseType()) {

            case INTEGER:
                value = Integer.valueOf(readInt(position));
                break;

            case SMALLINT:
                value = Short.valueOf(readShort(position));
                break;

            case BIGINT:
                value = Long.valueOf(readLong(position));
                break;

            case TINYINT:
                value = Byte.valueOf(readByte(position));
                break;

            case FLOAT:
                value = Float.valueOf(readFloat(position));
                break;

            case DOUBLE:
                value = Double.valueOf(readDouble(position));
                break;

            case CHAR:
                value = readFixedSizeString(position, colType.getLength());
                break;

            case VARCHAR:
                value = readVarString65535(position);
                break;

            case FILE_POINTER:
                value = new FilePointer(readUnsignedShort(position), readUnsignedShort(position + 2));
                break;

            default:
                throw new UnsupportedOperationException("Cannot currently read type " + colType.getBaseType());
        }

        return value;
    }

    /**
     * This method provides a higher-level wrapper around the other methods in
     * the <tt>DBPage</tt> class, allowing object-values to be stored, as long
     * as the object isn't <tt>null</tt> and the data-type is provided along
     * with the object.
     *
     * @param position the location in the page to start writing the value to
     *
     * @param colType the type of the value being stored
     *
     * @param value the object containing the data to store
     *
     * @return the total number of bytes written in the operation; i.e. this is
     *         the amount that the position is advanced.
     *
     * @throws NullPointerException if <tt>colType</tt> or <tt>value</tt> is
     *         <tt>null</tt>
     *
     * @throws IllegalArgumentException if the input string is longer than
     *         <tt>len</tt> characters
     */
    public int writeObject(int position, ColumnType colType, Object value) {

        if (colType == null)
            throw new NullPointerException("colType cannot be null");

        if (value == null)
            throw new NullPointerException("value cannot be null");

        int dataSize;

        // This code relies on Java autoboxing. Go, syntactic sugar.
        switch (colType.getBaseType()) {

            case INTEGER: {
                int iVal = TypeConverter.getIntegerValue(value);
                writeInt(position, iVal);
                dataSize = 4;
                break;
            }

            case SMALLINT: {
                short sVal = TypeConverter.getShortValue(value);
                writeShort(position, sVal);
                dataSize = 2;
                break;
            }

            case BIGINT: {
                long lVal = TypeConverter.getLongValue(value);
                writeLong(position, lVal);
                dataSize = 8;
                break;
            }

            case TINYINT: {
                byte bVal = TypeConverter.getByteValue(value);
                writeByte(position, bVal);
                dataSize = 1;
                break;
            }

            case FLOAT: {
                float fVal = TypeConverter.getFloatValue(value);
                writeFloat(position, fVal);
                dataSize = 4;
                break;
            }

            case DOUBLE: {
                double dVal = TypeConverter.getDoubleValue(value);
                writeDouble(position, dVal);
                dataSize = 8;
                break;
            }

            case CHAR: {
                String strVal = TypeConverter.getStringValue(value);
                writeFixedSizeString(position, strVal, colType.getLength());
                dataSize = colType.getLength();
                break;
            }

            case VARCHAR: {
                String strVal = TypeConverter.getStringValue(value);
                writeVarString65535(position, strVal);
                dataSize = 2 + strVal.length();
                break;
            }

            case FILE_POINTER: {
                FilePointer fptr = (FilePointer) value;
                writeShort(position, fptr.getPageNo());
                writeShort(position + 2, fptr.getOffset());
                dataSize = 4;
                break;
            }

            default:
                throw new UnsupportedOperationException("Cannot currently store type " + colType.getBaseType());
        }

        return dataSize;
    }

    /** Returns an objects size on disk. */
    public static int getObjectDiskSize(Object value, ColumnType colType) {
        if (colType == null)
            throw new NullPointerException("colType cannot be null");

        if (value == null)
            throw new NullPointerException("value cannot be null");

        int dataSize;

        // This code relies on Java autoboxing. Go, syntactic sugar.
        switch (colType.getBaseType()) {

            case INTEGER: {
                dataSize = 4;
                break;
            }

            case SMALLINT: {
                dataSize = 2;
                break;
            }

            case BIGINT: {
                dataSize = 8;
                break;
            }

            case TINYINT: {
                dataSize = 1;
                break;
            }

            case FLOAT: {
                dataSize = 4;
                break;
            }

            case DOUBLE: {
                dataSize = 8;
                break;
            }

            case CHAR: {
                dataSize = colType.getLength();
                break;
            }

            case VARCHAR: {
                String strVal = TypeConverter.getStringValue(value);
                dataSize = 2 + strVal.length();
                break;
            }

            case FILE_POINTER: {
                dataSize = 4;
                break;
            }

            default:
                throw new UnsupportedOperationException("Cannot currently store type " + colType.getBaseType());
        }

        return dataSize;
    }

    public String toFormattedString() {
        StringBuilder buf = new StringBuilder();

        int pageSize = dbFile.getPageSize();
        buf.append(String.format("DBPage[file=%s, pageNo=%d, pageSize=%d", dbFile, pageNo, pageSize));

        buf.append("\npageData =");
        for (int i = 0; i < pageSize; i++) {
            if (i % 32 == 0)
                buf.append("\n                ");

            buf.append(String.format(" %02X", pageData[i]));
        }

        buf.append("\noldPageData =");
        for (int i = 0; i < pageSize; i++) {
            if (i % 32 == 0)
                buf.append("\n                ");

            buf.append(String.format(" %02x", oldPageData[i]));
        }

        buf.append("\n]");

        return buf.toString();
    }
}

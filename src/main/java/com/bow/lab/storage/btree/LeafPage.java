package com.bow.lab.storage.btree;

import java.util.ArrayList;
import java.util.List;

import com.bow.maple.expressions.TupleComparator;
import com.bow.maple.expressions.TupleLiteral;
import com.bow.maple.indexes.IndexFileInfo;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.btreeindex.BTreeIndexManager;
import com.bow.maple.storage.btreeindex.BTreeIndexPageTuple;
import com.bow.maple.storage.btreeindex.InnerPage;
import com.bow.maple.storage.btreeindex.InnerPageOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This class wraps a {@link DBPage} object that is a leaf page in the B
 * <sup>+</sup> tree implementation, to provide some of the basic
 * leaf-management operations necessary for the indexing structure.
 * </p>
 * <p>
 * Operations involving individual inner-pages are provided by the
 * {@link InnerPage} wrapper-class. Higher-level operations involving multiple
 * leaves and/or inner pages of the B<sup>+</sup> tree structure, are provided
 * by the {@link LeafPageOperations} and {@link InnerPageOperations} classes.
 * </p>
 */
public class LeafPage {

    private static Logger logger = LoggerFactory.getLogger(LeafPage.class);

    /**
     * page的第一个字节放置page type
     */
    public static final int OFFSET_PAGE_TYPE = 0;

    /**
     * 索引中只有最后一个叶子节点没有next sibling，next page被设置为0
     */
    public static final int OFFSET_NEXT_PAGE_NO = 1;

    /**
     * The offset where the number of key+pointer entries is stored in the page.
     */
    public static final int OFFSET_NUM_ENTRIES = 3;

    /**
     * The offset of the first key in the leaf page.
     */
    public static final int OFFSET_FIRST_KEY = 5;

    private DBPage dbPage;

    /**
     * Information about the index itself, such as what file it is stored in,
     * its name, the columns in the index, and so forth.
     */
    private IndexFileInfo idxFileInfo;

    /**
     * Since we require the schema of the index rather frequently, this is a
     * cached copy of the index's schema.
     */
    private List<ColumnInfo> colInfos;

    /** The number of entries (pointers + keys) stored within this leaf page. */
    private int numEntries;

    /**
     * A list of the keys stored in this leaf page. Each key also stores the
     * file-pointer for the associated tuple, as the last value in the key.
     */
    private List<BTreeIndexPageTuple> keys;

    /**
     * The total size of all data (pointers + keys + initial values) stored
     * within this leaf page. This is also the offset at which we can start
     * writing more data without overwriting anything.
     */
    private int endOffset;

    /**
     * Initialize the leaf-page wrapper class for the specified index page. The
     * contents of the leaf-page are cached in the fields of the wrapper object.
     *
     * @param dbPage 存储数据的页
     * @param idxFileInfo 索引的描述信息
     */
    public LeafPage(DBPage dbPage, IndexFileInfo idxFileInfo) {
        if (dbPage.readUnsignedByte(0) != BTreeIndexManager.BTREE_LEAF_PAGE) {
            throw new IllegalArgumentException(
                    "Specified DBPage " + dbPage.getPageNo() + " is not marked as a leaf-page.");
        }

        this.dbPage = dbPage;
        this.idxFileInfo = idxFileInfo;
        this.colInfos = idxFileInfo.getIndexSchema();

        loadPageContents();
    }

    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code LeafPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.
     *
     * @param dbPage the page to initialize as a leaf-page.
     *
     * @param idxFileInfo details about the index that the leaf-page is for
     *
     * @return a newly initialized {@code LeafPage} object wrapping the page
     */
    public static LeafPage init(DBPage dbPage, IndexFileInfo idxFileInfo) {
        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_LEAF_PAGE);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, 0);
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, 0);

        return new LeafPage(dbPage, idxFileInfo);
    }

    /**
     * This private helper scans through the leaf page's contents and caches the
     * contents of the leaf page in a way that makes it easy to use and
     * manipulate.
     */
    private void loadPageContents() {
        numEntries = dbPage.readUnsignedShort(OFFSET_NUM_ENTRIES);
        keys = new ArrayList<>(numEntries);
        if (numEntries > 0) {
            // Handle first key separately since we know its offset.
            BTreeIndexPageTuple key = new BTreeIndexPageTuple(dbPage, OFFSET_FIRST_KEY, colInfos);
            keys.add(key);
            // Handle remaining keys.
            for (int i = 1; i < numEntries; i++) {
                int keyEndOffset = key.getEndOffset();
                key = new BTreeIndexPageTuple(dbPage, keyEndOffset, colInfos);
                keys.add(key);
            }
            endOffset = key.getEndOffset();
        } else {
            // There are no entries (pointers + keys).
            endOffset = OFFSET_FIRST_KEY;
        }
    }

    /**
     * Returns the high-level details for the index that this page is a part of.
     *
     * @return the high-level details for the index
     */
    public IndexFileInfo getIndexFileInfo() {
        return idxFileInfo;
    }

    /**
     * Returns the page-number of this leaf page.
     *
     * @return the page-number of this leaf page.
     */
    public int getPageNo() {
        return dbPage.getPageNo();
    }

    /**
     * 获取下一页的pageNo,如果是最后一页了就返回0
     * 
     * @return 获取下一页的pageNo
     */
    public int getNextPageNo() {
        return dbPage.readUnsignedShort(OFFSET_NEXT_PAGE_NO);
    }

    public void setNextPageNo(int pageNo) {
        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be in range [0, 65535]; got " + pageNo);
        }

        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, pageNo);
    }

    /**
     * Returns the number of entries in this leaf-page. Note that this count
     * does not include the pointer to the next leaf; it only includes the keys
     * and associated pointers to tuples in the table-file.
     *
     * @return the number of entries in this leaf-page.
     */
    public int getNumEntries() {
        return numEntries;
    }

    /**
     * 剩余空间(in byte)
     * 
     * @return 剩余空间.
     */
    public int getFreeSpace() {
        return dbPage.getPageSize() - endOffset;
    }

    /**
     * Returns the key at the specified index.
     *
     * @param index the index of the key to retrieve
     *
     * @return the key at that index
     */
    public BTreeIndexPageTuple getKey(int index) {
        return keys.get(index);
    }

    /**
     * Returns the size of the key at the specified index, in bytes.
     * @param index the index of the key to get the size of
     * @return the size of the specified key, in bytes
     */
    public int getKeySize(int index) {
        BTreeIndexPageTuple key = getKey(index);
        return key.getEndOffset() - key.getOffset();
    }

    /**
     * This method inserts a key into the leaf page, making sure to keep keys in
     * monotonically increasing order. This method will throw an exception if
     * the leaf page already contains the specified key; this is acceptable
     * because keys include a "uniquifier" value, the file-pointer to the actual
     * tuple that the key is associated with. Thus, if a key appears multiple
     * times in a leaf-page, the index would actually be invalid.
     *
     * @param newKey the new key to add to the leaf page
     * 
     * @throws IllegalStateException if the specified key already appears in the
     *         leaf page.
     */
    public void addEntry(TupleLiteral newKey) {
        if (newKey.getStorageSize() == -1) {
            throw new IllegalArgumentException(
                    "New key's storage size must " + "be computed before this method is called.");
        }

        if (getFreeSpace() < newKey.getStorageSize()) {
            throw new IllegalArgumentException(String.format(
                    "Not enough space in this node to store the new key " + "(%d bytes free; %d bytes required)",
                    getFreeSpace(), newKey.getStorageSize()));
        }

        if (numEntries == 0) {
            logger.debug("Leaf page is empty; storing new entry at start.");
            addEntryAtIndex(newKey, 0);
        } else {
            int i;
            for (i = 0; i < numEntries; i++) {
                BTreeIndexPageTuple key = keys.get(i);
                logger.trace(i + ":  comparing " + newKey + " to " + key);

                // Compare the tuple to the current key. Once we find where the
                // new key/tuple should go, copy the key/pointer into the page.
                int cmp = TupleComparator.compareTuples(newKey, key);
                if (cmp < 0) {
                    logger.debug("Storing new entry at index " + i + " in the leaf page.");
                    addEntryAtIndex(newKey, i);
                    break;
                } else if (cmp == 0) {
                    // This should NEVER happen! Remember that every key has
                    // a uniquifier associated with it - the actual location of
                    // the associated tuple - so this should be an error.
                    throw new IllegalStateException("Key " + newKey + " already appears in the index!");
                }
            }

            if (i == numEntries) {
                // The new tuple will go at the end of this page's entries.
                logger.debug("Storing new entry at end of leaf page.");
                addEntryAtIndex(newKey, numEntries);
            }
        }

        // The addEntryAtIndex() method updates the internal fields that cache
        // where keys live, etc. So, we don't need to do that here.
    }

    /**
     * 将newKey插入到指定位置index
     * @param newKey the new key to insert into the leaf page
     * @param index 第index个tuple处。
     */
    private void addEntryAtIndex(TupleLiteral newKey, int index) {
        logger.debug("Leaf-page is starting with data ending at index " + endOffset + ", and has " + numEntries
                + " entries.");

        // Get the length of the new tuple, and add in the size of the
        // file-pointer as well.
        int len = newKey.getStorageSize();
        if (len == -1) {
            throw new IllegalArgumentException(
                    "New key's storage size must " + "be computed before this method is called.");
        }
        logger.debug("New key's storage size is " + len + " bytes");
        int keyOffset;
        if (index < numEntries) {
            //找到Index对应的key并开辟足够的空间
            BTreeIndexPageTuple key = getKey(index);
            keyOffset = key.getOffset();
            logger.debug(
                    "Moving leaf-page data in range [" + keyOffset + ", " + endOffset + ") over by " + len + " bytes");
            dbPage.moveDataRange(keyOffset, keyOffset + len, endOffset - keyOffset);
        } else {
            // The new key falls at the end of the data in the page.
            keyOffset = endOffset;
            logger.debug("New key is at end of leaf-page data; not moving anything.");
        }
        // Write the key and its associated file-pointer value into the page.
        PageTuple.storeTuple(dbPage, keyOffset, colInfos, newKey);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries + 1);
        // Reload the page contents now that we have a new key in the mix.
        loadPageContents();
        logger.debug("Wrote new key to leaf-page at offset " + keyOffset + ".");
        logger.debug(
                "Leaf-page is ending with data ending at index " + endOffset + ", and has " + numEntries + " entries.");
    }

    /**
     * 将此页的前count个entry移动到左页
     * @param leftSibling the left sibling of this leaf-node in the index file
     * @param count 前count个entry
     */
    public void moveEntriesLeft(LeafPage leftSibling, int count) {
        if (leftSibling == null)
            throw new IllegalArgumentException("leftSibling cannot be null");

        if (leftSibling.getNextPageNo() != getPageNo()) {
            throw new IllegalArgumentException("leftSibling " + leftSibling.getPageNo() + " isn't actually the left "
                    + "sibling of this leaf-node " + getPageNo());
        }
        if (count < 0 || count > numEntries) {
            throw new IllegalArgumentException("count must be in range [0, " + numEntries + "), got " + count);
        }

        int moveEndOffset = getKey(count).getOffset();
        int len = moveEndOffset - OFFSET_FIRST_KEY;

        //将本页的len字节数据复制到左边的叶子的后面，然后更新左页的总entry数量
        leftSibling.dbPage.write(leftSibling.endOffset, dbPage.getPageData(), OFFSET_FIRST_KEY, len);
        leftSibling.dbPage.writeShort(OFFSET_NUM_ENTRIES, leftSibling.numEntries + count);

        // 本页释放已移走数据的空间（把后面的数据移动到前面）
        dbPage.moveDataRange(moveEndOffset, OFFSET_FIRST_KEY, endOffset - moveEndOffset);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries - count);

        if (BTreeIndexManager.CLEAR_OLD_DATA){
            //后面数据置为0
            dbPage.setDataRange(endOffset - len, len, (byte) 0);
        }

        // Update the cached info for both leaves.
        loadPageContents();
        leftSibling.loadPageContents();
    }

    /**
     * 把本页后count个entry移动到右页
     * @param rightSibling the right sibling of this leaf-node in the index file
     * @param count 后count个entry
     */
    public void moveEntriesRight(LeafPage rightSibling, int count) {
        if (rightSibling == null){
            throw new IllegalArgumentException("rightSibling cannot be null");
        }
        if (getNextPageNo() != rightSibling.getPageNo()) {
            throw new IllegalArgumentException("rightSibling " + rightSibling.getPageNo() + " isn't actually the right "
                    + "sibling of this leaf-node " + getPageNo());
        }
        if (count < 0 || count > numEntries) {
            throw new IllegalArgumentException("count must be in range [0, " + numEntries + "), got " + count);
        }

        int startOffset = getKey(numEntries - count).getOffset();
        int len = endOffset - startOffset;

        // 右页腾出空间
        rightSibling.dbPage.moveDataRange(OFFSET_FIRST_KEY, OFFSET_FIRST_KEY + len,
                rightSibling.endOffset - OFFSET_FIRST_KEY);
        // 把左页的数据copy过来
        rightSibling.dbPage.write(OFFSET_FIRST_KEY, dbPage.getPageData(), startOffset, len);
        // 更新右页的entry-count
        rightSibling.dbPage.writeShort(OFFSET_NUM_ENTRIES, rightSibling.numEntries + count);

        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries - count);
        // 移除本页的相关数据
        if (BTreeIndexManager.CLEAR_OLD_DATA){
            dbPage.setDataRange(startOffset, len, (byte) 0);
        }
        //重新加载数据
        loadPageContents();
        rightSibling.loadPageContents();
    }
}

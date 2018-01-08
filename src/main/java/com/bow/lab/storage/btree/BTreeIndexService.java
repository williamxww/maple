package com.bow.lab.storage.btree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bow.lab.storage.IIndexService;
import com.bow.lab.storage.heap.PageTupleUtil;
import com.bow.maple.util.ExtensionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bow.lab.storage.IStorageService;
import com.bow.maple.expressions.TupleComparator;
import com.bow.maple.expressions.TupleLiteral;
import com.bow.maple.indexes.IndexFileInfo;
import com.bow.maple.indexes.IndexInfo;
import com.bow.maple.relations.ColumnIndexes;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.TableConstraintType;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.btreeindex.BTreeIndexPageTuple;
import com.bow.maple.storage.btreeindex.BTreeIndexVerifier;
import com.bow.maple.storage.btreeindex.HeaderPage;

/**
 * <p>
 * This is the class that manages B<sup>+</sup> tree indexes. These indexes are
 * used for enforcing primary, candidate and foreign keys, and also for
 * providing optimized access to tuples with specific values.
 * </p>
 * <p>
 * Here is a brief overview of the NanoDB B<sup>+</sup> tree file format:
 * </p>
 * <ul>
 * <li>Page 0 is always a header page, and specifies the entry-points in the
 * hierarchy: the root page of the tree, and the first and last leaves of the
 * tree. Page 0 also maintains a list of empty pages in the tree, so that adding
 * new nodes to the tree is fast. (See the {@link HeaderPage} class for
 * details.)</li>
 * <li>The remaining pages are either leaf nodes, inner nodes, or empty nodes.
 * The first byte of the page always indicates the kind of node. For details
 * about the internal structure of leaf and inner nodes, see the
 * {@link InnerPage} and {@link LeafPage} classes.</li>
 * <li>Empty nodes are formed into a simple singly linked list. Each empty node
 * holds a page-pointer to the next empty node in the sequence, using an
 * unsigned short stored at index 1 (after the page-type value in index 0). The
 * final empty page stores 0 as its next-page pointer value.</li>
 * </ul>
 * <p>
 * This index implementation always adds a uniquifier to tuples being stored in
 * the index; specifically, the file-pointer of the tuple being stored into the
 * index. This file-pointer also allows the referenced tuple to be retrieved
 * from the table via the index when needed. The tuple's file-pointer is always
 * appended to the key-value being stored, so the last column is always the
 * file-pointer to the tuple.
 * </p>
 */
public class BTreeIndexService implements IIndexService {

    private static Logger logger = LoggerFactory.getLogger(BTreeIndexService.class);

    public static final int BTREE_INNER_PAGE = 1;

    public static final int BTREE_LEAF_PAGE = 2;

    public static final int BTREE_EMPTY_PAGE = 3;

    /**
     * 旧数据会立马被清除
     */
    public static final boolean CLEAR_OLD_DATA = true;

    private IStorageService storageService = ExtensionLoader.getExtensionLoader(IStorageService.class).getExtension();

    private LeafPageOperations leafPageOps;

    private InnerPageOperations innerPageOps;

    /**
     * Initializes the heap-file table manager. This class shouldn't be
     * initialized directly, since the storage manager will initialize it when
     * necessary.
     * @throws IllegalArgumentException if <tt>storageService</tt> is
     *         <tt>null</tt>
     */
    public BTreeIndexService() {
        innerPageOps = new InnerPageOperations(this);
        leafPageOps = new LeafPageOperations(this, innerPageOps);
    }

    /**
     * 索引没有指定名字，为其生成一个前缀
     * 
     * @param idxFileInfo 索引信息
     * @return 名称前缀
     */
    @Override
    public String getUnnamedIndexPrefix(IndexFileInfo idxFileInfo) {
        // Generate a prefix based on the contents of the IndexFileInfo object.
        IndexInfo info = idxFileInfo.getIndexInfo();
        TableConstraintType constraintType = info.getConstraintType();

        if (constraintType == null)
            return "IDX_" + idxFileInfo.getTableName();

        switch (info.getConstraintType()) {
            case PRIMARY_KEY:
                return "PK_" + idxFileInfo.getTableName();

            case UNIQUE:
                return "CK_" + idxFileInfo.getTableName();

            default:
                throw new IllegalArgumentException("Unrecognized constraint type " + constraintType);
        }
    }

    /**
     * 初始化索引文件,索引文件的首页初始化
     * 
     * @param idxFileInfo 索引文件信息
     * @throws IOException e
     */
    @Override
    public void initIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        String indexName = idxFileInfo.getIndexName();
        String tableName = idxFileInfo.getTableName();
        DBFile dbFile = idxFileInfo.getDBFile();
        logger.info("Initializing new index {} on table {}, stored at {}", indexName, tableName, dbFile);

        // The index's header page just stores details of the indexing structure
        // itself, since the the actual schema information and other index
        // details are stored in the referenced table.
        DBPage headerPage = storageService.loadDBPage(dbFile, 0);
        HeaderPage.setRootPageNo(headerPage, 0);
        HeaderPage.setFirstLeafPageNo(headerPage, 0);
        HeaderPage.setFirstEmptyPageNo(headerPage, 0);
    }

    /**
     * This method reads in the schema and other critical information for the
     * specified table.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         table's schema and other details.
     */
    public void loadIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        // For now, we don't need to do anything in this method.
    }

    /**
     *
     * @param idxFileInfo the index to add the tuple to
     * @param tup 需要加索引的tuple
     * @throws IOException e
     */
    @Override
    public void addTuple(IndexFileInfo idxFileInfo, PageTuple tup) throws IOException {

        // 索引值 (key,file-pointer)
        TupleLiteral newTupleKey = makeStoredKeyValue(idxFileInfo, tup);
        logger.debug("Adding search-key value " + newTupleKey + " to index " + idxFileInfo.getIndexName());
        // Navigate to the leaf-page, creating one if the index is currently
        // empty.
        List<Integer> pagePath = new ArrayList<>();
        LeafPage leaf = navigateToLeafPage(idxFileInfo, newTupleKey, true, pagePath);

        leafPageOps.addEntry(leaf, newTupleKey, pagePath);
    }

    @Override
    public void deleteTuple(IndexFileInfo idxFileInfo, PageTuple tup) throws IOException {
        // TODO: IMPLEMENT
    }

    @Override
    public List<String> verifyIndex(IndexFileInfo idxFileInfo) throws IOException {
        BTreeIndexVerifier verifier = new BTreeIndexVerifier(idxFileInfo);
        List<String> errors = verifier.verify();

        return errors;
    }

    /**
     * 将searchKey放到索引文件的对应叶子节点里。
     *
     * @param idxFileInfo details of the index that is being navigated
     * @param searchKey 遍历b+树要查找的key
     * @param createIfNeeded false时，若发现页不存在则不管了
     * @param pagePath 记录了从根到叶子节点的路径
     * @return searchKey所在的叶子节点
     * @throws IOException e
     */
    private LeafPage navigateToLeafPage(IndexFileInfo idxFileInfo, TupleLiteral searchKey, boolean createIfNeeded,
            List<Integer> pagePath) throws IOException {

        String indexName = idxFileInfo.getIndexName();
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbpHeader = storageService.loadDBPage(dbFile, 0);

        // 从根开始找searchKey应该位于哪个叶子节点
        DBPage dbPage = getRootPage(idxFileInfo, createIfNeeded);
        if (!createIfNeeded && dbPage == null) {
            return null;
        }
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        int pageType = dbPage.readByte(0);
        if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE) {
            throw new IOException("Invalid page type encountered:  " + pageType);
        }
        if (pagePath != null) {
            pagePath.add(rootPageNo);
        }

        // 页内部按序找，找到比searchKey大的key则调到对应页，直到找到叶子节点。记录经过的所有节点。
        while (pageType != BTREE_LEAF_PAGE) {
            logger.debug("Examining non-leaf page " + dbPage.getPageNo() + " of index " + indexName);
            int nextPageNo = -1;
            InnerPage innerPage = new InnerPage(dbPage, idxFileInfo);
            int numKeys = innerPage.getNumKeys();
            if (numKeys < 1) {
                throw new IllegalStateException(
                        "Non-leaf page " + dbPage.getPageNo() + " is invalid:  it contains no keys!");
            }
            for (int i = 0; i < numKeys; i++) {
                // 遍历innerPage中每个key找到比searchKey大的key
                BTreeIndexPageTuple key = innerPage.getKey(i);
                int cmp = TupleComparator.comparePartialTuples(searchKey, key);
                if (cmp < 0) {
                    logger.debug(
                            "Value is less than key at index " + i + "; following pointer " + i + " before this key.");
                    // |p0|k0|p1|k1|p2|k2|p3|k3|p4|
                    nextPageNo = innerPage.getPointer(i);
                    break;
                } else if (cmp == 0) {
                    logger.debug("Value is equal to key at index " + i + "; following pointer " + (i + 1)
                            + " after this key.");
                    nextPageNo = innerPage.getPointer(i + 1);
                    break;
                }
            }

            if (nextPageNo == -1) {
                // 若searchKey>此页中所有key,那么此innerPage的最后一个pageNo就是下一页
                logger.debug("Value is greater than all keys in this page;" + " following last pointer " + numKeys
                        + " in the page.");
                nextPageNo = innerPage.getPointer(numKeys);
            }

            // 加在下一页并记录path
            dbPage = storageService.loadDBPage(dbFile, nextPageNo);
            pageType = dbPage.readByte(0);
            if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE) {
                throw new IOException("Invalid page type encountered:  " + pageType);
            }
            if (pagePath != null) {
                pagePath.add(nextPageNo);
            }
        }
        return new LeafPage(dbPage, idxFileInfo);
    }

    private DBPage getRootPage(IndexFileInfo idxFileInfo, boolean createIfNeeded) throws IOException {
        String indexName = idxFileInfo.getIndexName();

        // 根据首页可以知道root page的起始位置
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbpHeader = storageService.loadDBPage(dbFile, 0);

        // Get the root page of the index.
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        DBPage dbpRoot;
        if (rootPageNo == 0) {
            // 还没有数据页
            if (!createIfNeeded) {
                return null;
            }
            logger.debug("Index " + indexName + " currently has no data "
                    + "pages; finding/creating one to use as the root!");
            // 创建根数据页
            dbpRoot = getNewDataPage(dbFile);
            rootPageNo = dbpRoot.getPageNo();
            HeaderPage.setRootPageNo(dbpHeader, rootPageNo);
            HeaderPage.setFirstLeafPageNo(dbpHeader, rootPageNo);
            dbpRoot.writeByte(0, BTREE_LEAF_PAGE);
            LeafPage.init(dbpRoot, idxFileInfo);
            logger.debug("New root pageNo is " + rootPageNo);
        } else {
            // 索引已有rootPage了，加载它
            dbpRoot = storageService.loadDBPage(dbFile, rootPageNo);
            logger.debug("Index " + idxFileInfo.getIndexName() + " root pageNo is " + rootPageNo);
        }
        return dbpRoot;
    }

    /**
     * 获取新的数据页，优先从空页列表中获取，没有时重新创建。
     * 
     * @param dbFile the index file to get a new empty data page from
     * @return an empty {@code DBPage} that can be used as a new index page.
     * @throws IOException e
     */
    public DBPage getNewDataPage(DBFile dbFile) throws IOException {
        if (dbFile == null) {
            throw new IllegalArgumentException("dbFile cannot be null");
        }
        DBPage dbpHeader = storageService.loadDBPage(dbFile, 0);
        DBPage newPage;
        int pageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);
        if (pageNo == 0) {
            // 没有空页，就创建一个
            logger.debug("No empty pages.  Extending index file " + dbFile + " by one page.");
            int numPages = dbFile.getNumPages();
            newPage = storageService.loadDBPage(dbFile, numPages, true);
        } else {
            // 有空页就用，并将其从空页列表删除
            logger.debug("First empty page number is " + pageNo);
            newPage = storageService.loadDBPage(dbFile, pageNo);
            int nextEmptyPage = newPage.readUnsignedShort(1);
            HeaderPage.setFirstEmptyPageNo(dbpHeader, nextEmptyPage);
        }
        logger.debug("Found new data page for the index:  page " + newPage.getPageNo());
        // TODO: Increment the number of data pages?
        return newPage;
    }

    /**
     * 清除此页数据，将此页设置为空页列表的第一个。
     * 
     * @param dbPage the data-page that is no longer used.
     * @throws IOException e
     */
    public void releaseDataPage(DBPage dbPage) throws IOException {
        // TODO: 如果此页是indexFile的最后一页，我们可以直接删除
        DBFile dbFile = dbPage.getDBFile();
        // 将页类型改为空页
        dbPage.writeByte(0, BTREE_EMPTY_PAGE);
        DBPage dbpHeader = storageService.loadDBPage(dbFile, 0);
        // 取出当前第一个空页的页号，设置到此页上
        int prevEmptyPageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);
        dbPage.writeShort(1, prevEmptyPageNo);
        if (CLEAR_OLD_DATA) {
            // 清除所有数据
            dbPage.setDataRange(3, dbPage.getPageSize() - 3, (byte) 0);
        }
        // 将此页设置为第一个空页
        HeaderPage.setFirstEmptyPageNo(dbpHeader, dbPage.getPageNo());
    }

    /**
     * 从ptup中找出索引值和ptup的指针(pageNo,offset)，合并成一个newKeyVal
     *
     * @param idxFileInfo the details of the index to create the key for
     * @param ptup 原表中的tuple
     * @return 包含索引列值和ptup指针的newKeyVal
     */
    private TupleLiteral makeStoredKeyValue(IndexFileInfo idxFileInfo, PageTuple ptup) {
        // 找出索引列
        ColumnIndexes colIndexes = idxFileInfo.getTableColumnIndexes();
        // 从tuple中找出索引列的值放到newKeyVal
        TupleLiteral newKeyVal = new TupleLiteral();
        for (int i = 0; i < colIndexes.size(); i++) {
            newKeyVal.addValue(ptup.getColumnValue(colIndexes.getCol(i)));
        }
        // ptup的指针(pageNo,offset)放到最后
        newKeyVal.addValue(ptup.getExternalReference());
        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int storageSize = PageTupleUtil.getTupleStorageSize(colInfos, newKeyVal);
        newKeyVal.setStorageSize(storageSize);
        return newKeyVal;
    }
}

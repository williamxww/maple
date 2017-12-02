package com.bow.lab.storage.btree;

import java.io.IOException;
import java.util.List;

import com.bow.lab.storage.IStorageService;
import com.bow.maple.expressions.TupleLiteral;
import com.bow.maple.indexes.IndexFileInfo;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.StorageManager;
import com.bow.maple.storage.btreeindex.BTreeIndexPageTuple;
import com.bow.maple.storage.btreeindex.HeaderPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides high-level B<sup>+</sup> tree management operations
 * performed on inner nodes. These operations are provided here and not on the
 * {@link InnerPage} class since they sometimes involve splitting or merging
 * inner nodes, updating parent nodes, and so forth.
 */
public class InnerPageOperations {

    private static Logger logger = LoggerFactory.getLogger(InnerPageOperations.class);

    private IStorageService storageManager;

    private BTreeIndexService bTreeManager;

    public InnerPageOperations(BTreeIndexService bTreeManager) {
        this.bTreeManager = bTreeManager;
    }

    /**
     * 加在一个内部页
     * 
     * @param idxFileInfo 索引文件信息
     * @param pageNo 页
     * @return 数据页
     * @throws IOException e
     */
    public InnerPage loadPage(IndexFileInfo idxFileInfo, int pageNo) throws IOException {
        if (pageNo == 0){
            return null;
        }
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new InnerPage(dbPage, idxFileInfo);
    }

    /**
     * 在innerPage中根据(pagePtr1,pagePtr2)找到中间夹着的key，然后将其替换
     *
     * @param page the inner page to update the key in
     * @param pagePath the path to the page, from the root node
     * @param pagePtr1 the pointer P<sub>i</sub> before the key to update
     * @param key1 the new value of the key K<sub>i</sub> to store
     * @param pagePtr2 the pointer P<sub>i+1</sub> after the key to update
     *
     * @todo (Donnie) This implementation has a major failing that will occur
     *       infrequently - if the inner page doesn't have room for the new key
     *       (e.g. if the page was already almost full, and then the new key is
     *       larger than the old key) then the inner page needs to be split, per
     *       usual. Right now it will just throw an exception in this case. This
     *       is why the {@code pagePath} argument is provided, so that when this
     *       bug is fixed, the page-path will be available.
     */
    public void replaceKey(InnerPage page, List<Integer> pagePath, int pagePtr1, Tuple key1, int pagePtr2) {

        for (int i = 0; i < page.getNumPointers() - 1; i++) {
            if (page.getPointer(i) == pagePtr1 && page.getPointer(i + 1) == pagePtr2) {

                // Found the pair of pointers! Replace the key-value.
                BTreeIndexPageTuple oldKey = page.getKey(i);
                int oldKeySize = oldKey.getSize();

                int newKeySize = PageTuple.getTupleStorageSize(page.getIndexFileInfo().getIndexSchema(), key1);

                if (page.getFreeSpace() - oldKeySize + newKeySize >= 0) {
                    // We have room - go ahead and do this.
                    page.replaceKey(i, key1);
                } else {
                    // We need to split the inner page in this situation.
                    throw new UnsupportedOperationException("Can't replace key on inner page at index " + i
                            + ": out of space, and NanoDB doesn't know how to "
                            + " split inner pages in this situation yet.");
                }
                // Make sure we didn't cause any brain damage...
                assert page.getPointer(i) == pagePtr1;
                assert page.getPointer(i + 1) == pagePtr2;
                return;
            }
        }

        // 到这里了就说明在innerPage中找不到对应的(pagePtr1,key,pagePtr2)
        // 打印page内容
        logger.error("Couldn't find pair of pointers {} and {} in inner page {}!", pagePtr1, pagePtr2,
                page.getPageNo());
        logger.error("Page contents:\n" + page.toFormattedString());
        throw new IllegalStateException("Couldn't find sequence of page-pointers [" + pagePtr1 + ", " + pagePtr2
                + "] in non-leaf page " + page.getPageNo());
    }

    /**
     * 尝试着腾出bytesRequired bytes空间
     *
     * @param page the inner page to relocate entries from
     * @param adjPage 相邻页(前或后)
     * @param movingRight 朝左或右移动
     * @param bytesRequired 所需要的空间的大小
     * @param parentKeySize 在移动innerPage的entry时需要先将parentPage的对应key沉下来
     * @return 移动的entry数量
     */
    private int tryNonLeafRelocateForSpace(InnerPage page, InnerPage adjPage, boolean movingRight, int bytesRequired,
            int parentKeySize) {

        int numKeys = page.getNumKeys();
        int pageBytesFree = page.getFreeSpace();
        int adjBytesFree = adjPage.getFreeSpace();

        // parentKey是需要下沉到adjPage里的第一个key，如果空间不够就不用试了
        if (adjBytesFree < parentKeySize){
            return 0;
        }
        adjBytesFree -= parentKeySize;

        int keyBytesMoved = 0;
        int lastKeySize = parentKeySize;

        //移动的(key,page)总数量
        int numRelocated = 0;
        while (true) {
            // 此处2为pageNo的size
            if (adjBytesFree < keyBytesMoved + 2 * numRelocated) {
                //发现空间不够了就不移动了，把最后一次增加的numRelocated减去
                numRelocated--;
                break;
            }

            int index;
            if (movingRight){
                // 朝右移动就从page的最后一个entry<key,page>开始
                index = numKeys - numRelocated - 1;
            } else{
                index = numRelocated;
            }
            keyBytesMoved += lastKeySize;
            lastKeySize = page.getKey(index).getSize();
            logger.debug("Key " + index + " is " + lastKeySize + " bytes");
            numRelocated++;
            // 只要本页或相邻页的空间有一个足够即可
            if (pageBytesFree >= bytesRequired && (adjBytesFree + keyBytesMoved + 2 * numRelocated) >= bytesRequired) {
                break;
            }
        }
        assert numRelocated >= 0;
        return numRelocated;
    }

    /**
     * This helper function adds an entry (a key and associated pointer) to this
     * inner page, after the page-pointer {@code pagePtr1}.
     *
     * @param page the inner page to add the entry to
     *
     * @param pagePath the path of page-numbers to this inner page
     *
     * @param pagePtr1 the <u>existing</u> page that the new key and next-page
     *        number will be inserted after
     *
     * @param key1 the new key-value to insert after the {@code pagePtr1} value
     *
     * @param pagePtr2 the new page-pointer value to follow the {@code key1}
     *        value
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public void addEntry(InnerPage page, List<Integer> pagePath, int pagePtr1, Tuple key1, int pagePtr2)
            throws IOException {

        // The new entry will be the key, plus 2 bytes for the page-pointer.
        List<ColumnInfo> colInfos = page.getIndexFileInfo().getIndexSchema();
        int newEntrySize = PageTuple.getTupleStorageSize(colInfos, key1) + 2;

        if (page.getFreeSpace() < newEntrySize) {
            // 先尝试着将重新分配空间，如果还是不够则将此页分裂为
            if (!relocatePointersAndAddKey(page, pagePath, pagePtr1, key1, pagePtr2, newEntrySize)) {
                splitAndAddKey(page, pagePath, pagePtr1, key1, pagePtr2);
            }
        } else {
            // 空间够就直接将entry加入
            page.addEntry(pagePtr1, key1, pagePtr2);
        }

    }

    private boolean relocatePointersAndAddKey(InnerPage page, List<Integer> pagePath, int pagePtr1, Tuple key1,
            int pagePtr2, int newEntrySize) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException("Inner page number doesn't match last page-number in page path");
        }

        // See if we are able to relocate records either direction to free up
        // space for the new key.

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();

        if (pathSize == 1) // This node is also the root - no parent.
            return false; // There aren't any siblings to relocate to.

        int parentPageNo = pagePath.get(pathSize - 2);

        InnerPage parentPage = loadPage(idxFileInfo, parentPageNo);
        int numPointers = parentPage.getNumPointers();
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        // Check each sibling in its own code block so that we can constrain
        // the scopes of the variables a bit. This keeps us from accidentally
        // reusing the "prev" variables in the "next" section.

        {
            InnerPage prevPage = null;
            if (pagePtrIndex - 1 >= 0) {
                prevPage = loadPage(idxFileInfo, parentPage.getPointer(pagePtrIndex - 1));
            }
            if (prevPage != null) {
                // See if we can move some of this inner node's entries to the
                // previous node, to free up space.

                BTreeIndexPageTuple parentKey = parentPage.getKey(pagePtrIndex - 1);
                int parentKeySize = parentKey.getSize();

                int count = tryNonLeafRelocateForSpace(page, prevPage, false, newEntrySize, parentKeySize);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(
                            String.format("Relocating %d entries from " + "inner-page %d to left-sibling inner-page %d",
                                    count, page.getPageNo(), pagePtr1));

                    logger.debug("Space before relocation:  Inner = " + page.getFreeSpace() + " bytes\t\tSibling = "
                            + prevPage.getFreeSpace() + " bytes");

                    TupleLiteral newParentKey = page.movePointersLeft(prevPage, count, parentKey);

                    addEntryToInnerPair(prevPage, page, pagePtr1, key1, pagePtr2);

                    logger.debug("New parent-key is " + newParentKey);

                    pagePath.remove(pathSize - 1);
                    replaceKey(parentPage, pagePath, prevPage.getPageNo(), newParentKey, page.getPageNo());

                    logger.debug("Space after relocation:  Inner = " + page.getFreeSpace() + " bytes\t\tSibling = "
                            + prevPage.getFreeSpace() + " bytes");

                    return true;
                }
            }
        }

        {
            InnerPage nextPage = null;
            if (pagePtrIndex + 1 < numPointers) {
                nextPage = loadPage(idxFileInfo, parentPage.getPointer(pagePtrIndex + 1));
            }
            if (nextPage != null) {
                // See if we can move some of this inner node's entries to the
                // previous node, to free up space.

                BTreeIndexPageTuple parentKey = parentPage.getKey(pagePtrIndex);
                int parentKeySize = parentKey.getSize();

                int count = tryNonLeafRelocateForSpace(page, nextPage, true, newEntrySize, parentKeySize);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format(
                            "Relocating %d entries from " + "inner-page %d to right-sibling inner-page %d", count,
                            page.getPageNo(), pagePtr2));

                    logger.debug("Space before relocation:  Inner = " + page.getFreeSpace() + " bytes\t\tSibling = "
                            + nextPage.getFreeSpace() + " bytes");

                    TupleLiteral newParentKey = page.movePointersRight(nextPage, count, parentKey);

                    addEntryToInnerPair(page, nextPage, pagePtr1, key1, pagePtr2);

                    logger.debug("New parent-key is " + newParentKey);

                    pagePath.remove(pathSize - 1);
                    replaceKey(parentPage, pagePath, page.getPageNo(), newParentKey, nextPage.getPageNo());

                    logger.debug("Space after relocation:  Inner = " + page.getFreeSpace() + " bytes\t\tSibling = "
                            + nextPage.getFreeSpace() + " bytes");

                    return true;
                }
            }
        }

        // Couldn't relocate entries to either the previous or next page. We
        // must split the leaf into two.
        return false;
    }

    /**
     * <p>
     * This helper function splits the specified inner page into two pages, also
     * updating the parent page in the process, and then inserts the specified
     * key and page-pointer into the appropriate inner page. This method is used
     * to add a key/pointer to an inner page that doesn't have enough space,
     * when it isn't possible to relocate pointers to the left or right sibling
     * of the page.
     * </p>
     * <p>
     * When the inner node is split, half of the pointers are put into the new
     * sibling, regardless of the size of the keys involved. In other words,
     * this method doesn't try to keep the pages half-full based on bytes used.
     * </p>
     *
     * @param page the inner node to split and then add the key/pointer to
     *
     * @param pagePath the sequence of page-numbers traversed to reach this
     *        inner node.
     *
     * @param pagePtr1 the existing page-pointer after which the new key and
     *        pointer should be inserted
     *
     * @param key1 the new key to insert into the inner page, immediately after
     *        the page-pointer value {@code pagePtr1}.
     *
     * @param pagePtr2 the new page-pointer value to insert after the new key
     *        value
     *
     * @throws IOException if an IO error occurs during the operation.
     */
    private void splitAndAddKey(InnerPage page, List<Integer> pagePath, int pagePtr1, Tuple key1, int pagePtr2)
            throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException("Inner page number doesn't match last page-number in page path");
        }

        logger.debug("Splitting inner-page " + page.getPageNo() + " into two inner pages.");

        // Get a new blank page in the index, with the same parent as the
        // inner-page we were handed.

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage newDBPage = bTreeManager.getNewDataPage(dbFile);
        InnerPage newPage = InnerPage.init(newDBPage, idxFileInfo);

        // Figure out how many values we want to move from the old page to the
        // new page.

        int numPointers = page.getNumPointers();

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Relocating %d pointers from left-page %d" + " to right-page %d", numPointers,
                    page.getPageNo(), newPage.getPageNo()));
            logger.debug("    Old left # of pointers:  " + page.getNumPointers());
            logger.debug("    Old right # of pointers:  " + newPage.getNumPointers());
        }

        Tuple parentKey = null;
        InnerPage parentPage = null;

        int parentPageNo = 0;
        if (pathSize > 1)
            parentPageNo = pagePath.get(pathSize - 2);

        if (parentPageNo != 0) {
            parentPage = loadPage(idxFileInfo, parentPageNo);
            int parentPtrIndex = parentPage.getIndexOfPointer(page.getPageNo());
            if (parentPtrIndex < parentPage.getNumPointers() - 1)
                parentKey = parentPage.getKey(parentPtrIndex);
        }
        Tuple newParentKey = page.movePointersRight(newPage, numPointers / 2, parentKey);

        if (logger.isDebugEnabled()) {
            logger.debug("    New parent key:  " + newParentKey);
            logger.debug("    New left # of pointers:  " + page.getNumPointers());
            logger.debug("    New right # of pointers:  " + newPage.getNumPointers());
        }

        addEntryToInnerPair(page, newPage, pagePtr1, key1, pagePtr2);

        // If the current node doesn't have a parent, it's because it's
        // currently the root.
        if (parentPageNo == 0) {
            // Create a new root node and set both leaves to have it as their
            // parent.
            DBPage dbpParent = bTreeManager.getNewDataPage(dbFile);
            parentPage = InnerPage.init(dbpParent, idxFileInfo, page.getPageNo(), newParentKey, newPage.getPageNo());

            parentPageNo = parentPage.getPageNo();

            // We have a new root-page in the index!
            DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
            HeaderPage.setRootPageNo(dbpHeader, parentPageNo);

            logger.debug("Set index root-page to inner-page " + parentPageNo);
        } else {
            // Add the new page into the parent non-leaf node. (This may cause
            // the parent node's contents to be moved or split, if the parent
            // is full.)

            // (We already set the new node's parent-page-number earlier.)

            pagePath.remove(pathSize - 1);
            addEntry(parentPage, pagePath, page.getPageNo(), newParentKey, newPage.getPageNo());

            logger.debug("Parent page " + parentPageNo + " now has " + parentPage.getNumPointers() + " page-pointers.");
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Parent page contents:\n" + parentPage.toFormattedString());
        }
    }

    /**
     * This helper method takes a pair of inner nodes that are siblings to each
     * other, and adds the specified key to whichever node the key should go
     * into.
     *
     * @param prevPage the first page in the pair, left sibling of
     *        {@code nextPage}
     *
     * @param nextPage the second page in the pair, right sibling of
     *        {@code prevPage}
     *
     * @param pageNo1 the pointer to the left of the new key/pointer values that
     *        will be added to one of the pages
     *
     * @param key1 the new key-value to insert immediately after the existing
     *        {@code pageNo1} value
     *
     * @param pageNo2 the new pointer-value to insert immediately after the new
     *        {@code key1} value
     */
    private void addEntryToInnerPair(InnerPage prevPage, InnerPage nextPage, int pageNo1, Tuple key1, int pageNo2) {

        InnerPage page;

        // See if pageNo1 appears in the left page.
        int ptrIndex1 = prevPage.getIndexOfPointer(pageNo1);
        if (ptrIndex1 != -1) {
            page = prevPage;
        } else {
            // The pointer *should be* in the next page. Verify this...
            page = nextPage;

            if (nextPage.getIndexOfPointer(pageNo1) == -1) {
                throw new IllegalStateException(String.format("Somehow lost page-pointer %d from inner pages %d and %d",
                        pageNo1, prevPage.getPageNo(), nextPage.getPageNo()));
            }
        }

        page.addEntry(pageNo1, key1, pageNo2);
    }
}

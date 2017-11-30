package com.bow.lab.storage.btree;

import java.io.IOException;
import java.util.List;

import com.bow.lab.storage.IStorageService;
import com.bow.maple.expressions.TupleComparator;
import com.bow.maple.expressions.TupleLiteral;
import com.bow.maple.indexes.IndexFileInfo;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.btreeindex.BTreeIndexPageTuple;
import com.bow.maple.storage.btreeindex.HeaderPage;
import com.bow.maple.util.ExtensionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides high-level B<sup>+</sup> tree management operations
 * performed on leaf nodes. These operations are provided here and not on the
 * {@link LeafPage} class since they sometimes involve splitting or merging leaf
 * nodes, updating parent nodes, and so forth.
 */
public class LeafPageOperations {

    private static Logger logger = LoggerFactory.getLogger(LeafPageOperations.class);

    private IStorageService storageManager = ExtensionLoader.getExtensionLoader(IStorageService.class).getExtension();

    private BTreeIndexService bTreeManager;

    private InnerPageOperations innerPageOps;

    public LeafPageOperations(BTreeIndexService bTreeManager, InnerPageOperations innerPageOps) {
        this.bTreeManager = bTreeManager;
        this.innerPageOps = innerPageOps;
    }

    /**
     * 加在叶子页，pageNo如果为0则返回Null
     * @param idxFileInfo 记录了leaf-page的index文件信息.
     * @param pageNo 要加载的leaf-page的pageNo.
     * @return 一个初始化的{@link LeafPage}
     * @throws IOException if an IO error
     * @throws IllegalArgumentException if the specified page isn't a leaf-page
     */
    private LeafPage loadLeafPage(IndexFileInfo idxFileInfo, int pageNo) throws IOException {
        if (pageNo == 0){
            return null;
        }
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new LeafPage(dbPage, idxFileInfo);
    }

    /**
     * This helper function handles the operation of adding a new index-key and
     * tuple-pointer (contained within the key) to a leaf-page of the index.
     * This operation is provided here and not on the {@link LeafPage} class,
     * because adding the new entry might require the leaf page to be split into
     * two pages.
     *
     * @param leaf the leaf page to add the entry to
     *
     * @param newTupleKey the new entry to add to the leaf page
     *
     * @param pagePath the path of pages taken from the root page to this leaf
     *        page, represented as a list of page numbers in the data file
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public void addEntry(LeafPage leaf, TupleLiteral newTupleKey, List<Integer> pagePath) throws IOException {

        // Figure out where the new key-value goes in the leaf page.

        int newEntrySize = newTupleKey.getStorageSize();
        if (leaf.getFreeSpace() < newEntrySize) {
            // Try to relocate entries from this leaf to either sibling,
            // or if that can't happen, split the leaf page into two.
            if (!relocateEntriesAndAddKey(leaf, pagePath, newTupleKey)){
                splitLeafAndAddKey(leaf, pagePath, newTupleKey);
            }
        } else {
            // There is room in the leaf for the new key. Add it there.
            leaf.addEntry(newTupleKey);
        }
    }

    private boolean tryMoveLeft(LeafPage page, List<Integer> pagePath, TupleLiteral key) throws IOException {
        int pathSize = pagePath.size();
        int parentPageNo = 0;
        if (pathSize >= 2){
            parentPageNo = pagePath.get(pathSize - 2);
        }

        int bytesRequired = key.getStorageSize();
        IndexFileInfo idxFileInfo = page.getIndexFileInfo();
        InnerPage parentPage = innerPageOps.loadPage(idxFileInfo, parentPageNo);
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        //获取从parentPage获取前一页
        LeafPage prevPage = null;
        if (pagePtrIndex - 1 >= 0) {
            prevPage = loadLeafPage(idxFileInfo, parentPage.getPointer(pagePtrIndex - 1));
        }

        if (prevPage != null) {
            // 尝试将page上的内容往左页移动，希望能获取bytesRequired空间
            int count = tryLeafRelocateForSpace(page, prevPage, false, bytesRequired);

            if (count > 0) {
                // > 0表明移动count个entry就可以腾出空间
                logger.debug("Relocating {} entries from leaf-page {} to left-sibling leaf-page {}", count,
                        page.getPageNo(), prevPage.getPageNo());
                logger.debug("Space before relocation:  Leaf = " + page.getFreeSpace() + " bytes\t\tSibling = "
                        + prevPage.getFreeSpace() + " bytes");
                //往左叶上移动count个entry
                page.moveEntriesLeft(prevPage, count);
                logger.debug("Space after relocation:  Leaf = " + page.getFreeSpace() + " bytes\t\tSibling = "
                        + prevPage.getFreeSpace() + " bytes");

                //将key放到prevPage或是page中
                BTreeIndexPageTuple firstRightKey = addEntryToLeafPair(prevPage, page, key);

                //调整父页中的pageNo,key的顺序(因为马上要处理父页，因此这里将pagePath中的当前页移除)
                pagePath.remove(pathSize - 1);
                // inner page中 |prevPage|firstRightKey|page|
                innerPageOps.replaceKey(parentPage, pagePath, prevPage.getPageNo(), firstRightKey,
                        page.getPageNo());
                return true;
            }
        }
        return false;
    }


    private boolean tryMoveRight(LeafPage page, List<Integer> pagePath, TupleLiteral key) throws IOException {
        int pathSize = pagePath.size();
        int bytesRequired = key.getStorageSize();
        IndexFileInfo idxFileInfo = page.getIndexFileInfo();

        //加载父页
        int parentPageNo = 0;
        if (pathSize >= 2){
            //倒数第二个就是父页
            parentPageNo = pagePath.get(pathSize - 2);
        }
        InnerPage parentPage = innerPageOps.loadPage(idxFileInfo, parentPageNo);
        int numPointers = parentPage.getNumPointers();
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        //从父页上获得下一页的pageNo
        LeafPage nextPage = null;
        if (pagePtrIndex + 1 < numPointers) {
            nextPage = loadLeafPage(idxFileInfo, parentPage.getPointer(pagePtrIndex + 1));
        }

        if (nextPage != null) {
            // 尝试朝右移动数据，腾出空间
            int count = tryLeafRelocateForSpace(page, nextPage, true, bytesRequired);
            if (count > 0) {
                logger.debug("Relocating {} entries from leaf-page {} to right-sibling leaf-page {}",
                        count, page.getPageNo(), nextPage.getPageNo());
                logger.debug("Space before relocation:  Leaf = " + page.getFreeSpace() + " bytes. Sibling = "
                        + nextPage.getFreeSpace() + " bytes");

                //朝右移动数据
                page.moveEntriesRight(nextPage, count);
                logger.debug("Space after relocation:  Leaf = " + page.getFreeSpace() + " bytes\t\tSibling = "
                        + nextPage.getFreeSpace() + " bytes");

                //将key加入到page或者nextPage中
                BTreeIndexPageTuple firstRightKey = addEntryToLeafPair(page, nextPage, key);

                //调整父页中的pageNo,key的顺序(因为马上要处理父页，因此这里将pagePath中的当前页移除)
                pagePath.remove(pathSize - 1);
                innerPageOps.replaceKey(parentPage, pagePath, page.getPageNo(), firstRightKey,
                        nextPage.getPageNo());

                return true;
            }
        }
        return false;
    }

    private boolean relocateEntriesAndAddKey(LeafPage page, List<Integer> pagePath, TupleLiteral key)
            throws IOException {

        int pathSize = pagePath.size();
        if (pathSize == 1){
            return false;
        }
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            //page一定要是pagePath中最后的一个
            throw new IllegalArgumentException("leaf page number doesn't match last page-number in page path");
        }

        // 往下一页移动
        if(tryMoveLeft(page, pagePath, key)){
            return true;
        }

        // 往上一页移动
        if(tryMoveRight(page, pagePath, key)){
            return true;
        }

        // 叶子节点容不下了，需要将页节点拆分为2页
        return false;
    }

    /**
     * This helper method takes a pair of leaf nodes that are siblings to each
     * other, and adds the specified key to whichever leaf the key should go
     * into. The method returns the first key in the right leaf-page, since this
     * value is necessary to update the parent node of the pair of leaves.
     *
     * @param prevLeaf left sibling of {@code nextLeaf}
     * @param nextLeaf right sibling of {@code prevLeaf}
     * @param key the key to insert into the pair of leaves
     * @return the first key of {@code nextLeaf}
     */
    private BTreeIndexPageTuple addEntryToLeafPair(LeafPage prevLeaf, LeafPage nextLeaf, TupleLiteral key) {
        BTreeIndexPageTuple firstRightKey = nextLeaf.getKey(0);
        if (TupleComparator.compareTuples(key, firstRightKey) < 0) {
            // The new key goes in the left page.
            prevLeaf.addEntry(key);
        } else {
            // The new key goes in the right page.
            nextLeaf.addEntry(key);
            // Re-retrieve the right page's first key since it may have changed.
            firstRightKey = nextLeaf.getKey(0);
        }
        return firstRightKey;
    }

    /**
     * This helper function determines how many entries must be relocated from
     * one leaf-page to another, in order to free up the specified number of
     * bytes. If it is possible, the number of entries that must be relocated is
     * returned. If it is not possible, the method returns 0.
     *
     * leaf需要腾出bytesRequired空间，因此将此页的部分key转移到相邻页上。
     *
     * @param leaf the leaf node to relocate entries from
     * @param adjLeaf 相邻页，本页的部分key会转移到相邻页
     * @param movingRight pass {@code true} if the sibling is to the right of
     *        {@code page} (and therefore we are moving entries right), or
     *        {@code false} if the sibling is to the left of {@code page} (and
     *        therefore we are moving entries left).
     *
     * @param bytesRequired leaf需要腾出空间大小
     *
     * @return leaf为释放空间所需要移动的entry数量
     */
    private int tryLeafRelocateForSpace(LeafPage leaf, LeafPage adjLeaf, boolean movingRight, int bytesRequired) {

        int numKeys = leaf.getNumEntries();
        int leafBytesFree = leaf.getFreeSpace();
        int adjBytesFree = adjLeaf.getFreeSpace();

        logger.debug("Leaf bytes free:  " + leafBytesFree + " Adjacent leaf bytes free:  " + adjBytesFree);

        // Subtract the bytes-required from the adjacent-bytes-free value so
        // that we ensure we always have room to put the key in either node.
        adjBytesFree -= bytesRequired;

        int numRelocated = 0;
        while (true) {
            int index;
            if (movingRight){
                //若朝右页移动，则从最后的一个key开始，找到一片连续空间(>=bytesRequired)
                index = numKeys - numRelocated - 1;
            } else{
                //朝左，则从最左边的key开始找
                index = numRelocated;
            }
            int keySize = leaf.getKeySize(index);
            logger.debug("Key " + index + " is " + keySize + " bytes");
            if (adjBytesFree < keySize){
                //前一页的空闲空间不够了
                break;
            }

            numRelocated++;
            //将此页的key移到相邻页后free就增加了，相邻页空余空间就减少了
            leafBytesFree += keySize;
            adjBytesFree -= keySize;

            // 因为不知道new key会加到本页还是相邻页，因此只要一个空间足够即可
            if (leafBytesFree >= bytesRequired && adjBytesFree >= bytesRequired) {
                break;
            }
        }
        logger.debug("Can relocate " + numRelocated + " keys to free up space.");
        return numRelocated;
    }

    /**
     * <p>
     * This helper function splits the specified leaf-node into two nodes, also
     * updating the parent node in the process, and then inserts the specified
     * search-key into the appropriate leaf. This method is used to add a key to
     * a leaf that doesn't have enough space, when it isn't possible to relocate
     * values to the left or right sibling of the leaf.
     * </p>
     * <p>
     * When the leaf node is split, half of the keys are put into the new leaf,
     * regardless of the size of individual keys. In other words, this method
     * doesn't try to keep the leaves half-full based on bytes used.
     * </p>
     *
     *
     * @param leaf the leaf node to split and then add the key to
     * @param pagePath 从root到此节点所经过的pageNo
     * @param key the new key to insert into the leaf node
     * @throws IOException if an IO error occurs during the operation.
     */
    private void splitLeafAndAddKey(LeafPage leaf, List<Integer> pagePath, TupleLiteral key) throws IOException {

        int pathSize = pagePath.size();
        //当前的pageNo不是路径中的最后一个，就有问题
        if (pagePath.get(pathSize - 1) != leaf.getPageNo()) {
            throw new IllegalArgumentException("Leaf page number doesn't match last page-number in page path");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Splitting leaf-page " + leaf.getPageNo() + " into two leaves.");
            logger.debug("Old next-page:  " + leaf.getNextPageNo());
        }

        // Get a new blank page in the index, with the same parent as the
        // leaf-page we were handed.

        IndexFileInfo idxFileInfo = leaf.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage newDBPage = bTreeManager.getNewDataPage(dbFile);
        LeafPage newLeaf = LeafPage.init(newDBPage, idxFileInfo);

        // 新建的leaf始终跟在本页后面
        leaf.setNextPageNo(newLeaf.getPageNo());
        newLeaf.setNextPageNo(leaf.getNextPageNo());

        if (logger.isDebugEnabled()) {
            logger.debug("New next-page:  " + leaf.getNextPageNo());
            logger.debug("New next-leaf next-page:  " + newLeaf.getNextPageNo());
        }

        // 移一半的数据到右页
        int numEntries = leaf.getNumEntries();
        if (logger.isDebugEnabled()) {
            logger.debug("Relocating {} entries from left-leaf {} to right-leaf {}", numEntries,
                    leaf.getPageNo(), newLeaf.getPageNo());
            logger.debug("Old left # of entries:  " + leaf.getNumEntries());
            logger.debug("Old right # of entries:  " + newLeaf.getNumEntries());
        }
        leaf.moveEntriesRight(newLeaf, numEntries / 2);
        if (logger.isDebugEnabled()) {
            logger.debug("New left # of entries:  " + leaf.getNumEntries());
            logger.debug("New right # of entries:  " + newLeaf.getNumEntries());
        }

        BTreeIndexPageTuple firstRightKey = addEntryToLeafPair(leaf, newLeaf, key);

        // If the current node doesn't have a parent, it's because it's
        // currently the root.
        if (pathSize == 1) {
            // 创建一个root节点，是leaf和newLeaf的父节点
            DBPage parentPage = bTreeManager.getNewDataPage(dbFile);
            InnerPage.init(parentPage, idxFileInfo, leaf.getPageNo(), firstRightKey, newLeaf.getPageNo());
            int parentPageNo = parentPage.getPageNo();
            // We have a new root-page in the index!
            DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
            HeaderPage.setRootPageNo(dbpHeader, parentPageNo);
            logger.debug("Set index root-page to inner-page " + parentPageNo);
        } else {
            // Add the new leaf into the parent non-leaf node. (This may cause
            // the parent node's contents to be moved or split, if the parent
            // is full.)

            // (We already set the new leaf's parent-page-number earlier.)

            int parentPageNo = pagePath.get(pathSize - 2);
            DBPage dbpParent = storageManager.loadDBPage(dbFile, parentPageNo);
            InnerPage parentPage = new InnerPage(dbpParent, idxFileInfo);
            pagePath.remove(pathSize - 1);
            innerPageOps.addEntry(parentPage, pagePath, leaf.getPageNo(), firstRightKey, newLeaf.getPageNo());
            logger.debug("Parent page " + parentPageNo + " now has " + parentPage.getNumPointers() + " page-pointers.");
        }
    }
}

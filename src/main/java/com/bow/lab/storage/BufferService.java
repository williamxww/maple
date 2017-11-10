package com.bow.lab.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bow.maple.client.SessionState;
import com.bow.maple.storage.DBFile;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.FileManager;
import com.bow.maple.storage.StorageManager;
import com.bow.maple.storage.writeahead.LogSequenceNumber;
import com.bow.maple.transactions.TransactionManager;
import com.bow.maple.util.PropertiesUtil;
import com.bow.maple.util.StringUtil;

/**
 * The buffer manager reduces the number of disk IO operations by managing an
 * in-memory cache of data pages.
 *
 * @todo Add integrity checks, e.g. to make sure every cached page's file
 *       appears in the collection of cached files.
 */
public class BufferService {

    /**
     * The system property that can be used to specify the size of the page
     * cache in the buffer manager.
     */
    public static final String PROP_PAGECACHE_SIZE = "nanodb.pagecache.size";

    /** The default page-cache size is defined to be 4MB. */
    public static final long DEFAULT_PAGECACHE_SIZE = 4 * 1024 * 1024;

    /**
     * The system property that can be used to specify the page replacement
     * policy in the buffer manager.
     */
    public static final String PROP_PAGECACHE_POLICY = "nanodb.pagecache.policy";

    /**
     * 封装了数据文件和页号
     */
    private static class CachedPageInfo {
        public DBFile dbFile;

        public int pageNo;

        public CachedPageInfo(DBFile dbFile, int pageNo) {
            if (dbFile == null) {
                throw new IllegalArgumentException("dbFile cannot be null");
            }
            this.dbFile = dbFile;
            this.pageNo = pageNo;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachedPageInfo) {
                CachedPageInfo other = (CachedPageInfo) obj;
                return dbFile.equals(other.dbFile) && pageNo == other.pageNo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + dbFile.hashCode();
            hash = 31 * hash + pageNo;
            return hash;
        }
    }

    /**
     * 被钉住的page是不能被移除缓存
     */
    private static class PinnedPageInfo {
        /**
         * pin page的session
         */
        public int sessionID;

        public DBPage dbPage;

        public PinnedPageInfo(int sessionID, DBPage dbPage) {
            this.sessionID = sessionID;
            this.dbPage = dbPage;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PinnedPageInfo) {
                PinnedPageInfo other = (PinnedPageInfo) obj;
                return sessionID == other.sessionID && dbPage.equals(other.dbPage);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + sessionID;
            hash = 31 * hash + dbPage.hashCode();
            return hash;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(BufferService.class);

    private FileManager fileManager;

    /**
     * Map<filename,DBFile>所有已打开的文件
     */
    private Map<String, DBFile> cachedFiles;

    /**
     * 用来缓存数据页(不包括WAL pages)，此map拥有相关的失效策略如LRU
     */
    private Map<CachedPageInfo, DBPage> cachedPages;

    /**
     * This collection holds all pages that are pinned by various sessions that
     * are currently accessing the database.
     */
    private HashSet<PinnedPageInfo> pinnedPages;

    /**
     * Map<sessionId, Set<PinnedPageInfo>>
     */
    private Map<Integer, Set<PinnedPageInfo>> pinnedPagesBySessionID;

    /**
     * 总共已缓存字节数
     */
    private long totalBytesCached;

    /**
     * 配置的缓存大小
     */
    private long maxCacheSize;

    public BufferService(FileManager fileManager) {
        this.fileManager = fileManager;

        configureMaxCacheSize();

        cachedFiles = new LinkedHashMap();

        String replacementPolicy = configureReplacementPolicy();
        cachedPages = new LinkedHashMap(16, 0.75f, "lru".equals(replacementPolicy));

        totalBytesCached = 0;

        pinnedPages = new HashSet<PinnedPageInfo>();
        pinnedPagesBySessionID = new HashMap<Integer, Set<PinnedPageInfo>>();
    }

    /**
     * 配置最大缓存
     */
    private void configureMaxCacheSize() {
        // Set the default up-front; it's just easier that way.
        maxCacheSize = DEFAULT_PAGECACHE_SIZE;

        String str = PropertiesUtil.getProperty(PROP_PAGECACHE_SIZE);
        if (str != null) {
            try {
                maxCacheSize = StringUtil.toLongWithUnit(str);
            } catch (NumberFormatException e) {
                logger.error("Could not parse page-cache size value {}; using default value of {} bytes", str,
                        DEFAULT_PAGECACHE_SIZE);
            }
        }
    }

    private String configureReplacementPolicy() {
        String str = PropertiesUtil.getProperty(PROP_PAGECACHE_POLICY);
        if (str != null) {
            str = str.trim().toLowerCase();

            if (!("lru".equals(str) || "fifo".equals(str))) {
                logger.error(String.format(
                        "Unrecognized value \"%s\" for page-cache replacement " + "policy; using default value of LRU.",
                        str));
            }
        }

        return str;
    }

    /**
     * Retrieves the specified {@link DBFile} from the buffer manager, if it has
     * already been opened.
     *
     * @param filename The filename of the database file to retrieve. This
     *        should be ONLY the database filename, no path. The path is
     *        expected to be relative to the database's base directory.
     *
     * @return the {@link DBFile} corresponding to the filename, if it has
     *         already been opened, or <tt>null</tt> if the file isn't currently
     *         open.
     */
    public DBFile getFile(String filename) {
        DBFile dbFile = cachedFiles.get(filename);

        logger.debug(String.format("Requested file %s is%s in file-cache.", filename, (dbFile != null ? "" : " NOT")));

        return dbFile;
    }

    /**
     * 将dbFile存放在内存中
     * 
     * @param dbFile 打开的文件
     */
    public void addFile(DBFile dbFile) {
        if (dbFile == null) {
            throw new IllegalArgumentException("dbFile cannot be null");
        }

        String filename = dbFile.getDataFile().getName();
        if (cachedFiles.containsKey(filename)) {
            throw new IllegalStateException("File cache already contains file " + filename);
        }

        // NOTE: If we want to keep a cap on how many files are opened, we
        // would do that here.

        logger.debug(String.format("Adding file %s to file-cache.", filename));

        cachedFiles.put(filename, dbFile);
    }

    public void pinPage(DBPage dbPage) {
        // Make sure this page is pinned by the session so that we don't
        // flush it until the session is done with it.

        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);

        // 将pinnedPage添加到pinnedPages
        if (pinnedPages.add(pp)) {
            dbPage.incPinCount();
            logger.debug(String.format("Session %d is pinning page [%s,%d].  " + "New pin-count is %d.", sessionID,
                    dbPage.getDBFile(), dbPage.getPageNo(), dbPage.getPinCount()));
        }

        // 将pinnedPage添加到pinnedPagesBySessionID，方便查找
        Set<PinnedPageInfo> pinnedBySession = pinnedPagesBySessionID.get(sessionID);
        if (pinnedBySession == null) {
            pinnedBySession = new HashSet<PinnedPageInfo>();
            pinnedPagesBySessionID.put(sessionID, pinnedBySession);
        }
        pinnedBySession.add(pp);
    }

    public void unpinPage(DBPage dbPage) {
        // If the page is pinned by the session then unpin it.
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);

        // 从 pinned page set中移除
        if (pinnedPages.remove(pp)) {
            dbPage.decPinCount();
            logger.debug(String.format("Session %d is unpinning page " + "[%s,%d].  New pin-count is %d.", sessionID,
                    dbPage.getDBFile(), dbPage.getPageNo(), dbPage.getPinCount()));
        }

        // 从 pinnedBySession中移除
        Set<PinnedPageInfo> pinnedBySession = pinnedPagesBySessionID.get(sessionID);

        if (pinnedBySession != null) {
            pinnedBySession.remove(pp);

            // pinned page全移除后，将此session entry删除
            if (pinnedBySession.isEmpty()) {
                pinnedPagesBySessionID.remove(sessionID);
            }
        }
    }

    /**
     * This method unpins all pages pinned by the current session. This is
     * generally done at the end of each transaction so that pages aren't pinned
     * forever, and can actually be evicted from the buffer manager.
     */
    public void unpinAllPages() {
        // Unpin all pages pinned by this session.
        int sessionID = SessionState.get().getSessionID();

        // Remove the set of pages pinned by this session, and save the
        // return-value so we can iterate through it and unpin each page.
        Set<PinnedPageInfo> pinnedBySession = pinnedPagesBySessionID.remove(sessionID);

        // If no pages pinned, we're done.
        if (pinnedBySession == null)
            return;

        for (PinnedPageInfo pp : pinnedBySession) {
            DBPage dbPage = pp.dbPage;

            pinnedPages.remove(pp);
            dbPage.decPinCount();
            logger.debug(String.format("Session %d is unpinning page " + "[%s,%d].  New pin-count is %d.", sessionID,
                    dbPage.getDBFile(), dbPage.getPageNo(), dbPage.getPinCount()));
        }
    }

    public DBPage getPage(DBFile dbFile, int pageNo) {
        DBPage dbPage = cachedPages.get(new CachedPageInfo(dbFile, pageNo));

        logger.debug(String.format("Requested page [%s,%d] is%s in page-cache.", dbFile, pageNo,
                (dbPage != null ? "" : " NOT")));

        if (dbPage != null) {
            // Make sure this page is pinned by the session so that we don't
            // flush it until the session is done with it.
            pinPage(dbPage);
        }

        return dbPage;
    }

    /**
     * 将指定page加入到缓存中
     * 
     * @param dbPage 数据页
     * @throws IOException e
     */
    public void addPage(DBPage dbPage) throws IOException {
        if (dbPage == null) {
            throw new IllegalArgumentException("dbPage cannot be null");
        }

        DBFile dbFile = dbPage.getDBFile();
        int pageNo = dbPage.getPageNo();

        CachedPageInfo cpi = new CachedPageInfo(dbFile, pageNo);
        if (cachedPages.containsKey(cpi)) {
            throw new IllegalStateException(String.format("Page cache already contains page [%s,%d]", dbFile, pageNo));
        }

        logger.debug("Adding page [{},{}] to page-cache.", dbFile, pageNo);

        // 检查空间是否足够
        int pageSize = dbPage.getPageSize();
        ensureSpaceAvailable(pageSize);

        cachedPages.put(cpi, dbPage);

        // Make sure this page is pinned by the session so that we don't flush
        // it until the session is done with it.
        pinPage(dbPage);
    }

    /**
     * This helper function ensures that the buffer manager has the specified
     * amount of space available. This is done by removing pages out of the
     * buffer manager's cache
     *
     * @param bytesRequired the amount of space that should be made available in
     *        the cache, in bytes
     *
     * @throws IOException if an IO error occurs when flushing dirty pages out
     *         to disk
     */
    private void ensureSpaceAvailable(int bytesRequired) throws IOException {
        // 空间已足够
        if (bytesRequired + totalBytesCached <= maxCacheSize) {
            return;
        }

        // 空间不够时，移除部分页，移除时先记录write-ahead log
        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        if (!cachedPages.isEmpty()) {

            Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();

            while (entries.hasNext() && bytesRequired + totalBytesCached > maxCacheSize) {
                Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

                DBPage oldPage = entry.getValue();
                // Can't flush pages that are in use.
                if (oldPage.isPinned()) {
                    continue;
                }

                logger.debug("    Evicting page [{},{}] from page-cache to make room.", oldPage.getDBFile(),
                        oldPage.getPageNo());

                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // If the page is dirty, we need to write its data to disk
                // before
                // invalidating it. Otherwise, just invalidate it.
                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; must save to disk.");
                    dirtyPages.add(oldPage);
                } else {
                    oldPage.invalidate();
                }
            }
        }

        // If we have any dirty data pages, they need to be flushed to disk.
        writeDirtyPages(dirtyPages,true);

        if (bytesRequired + totalBytesCached > maxCacheSize){
            logger.warn("Buffer manager is currently using too much space.");
        }
    }

    private void writeDirtyPages(List<DBPage> dirtyPages, boolean invalidate) throws IOException {
        if (!dirtyPages.isEmpty()) {
            for (DBPage dbPage : dirtyPages) {
                DBFileType type = dbPage.getDBFile().getType();
                if (type == DBFileType.WRITE_AHEAD_LOG_FILE || type == DBFileType.TXNSTATE_FILE) {
                    // We don't log changes to these files.
                    continue;
                }
            }

            // Finally, we can write out each dirty page.
            for (DBPage dbPage : dirtyPages) {
                fileManager.saveDBPage(dbPage);

                if (invalidate) {
                    dbPage.invalidate();
                }

            }
        }
    }

    /**
     * This method writes all dirty pages in the specified file, optionally
     * syncing the file after performing the write. The pages are not removed
     * from the buffer manager after writing them; their dirty state is simply
     * cleared.
     *
     * @param dbFile the file whose dirty pages should be written to disk
     *
     * @param minPageNo dirty pages with a page-number less than this value will
     *        not be written to disk
     *
     * @param maxPageNo dirty pages with a page-number greater than this value
     *        will not be written to disk
     *
     * @param sync If true then the database file will be sync'd to disk; if
     *        false then no sync will occur. The sync will always occur, in case
     *        dirty pages had previously been flushed to disk without syncing.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeDBFile(DBFile dbFile, int minPageNo, int maxPageNo, boolean sync) throws IOException {

        logger.info(
                String.format("Writing all dirty pages for file %s to disk%s.", dbFile, (sync ? " (with sync)" : "")));

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();
                if (!oldPage.isDirty())
                    continue;

                int pageNo = oldPage.getPageNo();
                if (pageNo < minPageNo || pageNo > maxPageNo)
                    continue;

                logger.debug(
                        String.format("    Saving page [%s,%d] to disk.", oldPage.getDBFile(), oldPage.getPageNo()));

                dirtyPages.add(oldPage);
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ false);

        if (sync) {
            logger.debug("Syncing file " + dbFile);
            fileManager.syncDBFile(dbFile);
        }
    }

    /**
     * This method writes all dirty pages in the specified file, optionally
     * syncing the file after performing the write. The pages are not removed
     * from the buffer manager after writing them; their dirty state is simply
     * cleared.
     *
     * @param dbFile the file whose dirty pages should be written to disk
     *
     * @param sync If true then the database file will be sync'd to disk; if
     *        false then no sync will occur. The sync will always occur, in case
     *        dirty pages had previously been flushed to disk without syncing.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeDBFile(DBFile dbFile, boolean sync) throws IOException {
        writeDBFile(dbFile, 0, Integer.MAX_VALUE, sync);
    }

    /**
     * This method writes all dirty pages in the buffer manager to disk. The
     * pages are not removed from the buffer manager after writing them; their
     * dirty state is simply cleared.
     * 
     * @param sync if true, this method will sync all files in which dirty pages
     *        were found, with the exception of WAL files and the
     *        transaction-state file. If false, no file syncing will be
     *        performed.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeAll(boolean sync) throws IOException {
        logger.info("Writing ALL dirty pages in the Buffer Manager to disk.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();
        HashSet<DBFile> dirtyFiles = new HashSet<DBFile>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();
            if (!oldPage.isDirty())
                continue;

            DBFile dbFile = oldPage.getDBFile();
            DBFileType type = dbFile.getType();
            if (type != DBFileType.WRITE_AHEAD_LOG_FILE && type != DBFileType.TXNSTATE_FILE) {
                dirtyFiles.add(oldPage.getDBFile());
            }

            logger.debug(String.format("    Saving page [%s,%d] to disk.", dbFile, oldPage.getPageNo()));

            dirtyPages.add(oldPage);
        }

        writeDirtyPages(dirtyPages, /* invalidate */ false);

        if (sync) {
            logger.debug("Synchronizing all files containing dirty pages to disk.");
            for (DBFile dbFile : dirtyFiles)
                fileManager.syncDBFile(dbFile);
        }
    }

    /**
     * This method removes all cached pages in the specified file from the
     * buffer manager, writing out any dirty pages in the process. This method
     * is not generally recommended to be used, as it basically defeats the
     * purpose of the buffer manager in the first place; rather, the
     * {@link #writeDBFile} method should be used instead. There is a specific
     * situation in which it is used, when a file is being removed from the
     * Buffer Manager by the Storage Manager.
     *
     * @param dbFile the file whose pages should be flushed from the cache
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or the file's contents
     */
    public void flushDBFile(DBFile dbFile) throws IOException {
        logger.info("Flushing all pages for file " + dbFile + " from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();

                logger.debug(String.format("    Evicting page [%s,%d] from page-cache.", oldPage.getDBFile(),
                        oldPage.getPageNo()));

                // Remove the page from the cache.
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // If the page is dirty, we need to write its data to disk
                // before
                // invalidating it. Otherwise, just invalidate it.
                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; must save to disk.");
                    dirtyPages.add(oldPage);
                } else {
                    oldPage.invalidate();
                }
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ true);
    }

    /**
     * This method removes all cached pages from the buffer manager, writing out
     * any dirty pages in the process. This method is not generally recommended
     * to be used, as it basically defeats the purpose of the buffer manager in
     * the first place; rather, the {@link #writeAll} method should be used
     * instead. However, this method is useful to cause certain performance
     * issues to manifest with individual commands, and the Storage Manager also
     * uses it during shutdown processing to ensure all data is saved to disk.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or the file's contents
     */
    public void flushAll() throws IOException {
        logger.info("Flushing ALL database pages from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();

            logger.debug(String.format("    Evicting page [%s,%d] from page-cache.", oldPage.getDBFile(),
                    oldPage.getPageNo()));

            // Remove the page from the cache.
            entries.remove();
            totalBytesCached -= oldPage.getPageSize();

            // If the page is dirty, we need to write its data to disk before
            // invalidating it. Otherwise, just invalidate it.
            if (oldPage.isDirty()) {
                logger.debug("    Evicted page is dirty; must save to disk.");
                dirtyPages.add(oldPage);
            } else {
                oldPage.invalidate();
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ true);
    }

    /**
     * This method removes a file from the cache, first flushing all pages from
     * the file out of the cache. This operation is used by the Storage Manager
     * to close a data file.
     *
     * @param dbFile the file to remove from the cache.
     *
     * @throws IOException if an IO error occurs while writing out dirty pages
     */
    public void removeDBFile(DBFile dbFile) throws IOException {
        logger.info("Removing DBFile " + dbFile + " from buffer manager");
        flushDBFile(dbFile);
        cachedFiles.remove(dbFile.getDataFile().getName());
    }

    /**
     * This method removes ALL files from the cache, first flushing all pages
     * from the cache so that any dirty pages will be saved to disk (possibly
     * updating the write-ahead log in the process). This operation is used by
     * the Storage Manager during shutdown.
     *
     * @return a list of the files that were in the cache, so that they can be
     *         used by the caller if necessary (e.g. to sync and close each one)
     *
     * @throws IOException if an IO error occurs while writing out dirty pages
     */
    public List<DBFile> removeAll() throws IOException {
        logger.info("Removing ALL DBFiles from buffer manager");

        // Flush all pages, ensuring that dirty pages will be written too.
        flushAll();

        // Get the list of DBFiles we had in the cache, then clear the cache.
        ArrayList<DBFile> dbFiles = new ArrayList<DBFile>(cachedFiles.values());
        cachedFiles.clear();

        return dbFiles;
    }
}

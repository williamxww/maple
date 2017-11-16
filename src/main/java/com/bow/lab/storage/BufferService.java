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
import com.bow.maple.util.PropertiesUtil;
import com.bow.maple.util.StringUtil;

/**
 * 为了减少IO操作而架设的组件
 */
public class BufferService implements IBufferService {

    private static Logger logger = LoggerFactory.getLogger(BufferService.class);

    /**
     * 最大缓存大小配置项
     */
    private static final String PROP_PAGECACHE_SIZE = "nanodb.pagecache.size";

    /**
     * page-cache size 默认 4MB.
     */
    private static final long DEFAULT_PAGECACHE_SIZE = 4 * 1024 * 1024;

    /**
     * page cache的替换策略
     */
    private static final String PROP_PAGECACHE_POLICY = "nanodb.pagecache.policy";

    /**
     * 总共已缓存字节数
     */
    private long totalBytesCached;

    /**
     * 配置的缓存大小
     */
    private long maxCacheSize;

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
     * 所有被定住的page的信息
     */
    private Set<PinnedPageInfo> pinnedPages;

    /**
     * 以sessionId为key存放被定住的page的信息 Map<sessionId, Set<PinnedPageInfo>>
     */
    private Map<Integer, Set<PinnedPageInfo>> pinnedPagesBySessionID;

    public BufferService(FileManager fileManager) {
        this.fileManager = fileManager;
        this.maxCacheSize = getMaxCacheSize();
        cachedFiles = new LinkedHashMap<>();
        String replacementPolicy = getReplacementPolicy();
        cachedPages = new LinkedHashMap<>(16, 0.75f, "lru".equals(replacementPolicy));
        totalBytesCached = 0;
        pinnedPages = new HashSet<>();
        pinnedPagesBySessionID = new HashMap<>();
    }

    /**
     * 配置最大缓存
     */
    private long getMaxCacheSize() {
        long size = DEFAULT_PAGECACHE_SIZE;
        String str = PropertiesUtil.getProperty(PROP_PAGECACHE_SIZE);
        if (str != null) {
            try {
                size = StringUtil.toLongWithUnit(str);
            } catch (NumberFormatException e) {
                logger.error("Could not parse page-cache size value {}; using default value of {} bytes", str,
                        DEFAULT_PAGECACHE_SIZE);
            }
        }
        return size;
    }

    private String getReplacementPolicy() {
        String str = PropertiesUtil.getProperty(PROP_PAGECACHE_POLICY);
        if (str != null) {
            str = str.trim().toLowerCase();
            if (!("lru".equals(str) || "fifo".equals(str))) {
                logger.error("Unrecognized value {} for page-cache replacement policy; using default value of LRU.",
                        str);
            }
        }
        return str;
    }

    @Override
    public DBFile getFile(String filename) {
        DBFile dbFile = cachedFiles.get(filename);
        logger.debug(String.format("Requested file %s is%s in file-cache.", filename, (dbFile != null ? "" : " NOT")));
        return dbFile;
    }

    @Override
    public void addFile(DBFile dbFile) {
        if (dbFile == null) {
            throw new IllegalArgumentException("dbFile cannot be null");
        }
        String filename = dbFile.getDataFile().getName();
        if (cachedFiles.containsKey(filename)) {
            throw new IllegalStateException("File cache already contains file " + filename);
        }
        logger.debug("Adding file {} to file-cache.", filename);
        cachedFiles.put(filename, dbFile);
        if (cachedFiles.size() > PropertiesUtil.getInt("thr.open.files", 20)) {
            logger.warn("{} files is open. ", cachedFiles.size());
        }
    }

    @Override
    public void pinPage(DBPage dbPage) {
        // 将此page定住知道此session结束
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);

        // 将pinnedPage添加到pinnedPages
        if (pinnedPages.add(pp)) {
            dbPage.incPinCount();
            logger.debug("Session %d is pinning page [{},{}].  New pin-count is {}.", sessionID, dbPage.getDBFile(),
                    dbPage.getPageNo(), dbPage.getPinCount());
        }

        // 将pinnedPage添加到pinnedPagesBySessionID，方便查找
        Set<PinnedPageInfo> pinnedBySession = pinnedPagesBySessionID.get(sessionID);
        if (pinnedBySession == null) {
            pinnedBySession = new HashSet();
            pinnedPagesBySessionID.put(sessionID, pinnedBySession);
        }
        pinnedBySession.add(pp);
    }

    @Override
    public void unpinPage(DBPage dbPage) {
        // If the page is pinned by the session then unpin it.
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);

        // 从 pinned page set中移除
        if (pinnedPages.remove(pp)) {
            dbPage.decPinCount();
            logger.debug("Session {} is unpinning page [{},{}].  New pin-count is {}.", sessionID, dbPage.getDBFile(),
                    dbPage.getPageNo(), dbPage.getPinCount());
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
     * unpin 此session所有的page,一般是在事务结束时执行，便于这些page能被顺利的从buffer中清除
     */
    @Override
    public void unpinAllPages() {
        int sessionID = SessionState.get().getSessionID();

        Set<PinnedPageInfo> pinnedBySession = pinnedPagesBySessionID.remove(sessionID);
        if (pinnedBySession == null) {
            return;
        }
        for (PinnedPageInfo pp : pinnedBySession) {
            DBPage dbPage = pp.dbPage;
            pinnedPages.remove(pp);
            dbPage.decPinCount();
            logger.debug("Session %d is unpinning page " + "[{},{}].  New pin-count is {}.", sessionID,
                    dbPage.getDBFile(), dbPage.getPageNo(), dbPage.getPinCount());
        }
    }

    @Override
    public DBPage getPage(DBFile dbFile, int pageNo) {
        DBPage dbPage = cachedPages.get(new CachedPageInfo(dbFile, pageNo));
        logger.debug("Requested page [{},{}] is {} in page-cache.", dbFile, pageNo, (dbPage != null ? "" : " NOT"));
        if (dbPage != null) {
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
    @Override
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
     * 确保拥有bytesRequired byte的空间
     * 
     * @param bytesRequired 空间大小
     * @throws IOException 文件操作异常
     */
    private void ensureSpaceAvailable(int bytesRequired) throws IOException {

        if (bytesRequired + totalBytesCached <= maxCacheSize) {
            // 空间已足够
            return;
        }

        // 空间不够时，移除部分页
        List<DBPage> dirtyPages = new ArrayList<>();

        if (!cachedPages.isEmpty()) {
            Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();
            while (entries.hasNext() && bytesRequired + totalBytesCached > maxCacheSize) {
                // 空间不够时
                Map.Entry<CachedPageInfo, DBPage> entry = entries.next();
                DBPage oldPage = entry.getValue();
                if (oldPage.isPinned()) {
                    // 被定住了，就不清除
                    continue;
                }
                logger.debug("Evicting page [{},{}] from page-cache to make room.", oldPage.getDBFile(),
                        oldPage.getPageNo());
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // 如果不是脏页，直接将其注销
                if (oldPage.isDirty()) {
                    dirtyPages.add(oldPage);
                } else {
                    oldPage.invalidate();
                }
            }
        }

        // 将脏页刷到磁盘，并注销这些页
        writeDirtyPages(dirtyPages, true);
        if (bytesRequired + totalBytesCached > maxCacheSize) {
            logger.warn("Buffer manager is currently using too much space.");
        }
    }

    /**
     * 将dbFile中指定范围内的脏数据页刷出到磁盘
     * 
     * @param dbFile 指定文件
     * @param minPageNo 指定页范围
     * @param maxPageNo 指定页范围
     * @param sync 同步到磁盘{@link FileManager#syncDBFile(DBFile)}
     * @throws IOException 文件操作异常
     */
    @Override
    public void writeDBFile(DBFile dbFile, int minPageNo, int maxPageNo, boolean sync) throws IOException {
        logger.info("Writing all dirty pages for file {} to disk {}.", dbFile, (sync ? " (with sync)" : ""));

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();
        List<DBPage> dirtyPages = new ArrayList<>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();
            CachedPageInfo info = entry.getKey();

            // 找到指定文件
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();
                if (!oldPage.isDirty()) {
                    // 不是脏页就跳过
                    continue;
                }
                int pageNo = oldPage.getPageNo();
                if (pageNo < minPageNo || pageNo > maxPageNo) {
                    // 不是指定范围内的页
                    continue;
                }
                logger.debug("Saving page [{},{}] to disk.", oldPage.getDBFile(), oldPage.getPageNo());
                dirtyPages.add(oldPage);
            }
        }

        // 将脏页落盘
        writeDirtyPages(dirtyPages, false);
        if (sync) {
            logger.debug("Syncing file " + dbFile);
            // 真正写到了磁盘
            fileManager.syncDBFile(dbFile);
        }
    }

    /**
     * dbFile中所有脏页刷出到磁盘
     * 
     * @param dbFile 数据文件
     * @param sync 同步
     * @throws IOException 文件操作异常
     */
    @Override
    public void writeDBFile(DBFile dbFile, boolean sync) throws IOException {
        writeDBFile(dbFile, 0, Integer.MAX_VALUE, sync);
    }

    /**
     * 将所有的缓存页刷出到磁盘
     * 
     * @param sync 同步到磁盘
     * @throws IOException 文件异常
     */
    @Override
    public void writeAll(boolean sync) throws IOException {
        logger.info("Writing ALL dirty pages in the Buffer Manager to disk.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();
        List<DBPage> dirtyPages = new ArrayList<>();
        Set<DBFile> dirtyFiles = new HashSet<>();
        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();
            DBPage oldPage = entry.getValue();
            if (!oldPage.isDirty()) {
                continue;
            }
            DBFile dbFile = oldPage.getDBFile();
            DBFileType type = dbFile.getType();
            if (type != DBFileType.WRITE_AHEAD_LOG_FILE && type != DBFileType.TXNSTATE_FILE) {
                dirtyFiles.add(oldPage.getDBFile());
            }
            logger.debug("Saving page [{},{}] to disk.", dbFile, oldPage.getPageNo());
            dirtyPages.add(oldPage);
        }

        writeDirtyPages(dirtyPages, false);
        if (sync) {
            logger.debug("Synchronizing all files containing dirty pages to disk.");
            for (DBFile dbFile : dirtyFiles) {
                fileManager.syncDBFile(dbFile);
            }
        }
    }

    /**
     * 将dbFile的缓存页刷出到磁盘，并减少统计数量。
     * 
     * @param dbFile 指定文件
     * @throws IOException 文件操作异常
     */
    @Override
    public void flushDBFile(DBFile dbFile) throws IOException {
        logger.info("Flushing all pages for file " + dbFile + " from the Buffer Manager.");
        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();
        List<DBPage> dirtyPages = new ArrayList<>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();
            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();
                logger.debug("Evicting page [{},{}] from page-cache.", oldPage.getDBFile(), oldPage.getPageNo());
                // 从缓存中移除
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                if (oldPage.isDirty()) {
                    dirtyPages.add(oldPage);
                } else {
                    oldPage.invalidate();
                }
            }
        }
        writeDirtyPages(dirtyPages, true);
    }

    /**
     * 将所有的缓存页刷出到磁盘
     * 
     * @throws IOException 文件操作异常
     */
    @Override
    public void flushAll() throws IOException {
        logger.info("Flushing ALL database pages from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries = cachedPages.entrySet().iterator();
        List<DBPage> dirtyPages = new ArrayList<>();
        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();
            DBPage oldPage = entry.getValue();
            logger.debug("Evicting page [{},{}] from page-cache.", oldPage.getDBFile(), oldPage.getPageNo());

            // 从缓存中移除
            entries.remove();
            totalBytesCached -= oldPage.getPageSize();

            // 脏页要刷出到磁盘后再注销
            if (oldPage.isDirty()) {
                dirtyPages.add(oldPage);
            } else {
                oldPage.invalidate();
            }
        }
        writeDirtyPages(dirtyPages, true);
    }

    private void writeDirtyPages(List<DBPage> dirtyPages, boolean invalidate) throws IOException {
        if (!dirtyPages.isEmpty()) {
            // 将脏页刷出到磁盘
            for (DBPage dbPage : dirtyPages) {
                fileManager.saveDBPage(dbPage);
                if (invalidate) {
                    dbPage.invalidate();
                }
            }
        }
    }

    /**
     * 将dbFile从缓存中移除，先将所有缓存页刷出
     * 
     * @param dbFile dbFile
     * @throws IOException 文件操作异常
     */
    @Override
    public void removeDBFile(DBFile dbFile) throws IOException {
        logger.info("Removing DBFile " + dbFile + " from buffer manager");
        flushDBFile(dbFile);
        cachedFiles.remove(dbFile.getDataFile().getName());
    }

    /**
     * 移除缓存中所有的文件，一般是在系统关闭时调用。
     * 
     * @return 被移除的List<DBFile>
     * @throws IOException 文件操作异常
     */
    @Override
    public List<DBFile> removeAll() throws IOException {
        logger.info("Removing ALL DBFiles from buffer manager");
        flushAll();
        List<DBFile> dbFiles = new ArrayList<>(cachedFiles.values());
        cachedFiles.clear();
        return dbFiles;
    }

    /**
     * 封装了数据文件和页号
     */
    private static class CachedPageInfo {
        public DBFile dbFile;

        public int pageNo;

        CachedPageInfo(DBFile dbFile, int pageNo) {
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
        int sessionID;

        public DBPage dbPage;

        PinnedPageInfo(int sessionID, DBPage dbPage) {
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
}

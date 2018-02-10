package com.bow.lab.storage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bow.lab.indexes.IndexFileInfo;
import com.bow.lab.indexes.IndexManager;
import com.bow.maple.relations.ColumnIndexes;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.relations.Tuple;
import com.bow.maple.server.EventDispatchException;
import com.bow.maple.server.RowEventListener;
import com.bow.maple.storage.PageTuple;
import com.bow.maple.storage.StorageManager;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.util.ExtensionLoader;

/**
 * This class implements the {@link RowEventListener} interface to make sure
 * that all indexes on an updated table are kept up-to-date. This handler is
 * installed by the {@link StorageManager#init} setup method.
 */
public class IndexUpdater implements RowEventListener {

    private static Logger logger = LoggerFactory.getLogger(IndexUpdater.class);

    /**
     * A cached reference to the storage manager since we use it a lot in this
     * class.
     */
    private IStorageService storageService = ExtensionLoader.getExtensionLoader(IStorageService.class).getExtension();

    private IIndexService indexService = ExtensionLoader.getExtensionLoader(IIndexService.class).getExtension();

    @Override
    public void beforeRowInserted(TableFileInfo tblFileInfo, Tuple newValues) {
        // Ignore.
    }

    @Override
    public void afterRowInserted(TableFileInfo tblFileInfo, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException("newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowUpdated(TableFileInfo tblFileInfo, Tuple oldTuple, Tuple newValues) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException("oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowUpdated(TableFileInfo tblFileInfo, Tuple oldValues, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException("newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowDeleted(TableFileInfo tblFileInfo, Tuple oldTuple) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException("oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowDeleted(TableFileInfo tblFileInfo, Tuple oldValues) {
        // Ignore.
    }

    /**
     * This helper method handles the case when a tuple is being added to the
     * table, after the row has already been added to the table. All indexes on
     * the table are updated to include the new row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the new tuple that was inserted into the table
     */
    private void addRowToIndexes(TableFileInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Adding tuple to indexes for table " + tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnIndexes indexDef : schema.getIndexes().values()) {

            logger.debug("Adding tuple to index " + indexDef.getIndexName());

            try {
                IndexFileInfo idxFileInfo = indexService.openIndex(tblFileInfo, indexDef.getIndexName());

                IndexManager indexManager = idxFileInfo.getIndexManager();
                indexManager.addTuple(idxFileInfo, ptup);
            } catch (IOException e) {
                throw new EventDispatchException(
                        "Couldn't update index " + indexDef.getIndexName() + " for table " + tblFileInfo.getTableName(),
                        e);
            }
        }
    }

    /**
     * This helper method handles the case when a tuple is being removed from
     * the table, before the row has actually been removed from the table. All
     * indexes on the table are updated to remove the row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the tuple about to be removed from the table
     */
    private void removeRowFromIndexes(TableFileInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Removing tuple from indexes for table " + tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnIndexes indexDef : schema.getIndexes().values()) {
            logger.debug("Removing tuple from index " + indexDef.getIndexName());

            try {
                IndexFileInfo idxFileInfo = indexService.openIndex(tblFileInfo, indexDef.getIndexName());

                IndexManager indexManager = idxFileInfo.getIndexManager();
                indexManager.deleteTuple(idxFileInfo, ptup);
            } catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " + indexDef.getIndexName() + " for table "
                        + tblFileInfo.getTableName());
            }
        }
    }
}

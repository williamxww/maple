package com.bow.lab.storage;

import com.bow.maple.commands.Command;
import com.bow.maple.commands.ExecutionException;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.TableSchema;
import com.bow.maple.storage.DBFileType;
import com.bow.maple.storage.StorageManager;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.util.ExtensionLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wwxiang
 * @since 2017/11/8.
 */
public class CreateTableCmd extends Command {

    private String tableName;

    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();

    public CreateTableCmd() {
        super(Type.DDL);
    }

    private ITableService tableService = ExtensionLoader.getExtensionLoader(ITableService.class).getExtension();

    @Override
    public void execute() throws ExecutionException {
        TableFileInfo tblFileInfo = new TableFileInfo(tableName);
        tblFileInfo.setFileType(DBFileType.FRM_FILE);
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnInfo colInfo : columnInfos) {
            try {
                schema.addColumnInfo(colInfo);
            } catch (IllegalArgumentException iae) {
                throw new ExecutionException("Duplicate or invalid column \"" + colInfo.getName() + "\".", iae);
            }
        }
        try {
            tableService.createTable(tblFileInfo);
        } catch (IOException ioe) {
            throw new ExecutionException("Can't create table "+tableName, ioe);
        }
    }

    public void addColumn(ColumnInfo colInfo) {
        this.columnInfos.add(colInfo);
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnInfo> getColumnInfos() {
        return columnInfos;
    }

    public void setColumnInfos(List<ColumnInfo> columnInfos) {
        this.columnInfos = columnInfos;
    }

    public void setTableService(ITableService tableService) {
        this.tableService = tableService;
    }
}

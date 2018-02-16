package com.bow.maple.client.swingui;

import com.bow.maple.expressions.LiteralTuple;
import com.bow.maple.relations.Schema;

import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/27/11
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class TupleTableModel extends AbstractTableModel {
    
    private Schema schema;


    private ArrayList<LiteralTuple> tuples = new ArrayList<LiteralTuple>();


    public void setSchema(Schema schema) {
        this.schema = schema;
        tuples.clear();

        fireTableStructureChanged();
    }


    public void addTuple(LiteralTuple tuple) {
        int row = tuples.size();
        tuples.add(tuple);

        fireTableRowsInserted(row, row);
    }


    public String getColumnName(int column) {
        return schema.getColumnInfo(column).getColumnName().toString();
    }


    @Override
    public int getRowCount() {
        return tuples.size();
    }


    @Override
    public int getColumnCount() {
        if (schema == null)
            return 0;

        return schema.numColumns();
    }


    @Override
    public Object getValueAt(int row, int column) {
        LiteralTuple t = tuples.get(row);

        Object value = t.getColumnValue(column);
        if (value == null)
            value = "NULL";

        return value;
    }
}

package com.bow.lab.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bow.maple.commands.DropTableCommand;
import com.bow.maple.commands.InsertCommand;
import com.bow.maple.expressions.LiteralValue;
import com.bow.maple.server.EventDispatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bow.maple.commands.CreateTableCommand;
import com.bow.maple.commands.FromClause;
import com.bow.maple.commands.SelectClause;
import com.bow.maple.commands.SelectCommand;
import com.bow.maple.commands.SelectValue;
import com.bow.maple.expressions.ColumnName;
import com.bow.maple.expressions.ColumnValue;
import com.bow.maple.expressions.Expression;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.storage.StorageManager;

/**
 * @author vv
 * @since 2017/10/22.
 */
public class CreateTableTest {

    private String tableName = "TEST";

    @Before
    public void setup() throws IOException {
        StorageManager.init();
    }

    @After
    public void destroy() throws IOException {
        StorageManager.shutdown();
    }

    @Test
    public void create() throws Exception {

        CreateTableCommand command = new CreateTableCommand(tableName, false, false);
        ColumnType strType = new ColumnType(SQLDataType.VARCHAR);
        ColumnType intType = new ColumnType(SQLDataType.INTEGER);
        strType.setLength(10);
        ColumnInfo id = new ColumnInfo("ID", tableName, intType);
        ColumnInfo name = new ColumnInfo("NAME", tableName, strType);
        command.addColumn(id);
        command.addColumn(name);

        // 开启事务并执行命令
        EventDispatcher eventDispatch = EventDispatcher.getInstance();
        eventDispatch.fireBeforeCommandExecuted(command);
        command.execute();
        eventDispatch.fireAfterCommandExecuted(command);
    }

    @Test
    public void insert()throws Exception {
        List<String> names = new ArrayList<>();
        names.add("ID");
        names.add("NAME");
        List<Expression> expressions = new ArrayList<>();
        LiteralValue id = new LiteralValue("1");
        LiteralValue name = new LiteralValue("g");
        expressions.add(id);
        expressions.add(name);
        InsertCommand command = new InsertCommand(tableName, names, expressions);

        // 开启事务并执行命令
        EventDispatcher eventDispatch = EventDispatcher.getInstance();
        eventDispatch.fireBeforeCommandExecuted(command);
        command.execute();
        eventDispatch.fireAfterCommandExecuted(command);
    }

    @Test
    public void select() throws Exception {
        ColumnName idCn = new ColumnName("id");
        Expression idCv = new ColumnValue(idCn);
        SelectValue idVal = new SelectValue(idCv, null);
        SelectClause clause = new SelectClause();
        clause.addSelectValue(idVal);
        FromClause from = new FromClause(tableName, null);
        clause.setFromClause(from);
        SelectCommand command = new SelectCommand(clause);
        command.execute();
    }

    @Test
    public void drop() throws Exception {
        DropTableCommand command = new DropTableCommand(tableName,true);
        command.execute();
    }
}

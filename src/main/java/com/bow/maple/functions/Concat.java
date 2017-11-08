package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.Schema;
import com.bow.maple.expressions.ExpressionException;
import com.bow.maple.relations.ColumnType;


/**
 * Concatenates arguments as strings.  If any of the arguments is NULL, returns
 * NULL.
 * 
 * @author emil
 */
public class Concat extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        ColumnType colType = new ColumnType(SQLDataType.VARCHAR);
        return new ColumnInfo(colType);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call CONCAT on " +
                args.size() + " arguments");
        }
        
        StringBuilder buf = new StringBuilder();
        
        for (Expression arg : args) {
            Object val = arg.evaluate(env);

            if (val == null)
                return null;

            buf.append(TypeConverter.getStringValue(val));
        }
        
        return buf.toString();
    }
}

package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.ExpressionException;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.Schema;


/**
 * Computes the square root of a single argument. Returns NULL if argument
 * is NULL.
 * 
 * @author emil
 */
public class Sqrt extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        ColumnType colType = new ColumnType(SQLDataType.DOUBLE);
        return new ColumnInfo(colType);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call SQRT on " + args.size() 
                    + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);
        
        if (argVal == null)
            return null;

        return Math.sqrt(TypeConverter.getDoubleValue(argVal));
    }
}

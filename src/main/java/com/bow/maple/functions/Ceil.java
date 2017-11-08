package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.Schema;
import com.bow.maple.expressions.ExpressionException;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;


/**
 * Computes the smallest whole number larger than the argument.  Returns NULL if 
 * argument is NULL.  The result is always a double-precision number, even
 * though it is a whole number, since this is what {@link Math#ceil} returns.
 */
public class Ceil extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        // We could try to return an int or long, but Math.ceil() always returns
        // a double, so we'll just return a double too.
        ColumnType colType = new ColumnType(SQLDataType.DOUBLE);
        return new ColumnInfo(colType);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call CEIL on " + args.size() 
                    + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);

        if (argVal == null)
            return null;

        return Math.ceil(TypeConverter.getDoubleValue(argVal));
    }
}

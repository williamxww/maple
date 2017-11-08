package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.Schema;

import com.bow.maple.expressions.ExpressionException;


/**
 * Computes the whole number that is closest to the argument.  Returns NULL if
 * argument is NULL.
 * 
 * @author emil
 */
public class Round extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call ROUND on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        ColumnInfo argInfo = args.get(0).getColumnInfo(schema);
        SQLDataType argBaseType = argInfo.getType().getBaseType();

        SQLDataType retBaseType = SQLDataType.BIGINT;
        if (argBaseType == SQLDataType.FLOAT)
            retBaseType = SQLDataType.INTEGER;

        return new ColumnInfo(new ColumnType(retBaseType));
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call ROUND on " + args.size() 
                    + " arguments");
        }

        Object argVal = args.get(0).evaluate(env);
        
        if (argVal == null)
            return null;

        // If the argument is a float, return an int.
        if (argVal instanceof Float)
            return Math.round((Float) argVal);

        // Otherwise, return a long.
        return Math.round(TypeConverter.getDoubleValue(argVal));
    }
}

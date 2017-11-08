package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.ExpressionException;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.ColumnType;
import com.bow.maple.relations.SQLDataType;
import com.bow.maple.relations.Schema;


/**
 * Computes the absolute value of a single argument. Returns NULL if argument
 * is NULL.
 * 
 * @author emil
 */
public class Abs extends Function {

    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call ABS on " + args.size() +
                " arguments");
        }

        // Return the type of the first argument.
        ColumnInfo argInfo = args.get(0).getColumnInfo(schema);
        SQLDataType argBaseType = argInfo.getType().getBaseType();

        if (SQLDataType.isNumber(argBaseType))
            return argInfo;

        return new ColumnInfo(new ColumnType(SQLDataType.DOUBLE));
    }


    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 1) {
            throw new ExpressionException("Cannot call ABS on " + args.size() +
                " arguments");
        }
        
        Object argVal = args.get(0).evaluate(env);

        // A NULL input results in a NULL output.
        if (argVal == null)
            return null;

        if (argVal instanceof Byte) {
            return (byte) Math.abs((Byte) argVal);
        }
        else if (argVal instanceof Short) {
            return (short) Math.abs((Short) argVal);
        }
        else if (argVal instanceof Integer) {
            return Math.abs((Integer) argVal);
        }
        else if (argVal instanceof Long) {
            return Math.abs((Long) argVal);
        }
        else if (argVal instanceof Float) {
            return Math.abs((Float) argVal);
        }

        // If we got here then the argument wasn't any of the standard numeric
        // types.  Try to cast the input to a double-precision value!
        return Math.abs(TypeConverter.getDoubleValue(argVal));
    }
}

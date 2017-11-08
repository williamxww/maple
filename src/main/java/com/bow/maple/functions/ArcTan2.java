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
 * Computes the arc-tangent of two arguments.  Returns NULL if arguments are NULL.
 * 
 * @author emil
 */
public class ArcTan2 extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        ColumnType colType = new ColumnType(SQLDataType.DOUBLE);
        return new ColumnInfo(colType);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call ATAN2 on " +
                args.size() + " arguments");
        }

        Object argVal1 = args.get(0).evaluate(env);
        Object argVal2 = args.get(1).evaluate(env);

        if (argVal1 == null || argVal2 == null)
            return null;

        return Math.atan2(TypeConverter.getDoubleValue(argVal1),
                          TypeConverter.getDoubleValue(argVal2));
    }
}

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
 * Returns the first argument, raised to the second argument power. If any
 * of the arguments is {@code NULL}, returns {@code NULL}.
 * 
 * @author emil
 */
public class Pow extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        ColumnType colType = new ColumnType(SQLDataType.DOUBLE);
        return new ColumnInfo(colType);
    }
    
    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call POW on " + args.size() +
                " arguments");
        }

        Object argVal1 = args.get(0).evaluate(env);
        Object argVal2 = args.get(1).evaluate(env);
   
        if (argVal1 == null || argVal2 == null)
            return null;

        return Math.pow(TypeConverter.getDoubleValue(argVal1),
                        TypeConverter.getDoubleValue(argVal2));
    }
}

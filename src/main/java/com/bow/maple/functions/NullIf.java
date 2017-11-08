package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.ExpressionException;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Schema;


/**
 * Implements {@code NULLIF(cond, expr)}. Returns {@code NULL} if the first
 * argument is {@code TRUE}, else returns the second argument.
 * 
 * @author emil
 */
public class NullIf extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call NULLIF on " +
                args.size() + " arguments");
        }

        // Return the type of the second argument.
        return args.get(1).getColumnInfo(schema);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call NULLIF on " + args.size() 
                    + " arguments");
        }

        Object condVal = args.get(0).evaluate(env);

        if (condVal != null && TypeConverter.getBooleanValue(condVal))
            return null;
        else
            return args.get(1).evaluate(env);
    }

}

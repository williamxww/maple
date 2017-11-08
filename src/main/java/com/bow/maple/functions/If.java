package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.TypeConverter;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Schema;
import com.bow.maple.expressions.ExpressionException;


/**
 * Implements {@code IF (cond, expr1, expr2)}. If the first argument is
 * {@code TRUE}, returns {@code expr1}, else returns {@code expr2}.
 *
 * @author emil
 */
public class If extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 3) {
            throw new ExpressionException("Cannot call IF on " + args.size() +
                " arguments");
        }

        // Return the type of the second argument.
        return args.get(1).getColumnInfo(schema);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 3) {
            throw new ExpressionException("Cannot call IF on " + args.size() +
                " arguments");
        }

        Object condVal = args.get(0).evaluate(env);
        
        if (condVal != null && TypeConverter.getBooleanValue(condVal))
            return args.get(1).evaluate(env);
        else
            return args.get(2).evaluate(env);
    }
}

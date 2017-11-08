package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.ExpressionException;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Schema;


/**
 * Returns the greatest value of all arguments.  <ttNULL</tt> arguments are
 * ignored; the function only produces <tt>NULL</tt> if all inputs are
 * <tt>NULL</tt>.  The type of the result is the type of the first argument.
 */
public class Greatest extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call GREATEST on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        return args.get(0).getColumnInfo(schema);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call GREATEST on " +
                args.size() + " arguments");
        }
        
        Comparable greatest = null;
        for (Expression arg : args) {
            Comparable val = (Comparable) arg.evaluate(env);
            if (val == null)
                continue;

            if (greatest == null || val.compareTo(greatest) > 0)
                greatest = val;
        }

        return greatest;
    }
}

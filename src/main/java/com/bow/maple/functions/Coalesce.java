package com.bow.maple.functions;


import java.util.List;

import com.bow.maple.expressions.Expression;
import com.bow.maple.expressions.Environment;
import com.bow.maple.expressions.ExpressionException;

import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Schema;


/**
 * Returns the first non-<tt>NULL</tt> argument.  The function's return-type is
 * reported to be whatever the first argument's type is.
 * 
 * @author emil
 */
public class Coalesce extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call COALESCE on " +
                args.size() + " arguments");
        }

        // Return the type of the first argument.
        return args.get(0).getColumnInfo(schema);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 1) {
            throw new ExpressionException("Cannot call COALESCE on " +
                args.size() + " arguments");
        }
        
        for (Expression arg : args) {
            Object val = arg.evaluate(env);
            
            if (val != null)
                return val;
        }
        
        return null;
    }
}

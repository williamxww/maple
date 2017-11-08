package com.bow.maple.expressions;


import java.util.Collection;

import com.bow.maple.commands.SelectClause;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Schema;
import com.bow.maple.relations.SchemaNameException;


/**
 * This class represents a scalar subquery embedded in another query's
 * predicate.
 */
public class ScalarSubquery extends Expression {

    /** The subquery that is evaluated to a scalar value. */
    private SelectClause subquery;


    public ScalarSubquery(SelectClause subquery) {
        if (subquery == null)
            throw new IllegalArgumentException("subquery cannot be null");

        this.subquery = subquery;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // TODO:  IMPLEMENT
        return null;
    }


    public Object evaluate(Environment env) {
        // TODO:  IMPLEMENT
        return null;
    }


    public boolean hasSymbols() {
        // TODO:  IMPLEMENT
        return false;
    }


    // Pick up the javadocs from the Expression class.
    public void getAllSymbols(Collection<ColumnName> symbols) {
        // TODO:  IMPLEMENT
    }


    @Override
    public String toString() {
        return "(" + subquery.toString() + ")";
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @design This function operates correctly on the assumption that all
     *         supported types override Object.equals().
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ScalarSubquery) {
            ScalarSubquery other = (ScalarSubquery) obj;
            return subquery.equals(other.subquery);
        }
        return false;
    }


    /**
     * Computes the hash-code of this scalar subquery expression.
     */
    @Override
    public int hashCode() {
        return subquery.hashCode();
    }


    /**
     * Creates a copy of expression.
     *
     * @design The reference of the internal value is simply copied since the
     *         value types are all immutable.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ScalarSubquery expr = (ScalarSubquery) super.clone();

        // We don't clone SelectClause expressions at this point since they are
        // currently not cloneable.

        return expr;
    }
}


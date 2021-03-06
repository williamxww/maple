package com.bow.maple.commands;


import java.io.IOException;

import com.bow.maple.qeval.PlannerFactory;
import com.bow.maple.qeval.TuplePrinter;
import com.bow.maple.qeval.TupleProcessor;
import org.apache.log4j.Logger;

import com.bow.maple.client.SessionState;

import com.bow.maple.qeval.Planner;

import com.bow.maple.relations.Schema;
import com.bow.maple.relations.SchemaNameException;


/**
 * This command object represents a top-level <tt>SELECT</tt> command issued
 * against the database.  The query itself is represented by the
 * {@link SelectClause} class, particularly because a <tt>SELECT</tt> statement
 * can itself contain other <tt>SELECT</tt> statements.
 *
 * @see SelectClause
 */
public class SelectCommand extends QueryCommand {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SelectCommand.class);


    /**
     * This object contains all the details of the top-level select clause,
     * including any subqueries, that is going to be evaluated.
     */
    private SelectClause selClause;


    private TupleProcessor tupleProcessor;


    public SelectCommand(SelectClause selClause) {
        super(QueryCommand.Type.SELECT);

        if (selClause == null)
            throw new NullPointerException("selClause cannot be null");

        this.selClause = selClause;
    }


    /**
     * Returns the root select-clause for this select command.
     *
     * @return the root select-clause for this select command
     */
    public SelectClause getSelectClause() {
        return selClause;
    }


    /**
     * Prepares the <tt>SELECT</tt> statement for evaluation by analyzing the
     * schema details of the statement, and then preparing an execution plan
     * for the statement.
     */
    protected void prepareQueryPlan() throws IOException, SchemaNameException {
        Schema resultSchema = selClause.computeSchema();
        logger.debug("Prepared SelectClause:\n" + selClause);
        logger.debug("Result schema:  " + resultSchema);

        // Create a plan for executing the SQL query.
        Planner planner = PlannerFactory.getPlanner();
        plan = planner.makePlan(selClause);
    }
    
    
    public void setTupleProcessor(TupleProcessor tupleProcessor) {
        this.tupleProcessor = tupleProcessor;
    }


    protected TupleProcessor getTupleProcessor() {
        if (tupleProcessor == null)
            tupleProcessor = new TuplePrinter(SessionState.get().getOutputStream());

        return tupleProcessor;
    }


    @Override
    public String toString() {
        return "SelectCommand[" + selClause + "]";
    }
}


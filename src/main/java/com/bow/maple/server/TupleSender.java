package com.bow.maple.server;


import java.io.IOException;
import java.io.ObjectOutputStream;

import com.bow.maple.relations.Tuple;
import com.bow.maple.expressions.LiteralTuple;
import com.bow.maple.qeval.TupleProcessor;
import com.bow.maple.relations.Schema;


/**
 * This implementation of the tuple-processor interface sends the schema and
 * tuples produced by the <tt>SELECT</tt> statement over an
 * {@link ObjectOutputStream}.
 */
public class TupleSender implements TupleProcessor {

    private ObjectOutputStream objectOutput;


    public TupleSender(ObjectOutputStream objectOutput) {
        if (objectOutput == null)
            throw new IllegalArgumentException("objectOutput cannot be null");

        this.objectOutput = objectOutput;
    }


    public void setSchema(Schema schema) throws IOException {
        // If the incoming schema-object is not of type Schema then make a copy
        // of it and send it over.
        if (!schema.getClass().equals(Schema.class))
            schema = new Schema(schema);

        objectOutput.writeObject(schema);
    }


    public void process(Tuple tuple) throws IOException {
        LiteralTuple tupLit;
        
        if (!(tuple instanceof LiteralTuple))
            tupLit = new LiteralTuple(tuple);
        else
            tupLit = (LiteralTuple) tuple;
        
        objectOutput.writeObject(tupLit);
    }
}

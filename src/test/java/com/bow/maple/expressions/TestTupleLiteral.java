package com.bow.maple.expressions;


/**
 * This test class exercises the functionality of the
 * {@link LiteralTuple} class.
 **/
public class TestTupleLiteral {

    public void testSimpleCtors() {
        LiteralTuple tuple;

        tuple = new LiteralTuple();
        assert tuple.getColumnCount() == 0;

        tuple = new LiteralTuple(5);
        assert tuple.getColumnCount() == 5;
        for (int i = 0; i < 5; i++) {
            assert tuple.getColumnValue(i) == null;
            assert tuple.isNullValue(i);
        }
    }


    /** This test exercises the <code>addValue()</code> methods. */
    public void testAddValues() {
        LiteralTuple tuple = new LiteralTuple();

        tuple.addValue(new Integer(3));
        tuple.addValue("hello");
        tuple.addValue(new Double(2.1));
        tuple.addValue(null);
        tuple.addValue(new Long(-6L));

        assert tuple.getColumnCount() == 5;

        assert !tuple.isNullValue(0);
        assert !tuple.isNullValue(1);
        assert !tuple.isNullValue(2);
        assert  tuple.isNullValue(3);
        assert !tuple.isNullValue(4);

        assert tuple.getColumnValue(0).equals(new Integer(3));
        assert tuple.getColumnValue(1).equals("hello");
        assert tuple.getColumnValue(2).equals(new Double(2.1));
        assert tuple.getColumnValue(3) == null;
        assert tuple.getColumnValue(4).equals(new Long(-6L));
    }


    /** This test exercises the constructor that duplicates a tuple. */
    public void testTupleCtor() {
        LiteralTuple tuple1 = new LiteralTuple();
        tuple1.addValue(new Integer(5));
        tuple1.addValue(null);
        tuple1.addValue("hello");

        LiteralTuple tuple2 = new LiteralTuple(tuple1);
        assert(tuple2.getColumnCount() == 3);

        assert(tuple2.getColumnValue(0).equals(new Integer(5)));

        assert(tuple2.isNullValue(1));
        assert(tuple2.getColumnValue(1) == null);

        assert(tuple2.getColumnValue(2).equals("hello"));
    }


    /** This test exercises the <tt>appendTuple()</tt> method. */
    public void testAppendTuple() {
        LiteralTuple tuple1 = new LiteralTuple();
        tuple1.addValue(new Integer(5));
        tuple1.addValue(null);
        tuple1.addValue("hello");

        LiteralTuple tuple2 = new LiteralTuple();
        tuple2.appendTuple(tuple1);

        assert(tuple2.getColumnCount() == 3);

        assert(tuple2.getColumnValue(0).equals(new Integer(5)));

        assert(tuple2.isNullValue(1));
        assert(tuple2.getColumnValue(1) == null);

        assert(tuple2.getColumnValue(2).equals("hello"));
    }
}


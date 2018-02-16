package com.bow.maple.sql;



import com.bow.maple.expressions.LiteralTuple;
import com.bow.maple.server.CommandResult;
import com.bow.maple.server.NanoDBServer;


/**
 * This class exercises the database with some simple <tt>SELECT</tt>
 * statements against a single table, to see if simple selects and
 * predicates work properly.
 */
public class TestSelectProject extends SqlTestCase {

    public TestSelectProject() {
        super("setup_testSelectProject");
    }


    /**
     * This test performs some simple projects that reorder the columns,
     * to see if the queries produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testProjectReorderCols() throws Throwable {
        // Columns c, a
        LiteralTuple[] expected1 = {
            new LiteralTuple(  10, 1),
            new LiteralTuple(  20, 2),
            new LiteralTuple(  30, 3),
            new LiteralTuple(null, 4),
            new LiteralTuple(  40, 5),
            new LiteralTuple(  50, 6)
        };

        // Columns c, b
        LiteralTuple[] expected2 = {
            new LiteralTuple(  10,    "red"),
            new LiteralTuple(  20, "orange"),
            new LiteralTuple(  30,     null),
            new LiteralTuple(null,  "green"),
            new LiteralTuple(  40, "yellow"),
            new LiteralTuple(  50,   "blue")
        };

        CommandResult result;

        result = NanoDBServer.doCommand(
            "SELECT c, a FROM test_select_project", true);
        assert checkUnorderedResults(expected1, result);

        result = NanoDBServer.doCommand(
            "SELECT c, b FROM test_select_project", true);
        assert checkUnorderedResults(expected2, result);
    }


    /**
     * This test performs some simple projects that perform arithmetic on the
     * column values, to see if the queries produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testProjectMath() throws Throwable {
        // Columns a - 10 as am, c * 3 as cm
        LiteralTuple[] expected = {
            new LiteralTuple(-9,   30),
            new LiteralTuple(-8,   60),
            new LiteralTuple(-7,   90),
            new LiteralTuple(-6, null),
            new LiteralTuple(-5,  120),
            new LiteralTuple(-4,  150)
        };

        CommandResult result;

        result = NanoDBServer.doCommand(
            "SELECT a - 10 AS am, c * 3 AS cm FROM test_select_project", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs some simple projects that perform arithmetic on the
     * column values, along with a select predicate, to see if the queries
     * produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testSelectProjectMath() throws Throwable {
        // Columns b, a - 10 as am, c * 3 as cm
        LiteralTuple[] expected = {
            new LiteralTuple(    null, -7,   90),
            new LiteralTuple("yellow", -5,  120)
        };

        CommandResult result;

        result = NanoDBServer.doCommand(
            "SELECT b, a - 10 AS am, c * 3 AS cm FROM test_select_project " +
            "WHERE a > 2 AND c < 45", true);
        assert checkUnorderedResults(expected, result);
    }
}

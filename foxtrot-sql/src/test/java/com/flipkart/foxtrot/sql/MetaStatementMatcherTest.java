package com.flipkart.foxtrot.sql;

import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatement;
import com.flipkart.foxtrot.sql.extendedsql.desc.Describe;
import com.flipkart.foxtrot.sql.extendedsql.showtables.ShowTables;
import org.junit.Assert;
import org.junit.Test;

public class MetaStatementMatcherTest {
    private MetaStatementMatcher metaStatementMatcher = new MetaStatementMatcher();

    @Test
    public void testParseDescPositive1() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("desc europa");
        Assert.assertNotNull(extendedSqlStatement);
        Assert.assertTrue(extendedSqlStatement instanceof Describe);
        Describe describe = (Describe)extendedSqlStatement;
        Assert.assertEquals("europa", describe.getTable().getName());
    }

    @Test
    public void testParseDescPositive2() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("  \tDeSc   \tEuRopa");
        Assert.assertNotNull(extendedSqlStatement);
        Assert.assertTrue(extendedSqlStatement instanceof Describe);
        Describe describe = (Describe)extendedSqlStatement;
        Assert.assertEquals("europa", describe.getTable().getName());
    }

    @Test
    public void testParseDescNegative1() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("  \tDeSc   \tEuRopa aa");
        Assert.assertNull(extendedSqlStatement);
    }

    @Test
    public void testParseDescNegative2() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("  \tDeS   \tEuRopa aa");
        Assert.assertNull(extendedSqlStatement);
    }

    @Test
    public void testParseShowTablesPositive1() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("show tables");
        Assert.assertNotNull(extendedSqlStatement);
        Assert.assertTrue(extendedSqlStatement instanceof ShowTables);
    }

    @Test
    public void testParseShowTablesPositive2() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("\tShow  \t TaBLes");
        Assert.assertNotNull(extendedSqlStatement);
        Assert.assertTrue(extendedSqlStatement instanceof ShowTables);
    }

    @Test
    public void testParseShowTablesNegative1() throws Exception {
        ExtendedSqlStatement extendedSqlStatement = metaStatementMatcher.parse("\tSho  \t TaBLes");
        Assert.assertNull(extendedSqlStatement);
    }

}
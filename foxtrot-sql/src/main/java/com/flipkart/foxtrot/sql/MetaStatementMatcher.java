package com.flipkart.foxtrot.sql;

import com.flipkart.foxtrot.sql.extendedsql.ExtendedSqlStatement;
import com.flipkart.foxtrot.sql.extendedsql.desc.Describe;
import com.flipkart.foxtrot.sql.extendedsql.showtables.ShowTables;
import net.sf.jsqlparser.schema.Table;

public class MetaStatementMatcher {
    //desc <table>
    private static final String DESC_STATEMENT_MATCH="^\\s*[dD][eE][sS][cC]\\s+[a-zA-Z-_]+$";
    private static final String DESC_STATEMENT_DELIMITER="^\\s*[dD][eE][sS][cC]\\s+";

    //show tables
    private static final String SHOWTABLES_STATEMENT_MATCH="^\\s*[sS][hH][oO][wW]\\s+[tT][aA][bB][lL][eE][sS]$";

    public ExtendedSqlStatement parse(final String fql) {
        if(fql.matches(DESC_STATEMENT_MATCH)) {
            final String parts[] = fql.split(DESC_STATEMENT_DELIMITER);
            if(parts.length != 2) {
                throw new RuntimeException("Could not decode table name from desc statement. Table name format is: [a-zA-Z-_]+");
            }
            return new Describe(new Table(parts[1].toLowerCase()));
        }
        if(fql.matches(SHOWTABLES_STATEMENT_MATCH)) {
            return new ShowTables();
        }
        return null;
    }
}

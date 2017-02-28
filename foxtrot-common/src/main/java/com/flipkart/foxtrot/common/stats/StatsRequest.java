package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsRequest extends ActionRequest {

    private String table;

    private String field;

    private List<String> nesting;

    private FilterCombinerType combiner = FilterCombinerType.and;

    public StatsRequest() {
        super(Opcodes.STATS);
    }

    public StatsRequest(List<Filter> filters, String table, String field, List<String> nesting) {
        super(Opcodes.STATS, filters);
        this.table = table;
        this.field = field;
        this.nesting = nesting;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public List<String> getNesting() {
        return nesting;
    }

    public void setNesting(List<String> nesting) {
        this.nesting = nesting;
    }

    public FilterCombinerType getCombiner() {
        return combiner;
    }

    public void setCombiner(FilterCombinerType combiner) {
        this.combiner = combiner;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("nesting", nesting)
                .append("combiner", combiner)
                .toString();
    }
}

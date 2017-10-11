package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendRequest extends ActionRequest {

    private String table;

    private String field;

    private FilterCombinerType combiner = FilterCombinerType.and;

    private List<String> nesting;

    private Period period = Period.hours;

    private String timestamp = "_timestamp";

    public StatsTrendRequest() {
        super(Opcodes.STATS_TREND);
    }

    public StatsTrendRequest(List<Filter> filters, String table, String field, FilterCombinerType combiner, List<String> nesting, Period period, String timestamp) {
        super(Opcodes.STATS_TREND, filters);
        this.table = table;
        this.field = field;
        this.combiner = combiner;
        this.nesting = nesting;
        this.period = period;
        this.timestamp = timestamp;
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

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("combiner", combiner)
                .append("nesting", nesting)
                .append("period", period)
                .append("timestamp", timestamp)
                .toString();
    }
}

package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsTrendRequest extends ActionRequest {

    private String table;

    private String field;

    private FilterCombinerType combiner = FilterCombinerType.and;

    private Period period = Period.hours;

    private String timestamp = "_timestamp";

    public StatsTrendRequest() {

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
    public Set<String> validate() {
        Set<String> validationErrors = new HashSet<>();
        if (CollectionUtils.isStringNullOrEmpty(table)) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (CollectionUtils.isStringNullOrEmpty(field)) {
            validationErrors.add("field name cannot be null or empty");
        }
        if (CollectionUtils.isStringNullOrEmpty(timestamp)) {
            validationErrors.add("timestamp field cannot be null or empty");
        }
        if (combiner == null) {
            validationErrors.add(String.format("specify filter combiner (%s)", StringUtils.join(FilterCombinerType.values())));
        }
        if (period == null) {
            validationErrors.add(String.format("specify time period (%s)", StringUtils.join(Period.values())));
        }
        return validationErrors;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("combiner", combiner)
                .append("period", period)
                .append("timestamp", timestamp)
                .toString();
    }
}

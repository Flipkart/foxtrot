package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.FilterCombinerType;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsRequest extends ActionRequest {

    private String table;

    private String field;

    private FilterCombinerType combiner = FilterCombinerType.and;

    public StatsRequest() {

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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("field", field)
                .append("filters", getFilters())
                .append("combiner", combiner)
                .toString();
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
        if (combiner == null) {
            validationErrors.add(String.format("specify filter combiner (%s)", StringUtils.join(FilterCombinerType.values())));
        }
        return validationErrors;
    }
}

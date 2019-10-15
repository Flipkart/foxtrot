package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.query.Filter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Set;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class StatsRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    @NotNull
    @NotEmpty
    private String field;

    private Set<Stat> stats;

    private List<Double> percentiles;

    @Size(max = 10)
    private List<String> nesting;

    private Set<AnalyticsRequestFlags> flags;

    public StatsRequest() {
        super(Opcodes.STATS);
    }

    public StatsRequest(
            List<Filter> filters,
            String table,
            String field,
            List<Double> percentiles,
            Set<Stat> stats,
            List<String> nesting,
            Set<AnalyticsRequestFlags> flags) {
        super(Opcodes.STATS, filters);
        this.table = table;
        this.field = field;
        this.percentiles = percentiles;
        this.stats = stats;
        this.nesting = nesting;
        this.flags = flags;
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("stats", stats)
                .append("percentiles", percentiles)
                .append("nesting", nesting)
                .toString();
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

    public List<Double> getPercentiles() {
        return percentiles;
    }

    public void setPercentiles(List<Double> percentiles) {
        this.percentiles = percentiles;
    }

    public Set<Stat> getStats() {
        return stats;
    }

    public void setStats(Set<Stat> stats) {
        this.stats = stats;
    }
}

package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.Period;
import com.flipkart.foxtrot.common.query.Filter;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Set;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
@Getter
@Setter
public class StatsTrendRequest extends ActionRequest {

    private String table;

    private String field;

    private List<String> nesting;

    private Set<Stat> stats;

    private List<Double> percentiles;

    private Period period = Period.hours;

    private String timestamp = "_timestamp";

    private double compression = 100.0;

    private Set<AnalyticsRequestFlags> flags;

    public StatsTrendRequest() {
        super(Opcodes.STATS_TREND);
    }

    @Builder
    public StatsTrendRequest(List<Filter> filters, String table, String field, Set<Stat> stats, List<String> nesting,
                             List<Double> percentiles, Period period, String timestamp, double compression,
                             Set<AnalyticsRequestFlags> flags) {
        super(Opcodes.STATS_TREND, filters);
        this.table = table;
        this.field = field;
        this.nesting = nesting;
        this.stats = stats;
        this.percentiles = percentiles;
        this.period = period;
        this.timestamp = timestamp;
        this.compression = compression;
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
                .append("nesting", nesting)
                .append("stats", stats)
                .append("percentiles", percentiles)
                .append("period", period)
                .append("timestamp", timestamp)
                .append("compression", compression)
                .toString();
    }
}

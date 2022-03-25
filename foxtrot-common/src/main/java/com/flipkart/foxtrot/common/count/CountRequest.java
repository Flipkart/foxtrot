package com.flipkart.foxtrot.common.count;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.enums.CountPrecision;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.query.Filter;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CountRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private String field;

    private boolean isDistinct;

    private CountPrecision precision;

    private String consoleId;

    public CountRequest() {
        super(Opcodes.COUNT);
    }

    @Builder
    public CountRequest(String opcode,
                        List<Filter> filters,
                        boolean bypassCache,
                        Map<String, String> requestTags,
                        String table,
                        String field,
                        boolean isDistinct,
                        CountPrecision countPrecision,
                        String consoleId,
                        SourceType sourceType,
                        boolean extrapolationFlag) {
        super(opcode, filters, bypassCache, requestTags, sourceType, extrapolationFlag);
        this.table = table;
        this.field = field;
        this.isDistinct = isDistinct;
        this.precision = countPrecision;
        this.consoleId = consoleId;
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("consoleId", consoleId)
                .append("isDistinct", isDistinct)
                .toString();
    }
}

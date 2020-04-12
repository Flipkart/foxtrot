package com.flipkart.foxtrot.common.count;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import javax.validation.constraints.NotNull;
import com.flipkart.foxtrot.common.enums.CountPrecision;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
@Builder
@AllArgsConstructor
@Getter
@Setter
public class CountRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private String field;

    private boolean isDistinct;

    private CountPrecision precision;

    public CountRequest() {
        super(Opcodes.COUNT);
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("isDistinct", isDistinct)
                .toString();
    }
}

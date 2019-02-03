package com.flipkart.foxtrot.common.count;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.query.Filter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
public class CountRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    private String field;

    private boolean isDistinct;

    public CountRequest() {
        super(Opcodes.COUNT);
    }

    public CountRequest(List<Filter> filters, String table, String field, boolean isDistinct) {
        super(Opcodes.COUNT, filters);
        this.table = table;
        this.field = field;
        this.isDistinct = isDistinct;
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

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("table", table)
                .append("field", field)
                .append("isDistinct", isDistinct)
                .toString();
    }
}
